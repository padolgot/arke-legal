"""Arke — the living organism.

Startup:
  1. mount workspace (sdb)
  2. load config + models
  3. ingest digest/ if present
  4. enter main loop

Main loop (1-second pulse):
  - drain inbox  → process requests → write outbox
  - check digest → re-ingest if hash changed
"""
import hashlib
import logging
import signal
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np

from . import chunker, loader, mailbox, sdb, stress
from .bm25 import BM25Index
from .config import Config
from .models import LLM, Models
from .workspace import mount as mount_workspace
from .types import Chunk, Doc, SearchHit

CASE_NAME_TABLE = "case_names"
CASE_NAME_EXTRACT_CHARS = 2000
CASE_NAME_WORKERS = 10
CASE_NAME_PROMPT = (
    "Return a one-line label for this document.\n"
    "\n"
    "FIRST decide: is this a court judgment with named parties?\n"
    "\n"
    "IF YES → return ONLY the case title. Nothing else.\n"
    "Format: 'Party A v Party B [Year]' — year in square brackets ONLY if "
    "clearly stated in the document. If year is absent, omit the brackets "
    "entirely — never write the literal '[Year]'.\n"
    "Do NOT prefix with 'Case judgment,', 'Judgment on,', 'Court decision,' "
    "or any descriptor. The case title stands alone.\n"
    "  Caparo Industries v Dickman [1990]\n"
    "  R (Miller) v Prime Minister [2019]\n"
    "  Baird Textile Holdings Ltd v Marks and Spencer plc\n"
    "\n"
    "IF NO (contract, memo, letter, witness statement, expert report, opinion, "
    "email, pleading, research note, etc.) → return a brief descriptor: "
    "document type + subject + date if available.\n"
    "  Engagement letter, Smith Holdings audit, January 2022\n"
    "  Witness statement of James Wilson, March 2024\n"
    "  Expert report on construction defects, Dr Jane Smith, 2020\n"
    "\n"
    "Hard rules:\n"
    "- One line, plain text, no quotes, no trailing punctuation.\n"
    "- Never include the word 'unknown' inside the label — if a party or date "
    "is unknown, omit that piece.\n"
    "- Never include literal placeholders like '[Year]' or '[Date]'.\n"
    "- If the document's nature is genuinely impossible to identify at all, "
    "return exactly the single word: unknown"
)

logger = logging.getLogger(__name__)

TICK = 1.0  # seconds


def run() -> None:
    cfg = Config.from_env().resolved()
    ws = mount_workspace(cfg.workspace)
    mailbox.setup(ws.inbox, ws.outbox)
    models = Models.load(cfg)

    digest_path = ws.path / "digest"
    docs: dict[str, Doc] = {}
    bm25 = BM25Index()
    last_digest_hash = ""

    if digest_path.exists():
        logger.info("loading digest on startup...")
        last_digest_hash = _ingest(digest_path, cfg, models, docs, bm25)

    logger.info("arke ready [%s] — %d docs, %d chunks", ws.name, len(docs), _chunk_count(docs))

    # systemd sends SIGTERM on stop. Translate to KeyboardInterrupt so the
    # main loop unwinds cleanly — no half-processed message left in inbox.
    def _on_sigterm(signum, frame):
        del signum, frame
        raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, _on_sigterm)

    try:
        while True:
            _drain(docs, bm25, cfg, models)
            last_digest_hash = _watch_digest(digest_path, last_digest_hash, cfg, models, docs, bm25)
            time.sleep(TICK)
    except KeyboardInterrupt:
        logger.info("shutting down")


def main() -> None:
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()


# --- ingest ------------------------------------------------------------------

def _ingest(digest_path: Path, cfg: Config, models: Models, docs: dict[str, Doc], bm25: BM25Index) -> str:
    """case_name must exist BEFORE embedding so it can be prepended as a
    contextual header — without it, mid-judgment chunks have no anchor to
    the case identity.

    Two ingest modes, auto-detected:
      manifest — `manifest.jsonl` present in digest_path. Discovery and metadata
                 come from it; doc_id matches the citation graph; LLM case_name
                 extraction is skipped (we already have `title`).
      walk     — no manifest. Generic rglob by extension + LLM case_name. This
                 is the rclone-sync flow (SharePoint/OneDrive)."""
    docs.clear()
    bm25.clear()
    model_key = cfg.embed_model_path or cfg.cloud_embed_model

    manifest_mode = (digest_path / "manifest.jsonl").exists()
    if manifest_mode:
        pairs = loader.load_corpus(digest_path)
        logger.info("ingest start (manifest) — %d docs under %s", len(pairs), digest_path)
    else:
        files = [p for p in sorted(digest_path.rglob("*")) if p.is_file() and not p.name.startswith(".")]
        total_files = len(files)
        logger.info("ingest start (walk) — %d files under %s", total_files, digest_path)
        pairs = []
        for file_idx, path in enumerate(files, 1):
            result = loader.load_file(path, root=digest_path)
            if result is None:
                logger.info("[%d/%d] skipped (unsupported): %s", file_idx, total_files, path.name)
                continue
            pairs.append(result)

    for doc, text in pairs:
        chunk_datas = chunker.chunk(text, cfg.chunk_size, cfg.overlap)
        for i, cd in enumerate(chunk_datas):
            doc.chunks.append(
                Chunk(doc_id=doc.id, chunk_index=i, clean=cd.clean, head=cd.head, tail=cd.tail)
            )
        docs[doc.id] = doc

    if not manifest_mode:
        _fill_case_names(docs, models.llm)
    for doc in docs.values():
        case_name = doc.metadata.get("case_name", "") or ""
        if not case_name:
            continue
        for chunk in doc.chunks:
            chunk.context_header = case_name

    # BM25 sees overlapped() (no header) — keeps IDF clean. Embedder sees
    # baked() (header + overlapped) — anchors mid-judgment chunks to case identity.
    # Embed in parallel — per-doc HTTP latency is the bottleneck (5-7s/call), and
    # OpenAI embed endpoints accept high concurrency. Workers tuned to stay under
    # tier RPM/TPM limits; back off if 429s appear.
    cached_total, embedded_total = _embed_all_parallel(docs, models.embedder, model_key)

    for doc in docs.values():
        for chunk in doc.chunks:
            bm25.add(f"{doc.id}:{chunk.chunk_index}", chunk.overlapped())

    bm25.build()
    logger.info(
        "ingest done — %d docs, %d chunks (%d cached, %d embedded)",
        len(docs), _chunk_count(docs), cached_total, embedded_total,
    )

    return _dir_hash(digest_path)


EMBED_WORKERS = 4


def _embed_doc(doc: Doc, embedder, model_key: str) -> tuple[Doc, int, str | None]:
    """Embed every uncached chunk in `doc` and persist.

    Returns (doc, n_embedded, error). error is None on success, an error
    string on failure — so one bad doc doesn't tear down the whole pool."""
    missing_idx: list[int] = []
    missing_texts: list[str] = []
    for i, chunk in enumerate(doc.chunks):
        if chunk.load_embedding(model_key, "1"):
            continue
        missing_idx.append(i)
        missing_texts.append(chunk.baked())

    if not missing_texts:
        return doc, 0, None
    try:
        vecs = embedder.embed(missing_texts)
    except Exception as e:
        return doc, 0, f"{type(e).__name__}: {e}"
    for idx, vec in zip(missing_idx, vecs):
        doc.chunks[idx].embedding = np.array(vec, dtype=np.float32)
        doc.chunks[idx].save_embedding(model_key, "1")
    return doc, len(missing_texts), None


def _embed_all_parallel(docs: dict[str, Doc], embedder, model_key: str) -> tuple[int, int]:
    """Parallel-embed all docs. Returns (cached_total, embedded_total).

    Failed docs are logged and skipped — their chunks have no embedding and
    will not surface in cosine retrieval, but BM25 still indexes them."""
    cached_total = 0
    embedded_total = 0
    failed = 0
    total = len(docs)
    with ThreadPoolExecutor(max_workers=EMBED_WORKERS) as ex:
        futures = {ex.submit(_embed_doc, doc, embedder, model_key): doc for doc in docs.values()}
        for completed_idx, future in enumerate(as_completed(futures), 1):
            doc, n_embedded, error = future.result()
            if error:
                failed += 1
                logger.warning("[%d/%d] %s — embed FAILED: %s", completed_idx, total, doc.label, error)
                continue
            n_cached = len(doc.chunks) - n_embedded
            cached_total += n_cached
            embedded_total += n_embedded
            ctx_yes = bool(doc.chunks and doc.chunks[0].context_header)
            logger.info(
                "[%d/%d] %s — %d chunks (%d cached, %d embedded) ctx=%s",
                completed_idx, total, doc.label, len(doc.chunks), n_cached, n_embedded,
                "yes" if ctx_yes else "no",
            )
    if failed:
        logger.warning("embed: %d/%d docs failed (skipped, no cosine for them)", failed, total)
    return cached_total, embedded_total


def _extract_case_name(doc: Doc, llm: LLM) -> str:
    if not doc.chunks:
        return ""
    sample = (doc.chunks[0].head + " " + doc.chunks[0].clean)[:CASE_NAME_EXTRACT_CHARS]
    try:
        raw = llm.chat(CASE_NAME_PROMPT, sample).strip()
    except Exception as e:
        logger.warning("case-name extract failed for %s: %s", doc.id[:8], e)
        return ""
    if not raw or raw.lower() == "unknown" or len(raw) > 200 or "\n" in raw:
        return ""
    return raw


def _fill_case_names(docs: dict[str, Doc], llm: LLM) -> None:
    """Cache is keyed by doc.id (content hash) → survives restarts,
    invalidates automatically when a doc's content changes."""
    pending: list[Doc] = []
    hits = 0
    for doc in docs.values():
        cached = sdb.get_json(CASE_NAME_TABLE, doc.id)
        if cached is not None:
            doc.metadata["case_name"] = cached.get("name", "")
            hits += 1
        else:
            pending.append(doc)

    logger.info("case-names: %d cached, %d pending", hits, len(pending))
    if not pending:
        return

    def worker(doc: Doc) -> tuple[Doc, str]:
        return doc, _extract_case_name(doc, llm)

    with ThreadPoolExecutor(max_workers=CASE_NAME_WORKERS) as ex:
        for future in as_completed(ex.submit(worker, d) for d in pending):
            try:
                doc, name = future.result()
                doc.metadata["case_name"] = name
                sdb.put_json(CASE_NAME_TABLE, doc.id, {"name": name})
            except Exception as e:
                logger.warning("case-name persist failed: %s", e)

    logger.info("case-names: extracted %d via LLM", len(pending))


# --- main loop ---------------------------------------------------------------

def _drain(docs: dict[str, Doc], bm25: BM25Index, cfg: Config, models: Models) -> None:
    for msg_id, request in mailbox.drain():
        try:
            response = _dispatch(request, docs, bm25, cfg, models)
        except Exception as e:
            logger.warning("handler error: %s", e)
            response = {"ok": False, "error": str(e)}
        mailbox.reply(msg_id, response)


def _dispatch(request: dict, docs: dict[str, Doc], bm25: BM25Index, cfg: Config, models: Models) -> dict:
    cmd = request.get("cmd")

    if cmd == "stress":
        return stress.handle(request, docs, bm25, cfg, models)

    if cmd == "search":
        return _search(request, docs, bm25, cfg, models)

    if cmd == "ping":
        return {"ok": True, "pong": True}

    return {"ok": False, "error": f"unknown cmd: {cmd}"}


_CITATION_FIELDS = (
    "canonical_id", "neutral_citation", "celex", "ecli", "party_slug",
    "title", "court", "date", "doc_type", "category", "cite_in_count",
    "url", "source",
)


def _citation_row(hit: SearchHit, docs: dict[str, Doc]) -> dict:
    """Shape one search hit for an LLM/MCP consumer. Manifest-mode docs
    surface rich fields (citation IDs, court, date, weight); walk-mode docs
    surface only the minimum (doc_id + chunk + snippet)."""
    doc = docs.get(hit.chunk.doc_id)
    meta = doc.metadata if doc else {}
    row = {
        "doc_id": hit.chunk.doc_id,
        "chunk_index": hit.chunk.chunk_index,
        "score": round(hit.similarity, 3),
        "snippet": hit.chunk.clean,
    }
    for field in _CITATION_FIELDS:
        value = meta.get(field)
        if value:
            row[field] = value
    return row


def _search(request: dict, docs: dict[str, Doc], bm25: BM25Index, cfg: Config, models: Models) -> dict:
    """Retrieval-only probe — no LLM. Powers eval/sweep AND the MCP/email
    consumer surface that needs rich citation metadata."""
    query = request.get("query", "")
    if not query:
        return {"ok": False, "error": "query is required"}
    q_vec = np.array(models.embedder.embed([query])[0], dtype=np.float32)
    hits = stress.hybrid_search(docs, bm25, q_vec, query, cfg.k, cfg.alpha)
    return {
        "ok": True,
        "citations": [_citation_row(h, docs) for h in hits],
    }


def _watch_digest(
    digest_path: Path,
    last_hash: str,
    cfg: Config,
    models: Models,
    docs: dict[str, Doc],
    bm25: BM25Index,
) -> str:
    if not digest_path.exists():
        return last_hash

    current_hash = _dir_hash(digest_path)
    if current_hash == last_hash:
        return last_hash

    logger.info("new digest detected, re-ingesting...")
    return _ingest(digest_path, cfg, models, docs, bm25)


# --- helpers -----------------------------------------------------------------

def _chunk_count(docs: dict[str, Doc]) -> int:
    return sum(len(d.chunks) for d in docs.values())


def _dir_hash(path: Path) -> str:
    h = hashlib.md5()
    for f in sorted(path.rglob("*")):
        if f.is_file():
            st = f.stat()
            h.update(str(f.relative_to(path)).encode())
            h.update(str(st.st_size).encode())
            h.update(str(st.st_mtime_ns).encode())
    return h.hexdigest()
