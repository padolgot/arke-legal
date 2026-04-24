"""Arke — the living organism.

Startup sequence:
  1. mount workspace (sdb)
  2. load config + models
  3. consume digest/ if present
  4. enter main loop

Main loop (1-second pulse):
  - drain inbox  → process requests → write outbox
  - check digest → re-ingest if hash changed
"""
import hashlib
import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np

from . import chunker, loader, mailbox, sdb
from .bm25 import BM25Index
from .config import Config
from .models import Models
from .workspace import mount as mount_workspace
from .types import Chunk, Doc, SearchHit

logger = logging.getLogger(__name__)

TICK = 1.0  # seconds

SYSTEM_PROMPT = (
    "You are Arke — a private legal intelligence service for a practising "
    "litigator. You produce research memoranda from the case materials "
    "provided with each query.\n"
    "\n"
    "Rules:\n"
    "- Answer only from the provided documents. If the documents do not "
    "address the question, say so in one sentence and stop.\n"
    "- Every statement of law or fact must carry an inline citation marker "
    "[n] matching the document number in the context.\n"
    "- Open with a one-sentence conclusion. Follow with numbered points "
    "that support it, each ending with its [n] markers.\n"
    "- Write in professional British legal register. Plain prose. No "
    "bullet soup, no headings unless genuinely needed.\n"
    "- Never offer advice, recommendations, or your own opinion. No \"I "
    "think\", no \"you should\", no \"it is advisable\". State what the "
    "authorities hold.\n"
    "- Never invent case names, citations, holdings, or statutory "
    "references."
)


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

    while True:
        _drain(docs, bm25, cfg, models)
        last_digest_hash = _watch_digest(digest_path, last_digest_hash, cfg, models, docs, bm25)
        time.sleep(TICK)


def main() -> None:
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()


# --- ingest ------------------------------------------------------------------

def _ingest(digest_path: Path, cfg: Config, models: Models, docs: dict[str, Doc], bm25: BM25Index) -> str:
    docs.clear()
    bm25.clear()
    model_key = cfg.embed_model_path or cfg.cloud_embed_model

    files = [p for p in sorted(digest_path.rglob("*")) if p.is_file() and not p.name.startswith(".")]
    total_files = len(files)
    logger.info("ingest start — %d files under %s", total_files, digest_path)

    cached_total = 0
    embedded_total = 0

    for file_idx, path in enumerate(files, 1):
        result = loader.load_file(path, root=digest_path)
        if result is None:
            logger.info("[%d/%d] skipped (unsupported): %s", file_idx, total_files, path.name)
            continue

        doc, text = result

        sdb.put_bin("sources", doc.id, path.read_bytes())

        chunk_datas = chunker.chunk(text, cfg.chunk_size, cfg.overlap)
        chunks = [
            Chunk(doc_id=doc.id, chunk_index=i, clean=cd.clean, head=cd.head, tail=cd.tail)
            for i, cd in enumerate(chunk_datas)
        ]

        missing_idx: list[int] = []
        missing_texts: list[str] = []
        for i, chunk in enumerate(chunks):
            if chunk.load_embedding(model_key, "1"):
                continue
            missing_idx.append(i)
            missing_texts.append(chunk.overlapped())

        if missing_texts:
            vecs = models.embedder.embed(missing_texts)
            for idx, vec in zip(missing_idx, vecs):
                chunks[idx].embedding = np.array(vec, dtype=np.float32)
                chunks[idx].save_embedding(model_key, "1")

        cached = len(chunks) - len(missing_texts)
        embedded = len(missing_texts)

        for chunk in chunks:
            bm25.add(f"{doc.id}:{chunk.chunk_index}", chunk.overlapped())
            doc.chunks.append(chunk)

        doc.save()
        docs[doc.id] = doc
        cached_total += cached
        embedded_total += embedded
        logger.info(
            "[%d/%d] %s — %d chunks (%d cached, %d embedded)",
            file_idx, total_files, path.name, len(chunk_datas), cached, embedded,
        )

    bm25.build()
    logger.info(
        "ingest done — %d docs, %d chunks (%d cached, %d embedded)",
        len(docs), _chunk_count(docs), cached_total, embedded_total,
    )
    return _dir_hash(digest_path)


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

    if cmd == "ask":
        return _ask(request, docs, bm25, cfg, models)

    if cmd == "stress":
        return _stress_test(request, docs, bm25, cfg, models)

    if cmd == "ping":
        return {"ok": True, "pong": True}

    if cmd == "sample":
        return _sample(request, docs)

    return {"ok": False, "error": f"unknown cmd: {cmd}"}


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


# --- ask ---------------------------------------------------------------------

def _ask(request: dict, docs: dict[str, Doc], bm25: BM25Index, cfg: Config, models: Models) -> dict:
    query = request.get("query", "")
    if not query:
        return {"ok": False, "error": "query is required"}

    q_vec = np.array(models.embedder.embed([query])[0], dtype=np.float32)
    hits = _hybrid_search(docs, bm25, q_vec, query, cfg.k, cfg.alpha)

    if not hits:
        return {"ok": True, "answer": "No relevant documents found.", "citations": []}

    context = "\n\n".join(
        f"[{i+1}] (source: {_source_label(docs[h.chunk.doc_id])}) {h.chunk.clean}"
        for i, h in enumerate(hits)
    )
    answer = models.llm.chat(SYSTEM_PROMPT, f"Documents:\n{context}\n\nQuestion: {query}")

    citations: list[dict] = []
    for hit in hits:
        doc = docs[hit.chunk.doc_id]
        label = _source_label(doc)
        filename = doc.metadata.get("filename") or label
        citations.append({
            "doc_id": doc.id,
            "source": label,
            "filename": filename,
            "text": hit.chunk.clean,
            "score": round(hit.similarity, 3),
        })
    return {"ok": True, "answer": answer, "citations": citations}


def _source_label(doc: Doc) -> str:
    filename = doc.metadata.get("filename") or doc.source or doc.id[:8]
    return Path(filename).stem


def _sample(request: dict, docs: dict[str, Doc]) -> dict:
    import random
    limit = request.get("limit", 50)
    all_chunks = [chunk for doc in docs.values() for chunk in doc.chunks]
    sample = random.sample(all_chunks, min(limit, len(all_chunks)))
    return {"ok": True, "chunks": [
        {"doc_id": c.doc_id, "chunk_index": c.chunk_index, "clean": c.clean, "head": c.head, "tail": c.tail}
        for c in sample
    ]}


# --- stress-test (adversarial authority) ------------------------------------

COUNTER_QUERIES_PROMPT = (
    "You receive a legal argument that a practising litigator wants to "
    "stress-test. Generate 5 short search queries that would find authorities "
    "in their own corpus that OPPOSE, LIMIT, or DISTINGUISH this position.\n"
    "\n"
    "Focus on: contrary precedents, doctrinal limits of the claimed principle, "
    "cases with opposite ratio on similar facts, distinguishing authorities, "
    "exceptions to the rule.\n"
    "\n"
    "Output: one query per line. No numbering. No explanations. 5 queries."
)

PER_DOC_PROMPT = (
    "You analyse one document from a litigator's case archive. Identify chunks "
    "that form an adversarial mosaic against the lawyer's argument — parts of "
    "the document that would weaken, limit, or contradict the argument.\n"
    "\n"
    "Return ONLY a JSON array of chunk indices, e.g. [3, 7, 14, 22].\n"
    "\n"
    "Rules:\n"
    "- Range 5-25 indices when the document has adversarial content.\n"
    "- Return [] if the document contains nothing adversarial to the argument.\n"
    "- Do not explain. Output only the JSON array."
)

STRESS_SYSTEM_PROMPT = (
    "You are adverse counsel reviewing a lawyer's position. From their own "
    "archive you receive passages that may weaken, limit, or contradict it. "
    "Produce a citation mosaic showing what in the lawyer's own documents "
    "cuts against their argument.\n"
    "\n"
    "Format (STRICT — do not deviate):\n"
    "- Start directly with one opening sentence naming the main vulnerabilities. "
    "No salutation, no 'Dear', no date, no 'Yours sincerely', no signature, no placeholders.\n"
    "- Then 2-3 sections, each drawn from a DIFFERENT source document. "
    "If you only use one source, that is a failure.\n"
    "- Each section starts with '## ' followed by the source name.\n"
    "- Under each heading, quote the relevant passages as '> ' blockquotes.\n"
    "- After each quote, one sentence in plain prose explaining why it cuts "
    "against the argument.\n"
    "- British legal register. No advice, no opinion, no hedging.\n"
    "- Skip sources that do not cut against the argument.\n"
    "- Use ONLY the passages provided. Never invent case names or holdings."
)

STRESS_MAX_DOCS = 10
STRESS_DOC_MAX_TOKENS = 50000
STRESS_RETRIEVAL_K = 20
STRESS_MAX_WORKERS = 3


def _counter_queries(argument: str, llm) -> list[str]:
    raw = llm.chat(COUNTER_QUERIES_PROMPT, argument)
    queries = [q.strip(" -•*.\t").strip() for q in raw.splitlines()]
    return [q for q in queries if q][:5]


def _per_doc_filter(argument: str, doc: Doc, llm) -> list[int]:
    chunks_block = "\n\n".join(f"[{i}] {c.clean}" for i, c in enumerate(doc.chunks))
    user = f"Argument:\n{argument}\n\nDocument chunks:\n{chunks_block}"
    raw = llm.chat(PER_DOC_PROMPT, user)
    match = re.search(r'\[[\d,\s]*\]', raw)
    if not match:
        return []
    try:
        indices = json.loads(match.group(0))
    except (json.JSONDecodeError, ValueError):
        return []
    return [i for i in indices if isinstance(i, int) and 0 <= i < len(doc.chunks)]


def _doc_token_estimate(doc: Doc) -> int:
    return sum(len(c.clean) for c in doc.chunks) // 4


def _stress_test(
    request: dict, docs: dict[str, Doc], bm25: BM25Index, cfg: Config, models: Models
) -> dict:
    argument = request.get("argument") or request.get("query") or ""
    if not argument:
        return {"ok": False, "error": "argument is required"}

    logger.info("stress-test: argument (%d chars)", len(argument))

    counter_qs = _counter_queries(argument, models.llm)
    logger.info("stress-test: %d counter-queries generated", len(counter_qs))

    # Broad retrieval — direct + counter-queries, dedupe by chunk key
    all_queries = [argument] + counter_qs
    seen_chunks: dict[str, SearchHit] = {}
    for q in all_queries:
        q_vec = np.array(models.embedder.embed([q])[0], dtype=np.float32)
        hits = _hybrid_search(docs, bm25, q_vec, q, STRESS_RETRIEVAL_K, cfg.alpha)
        for h in hits:
            key = f"{h.chunk.doc_id}:{h.chunk.chunk_index}"
            if key not in seen_chunks or h.similarity > seen_chunks[key].similarity:
                seen_chunks[key] = h

    # Rank docs by best chunk score across all queries
    by_doc: dict[str, float] = {}
    for h in seen_chunks.values():
        did = h.chunk.doc_id
        if did not in by_doc or h.similarity > by_doc[did]:
            by_doc[did] = h.similarity
    top_doc_ids = sorted(by_doc, key=lambda d: by_doc[d], reverse=True)[:STRESS_MAX_DOCS]
    logger.info("stress-test: %d candidate docs", len(top_doc_ids))

    # Size filter first, then fan out per-doc LLM in parallel
    fit_docs: list[Doc] = []
    for doc_id in top_doc_ids:
        doc = docs[doc_id]
        est = _doc_token_estimate(doc)
        if est > STRESS_DOC_MAX_TOKENS:
            logger.info("stress-test: skip %s (%dk tokens)", _source_label(doc), est // 1000)
            continue
        fit_docs.append(doc)

    mosaics: dict[str, list[Chunk]] = {}
    if fit_docs:
        with ThreadPoolExecutor(max_workers=min(STRESS_MAX_WORKERS, len(fit_docs))) as ex:
            results = list(ex.map(lambda d: (d, _per_doc_filter(argument, d, models.llm)), fit_docs))
        for doc, indices in results:
            if indices:
                mosaics[doc.id] = [doc.chunks[i] for i in indices]
            logger.info("stress-test: %s → %d/%d chunks", _source_label(doc), len(indices), len(doc.chunks))

    if not mosaics:
        return {
            "ok": True,
            "answer": "No adversarial authorities found in the provided archive.",
            "citations": [],
        }

    # Final LLM — writes the mosaic letter from selected chunks
    citations: list[dict] = []
    context_parts: list[str] = []
    idx = 1
    for doc_id, chunks in mosaics.items():
        doc = docs[doc_id]
        label = _source_label(doc)
        filename = doc.metadata.get("filename") or label
        for c in chunks:
            context_parts.append(f"[{idx}] (source: {label}) {c.clean}")
            citations.append({
                "doc_id": doc.id,
                "source": label,
                "filename": filename,
                "text": c.clean,
                "score": 1.0,
            })
            idx += 1

    context = "\n\n".join(context_parts)
    user_msg = f"Lawyer's position:\n{argument}\n\nPassages from archive:\n{context}"
    answer = models.llm.chat(STRESS_SYSTEM_PROMPT, user_msg)
    logger.info("stress-test: complete — %d docs in mosaic, %d citations, answer=%dchars", len(mosaics), len(citations), len(answer))
    logger.info("stress-test: answer preview:\n%s", answer[:2000])

    return {"ok": True, "answer": answer, "citations": citations}


def _hybrid_search(
    docs: dict[str, Doc],
    bm25: BM25Index,
    q_vec: np.ndarray,
    query: str,
    k: int,
    alpha: float,
) -> list[SearchHit]:
    q_norm = np.linalg.norm(q_vec)
    if q_norm == 0:
        return []

    # cosine scores
    cosine: dict[str, float] = {}
    for doc in docs.values():
        for chunk in doc.chunks:
            if chunk.embedding is None:
                continue
            c_norm = np.linalg.norm(chunk.embedding)
            if c_norm == 0:
                continue
            key = f"{chunk.doc_id}:{chunk.chunk_index}"
            cosine[key] = float(np.dot(q_vec, chunk.embedding) / (q_norm * c_norm))

    # bm25 scores — normalize to [0, 1]
    bm25_raw = bm25.scores(query)
    bm25_max = max(bm25_raw.values(), default=1.0)
    bm25_norm = {k: v / bm25_max for k, v in bm25_raw.items()} if bm25_max > 0 else {}

    # hybrid score
    all_keys = set(cosine) | set(bm25_norm)
    scored: list[tuple[str, float]] = []
    for key in all_keys:
        score = alpha * cosine.get(key, 0.0) + (1 - alpha) * bm25_norm.get(key, 0.0)
        scored.append((key, score))

    scored.sort(key=lambda x: x[1], reverse=True)

    # resolve keys back to chunks
    chunk_map: dict[str, Chunk] = {
        f"{chunk.doc_id}:{chunk.chunk_index}": chunk
        for doc in docs.values()
        for chunk in doc.chunks
    }

    hits: list[SearchHit] = []
    for key, score in scored[:k]:
        chunk = chunk_map.get(key)
        if chunk:
            hits.append(SearchHit(chunk=chunk, similarity=score))
    return hits


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
