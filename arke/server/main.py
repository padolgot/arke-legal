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
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np

from . import chunker, loader, mailbox, sdb
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

    _fill_case_names(docs, models.llm)

    return _dir_hash(digest_path)


def _extract_case_name(doc: Doc, llm: LLM) -> str:
    """Ask the fast LLM for 'Party A v Party B'. Returns "" on unknown/failure."""
    if not doc.chunks:
        return ""
    sample = (doc.chunks[0].head + " " + doc.chunks[0].clean)[:CASE_NAME_EXTRACT_CHARS]
    try:
        raw = llm.chat(CASE_NAME_PROMPT, sample).strip()
    except Exception as e:
        logger.warning("case-name extract failed for %s: %s", doc.id[:8], e)
        return ""
    # Reject if LLM gave us free-form noise
    if not raw or raw.lower() == "unknown" or len(raw) > 200 or "\n" in raw:
        return ""
    return raw


def _fill_case_names(docs: dict[str, Doc], llm: LLM) -> None:
    """Populate doc.metadata['case_name'] via cache lookup + parallel LLM for misses.
    Cache is keyed by doc.id (content hash) → survives restarts, invalidates on
    content change automatically.
    """
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
            doc, name = future.result()
            doc.metadata["case_name"] = name
            sdb.put_json(CASE_NAME_TABLE, doc.id, {"name": name})

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
    return doc.metadata.get("filename") or doc.source or doc.id[:8]


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

PER_DOC_PROMPT = (
    "You analyse one document from a litigator's case archive. Identify chunks "
    "that form an adversarial mosaic against the lawyer's argument — the parts "
    "that would weaken, limit, or contradict it. Be generous: include any "
    "chunk that even partially supports an adversarial reading. A separate "
    "pass will refine the selection later.\n"
    "\n"
    "Return ONLY a JSON array of chunk indices, e.g. [3, 7, 14, 22].\n"
    "\n"
    "Rules:\n"
    "- Range 5-25 indices when the document has adversarial content.\n"
    "- Return [] if the document contains nothing adversarial.\n"
    "- Do not explain. Output only the JSON array."
)

MOSAIC_SYSTEM_PROMPT = (
    "You are counsel stress-testing a lawyer's position. From passages in "
    "their archive, identify cases that bear on the argument. For each case, "
    "classify whether the authority SUPPORTS the lawyer's position or CUTS "
    "AGAINST it, and write the specific legal proposition the passages prove.\n"
    "\n"
    "Output STRICTLY a JSON array:\n"
    '[{"stance": "SUPPORTS"|"ATTACKS", "label": "proposition phrase", "passages": [1, 4]}, ...]\n'
    "\n"
    "Each cluster is ONE document (one authority) making ONE point. All "
    "passages in a cluster must come from the same source document.\n"
    "\n"
    "stance — your honest judgement of valence:\n"
    "  SUPPORTS: the authority reinforces the lawyer's position\n"
    "  ATTACKS: the authority weakens or contradicts the lawyer's position\n"
    "Both are valuable — partners need contrast (gas + brake).\n"
    "\n"
    "label — tight legal proposition the passages establish. 3-8 words. "
    "Specific to the argument. Doctrinal language, not a case name.\n"
    "  SUPPORTS examples:\n"
    "    'No duty to public purchasers of shares'\n"
    "    'Disclaimer effective to exclude responsibility'\n"
    "    'Proximity absent where no direct dealings'\n"
    "  ATTACKS examples:\n"
    "    'Duty found where reliance was specific and known'\n"
    "    'Assumption of responsibility implied despite disclaimer'\n"
    "    'Indeterminate class does not bar duty'\n"
    "  Bad for either stance:\n"
    "    'Caparo precedent'        (just names case)\n"
    "    'Duty of care limits'     (too generic)\n"
    "\n"
    "Rules:\n"
    "- passages: 1-indexed passage numbers. 1-3 per cluster.\n"
    "- 3-5 clusters total, each from a DIFFERENT source document.\n"
    "- Mix SUPPORTS and ATTACKS naturally based on what the corpus contains — "
    "don't force one side.\n"
    "- Skip passages off-topic to the argument.\n"
    "- If corpus contains nothing on-topic, output [].\n"
    "- Output ONLY the JSON array."
)

TRIMMER_SYSTEM_PROMPT = (
    "You receive a mosaic of case-law excerpts chosen as adversarial authority. "
    "Your job is to trim procedural narrative, lead-in, and background from "
    "within each quoted passage, leaving only the operative legal substance — "
    "the ratio, the holding, the doctrinal statement.\n"
    "\n"
    "Rules:\n"
    "- Within each blockquote, DELETE procedural text (recitations of claim "
    "paragraphs, statement-of-claim references, background facts, procedural "
    "history) and REPLACE the deleted span with '[…]' (bracket-ellipsis).\n"
    "- NEVER remove a blockquote entirely.\n"
    "- NEVER rewrite, paraphrase, or add new words. The ONLY new text you "
    "may introduce is '[…]'. Every remaining word must appear verbatim in "
    "the input.\n"
    "- Preserve the structure exactly: ## headers (including [SUPPORTS]/[ATTACKS] "
    "tags and middle-dot separators), > blockquotes, — source lines.\n"
    "- If a passage is already lean (mostly ratio / holding), leave it as-is.\n"
    "\n"
    "Output the trimmed markdown. No preamble, no explanation."
)

STRESS_MAX_DOCS = 10
STRESS_DOC_MAX_TOKENS = 50000
STRESS_RETRIEVAL_K = 40
STRESS_MAX_WORKERS = 3
STRESS_MAX_PASSAGES_PER_DOC = 6


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


MAX_CHUNKS_PER_PASSAGE = 2


def _merge_adjacent(chunks: list[Chunk]) -> list[str]:
    """Group chunks by contiguous chunk_index, split long runs into sub-runs
    of at most MAX_CHUNKS_PER_PASSAGE. Returns one passage text per sub-run.
    """
    if not chunks:
        return []
    ordered = sorted(chunks, key=lambda c: c.chunk_index)
    runs: list[list[Chunk]] = [[ordered[0]]]
    for c in ordered[1:]:
        if c.chunk_index == runs[-1][-1].chunk_index + 1:
            runs[-1].append(c)
        else:
            runs.append([c])
    sub_runs: list[list[Chunk]] = []
    for run in runs:
        for i in range(0, len(run), MAX_CHUNKS_PER_PASSAGE):
            sub_runs.append(run[i : i + MAX_CHUNKS_PER_PASSAGE])
    passages: list[str] = []
    for run in sub_runs:
        body = " ".join(c.clean for c in run)
        text = f"{run[0].head} {body} {run[-1].tail}"
        passages.append(" ".join(text.split()))
    return passages


def _parse_clusters(raw: str, passage_count: int) -> list[dict]:
    """Parse LLM JSON output into validated clusters: [{stance, label, passages}]."""
    match = re.search(r"\[.*\]", raw, re.DOTALL)
    if not match:
        return []
    try:
        data = json.loads(match.group(0))
    except (json.JSONDecodeError, ValueError):
        return []
    clusters: list[dict] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        label = item.get("label")
        passages = item.get("passages")
        stance = str(item.get("stance", "ATTACKS")).upper().strip()
        if stance not in ("SUPPORTS", "ATTACKS"):
            stance = "ATTACKS"
        if not isinstance(label, str) or not isinstance(passages, list):
            continue
        valid = [p for p in passages if isinstance(p, int) and 1 <= p <= passage_count]
        label_clean = label.strip().rstrip(".").strip()
        if label_clean and valid:
            clusters.append({"stance": stance, "label": label_clean, "passages": valid})
    return clusters


def _stress_test(
    request: dict, docs: dict[str, Doc], bm25: BM25Index, cfg: Config, models: Models
) -> dict:
    argument = request.get("argument") or request.get("query") or ""
    if not argument:
        return {"ok": False, "error": "argument is required"}

    logger.info("stress-test: argument (%d chars)", len(argument))

    # Direct hybrid search on the argument — no counter-queries (drift-prone).
    # Per-doc LLM filter below does the adversarial selection.
    q_vec = np.array(models.embedder.embed([argument])[0], dtype=np.float32)
    hits = _hybrid_search(docs, bm25, q_vec, argument, STRESS_RETRIEVAL_K, cfg.alpha)

    # Aggregate to doc-level by best chunk score
    by_doc: dict[str, float] = {}
    for h in hits:
        did = h.chunk.doc_id
        if did not in by_doc or h.similarity > by_doc[did]:
            by_doc[did] = h.similarity
    top_doc_ids = sorted(by_doc, key=lambda d: by_doc[d], reverse=True)[:STRESS_MAX_DOCS]
    logger.info("stress-test: %d candidate docs from %d chunks", len(top_doc_ids), len(hits))

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

    # Merge adjacent selected chunks per doc, cap per-doc, interleave across docs
    from itertools import zip_longest

    per_doc: list[list[dict]] = []
    for doc_id, chunks in mosaics.items():
        doc = docs[doc_id]
        filename = _source_label(doc)
        case_name = doc.metadata.get("case_name", "") or ""
        doc_passages = [
            {
                "doc_id": doc.id,
                "filename": filename,
                "case_name": case_name,
                "text": text,
            }
            for text in _merge_adjacent(chunks)[:STRESS_MAX_PASSAGES_PER_DOC]
        ]
        if doc_passages:
            per_doc.append(doc_passages)

    # Round-robin so the first slots in the LLM context span all docs
    passages: list[dict] = [
        p for group in zip_longest(*per_doc) for p in group if p is not None
    ]

    def _p_label(p: dict) -> str:
        return f"{p['case_name']} ({p['filename']})" if p["case_name"] else p["filename"]
    context = "\n\n".join(
        f"[{i+1}] (source: {_p_label(p)}) {p['text']}" for i, p in enumerate(passages)
    )
    user_msg = f"Argument:\n{argument}\n\nPassages:\n{context}"
    raw = models.strong_llm.chat(MOSAIC_SYSTEM_PROMPT, user_msg)
    all_clusters = _parse_clusters(raw, len(passages))
    # Enforce single-source per cluster and unique-source across clusters
    seen_docs: set[str] = set()
    clusters: list[dict] = []
    for c in all_clusters:
        cluster_docs = {passages[p - 1]["doc_id"] for p in c["passages"]}
        if len(cluster_docs) != 1:
            continue
        doc_id = next(iter(cluster_docs))
        if doc_id in seen_docs:
            continue
        seen_docs.add(doc_id)
        clusters.append(c)
    logger.info(
        "stress-test: LLM returned %d clusters → %d after single-source / unique-doc filter",
        len(all_clusters), len(clusters),
    )

    if not clusters:
        return {
            "ok": True,
            "answer": "No adversarial authorities found in the provided archive.",
            "citations": [],
        }

    # Render: '## [FOR|AGAINST] <case_name> · <label>' per cluster
    parts: list[str] = []
    used: list[dict] = []
    for cluster in clusters:
        first = passages[cluster["passages"][0] - 1]
        case_name = first["case_name"]
        stance = cluster["stance"]
        label = cluster["label"]
        if case_name:
            header = f"[{stance}] {case_name} · {label}"
        else:
            header = f"[{stance}] {label}"
        parts.append(f"## {header}")
        for p_num in cluster["passages"]:
            passage = passages[p_num - 1]
            parts.append(f"> {passage['text']}")
            used.append(passage)
        parts.append(f"— {first['filename']}")

    raw_answer = "\n\n".join(parts)
    logger.info(
        "stress-test: mosaic — %d clusters, %d/%d passages, raw=%dchars",
        len(clusters), len(used), len(passages), len(raw_answer),
    )
    for i, cluster in enumerate(clusters, 1):
        first = passages[cluster["passages"][0] - 1]
        logger.info("  cluster %d: [%s] %r case=%r file=%s n_passages=%d",
                    i, cluster["stance"], cluster["label"],
                    first["case_name"] or "(none)",
                    first["filename"], len(cluster["passages"]))

    # Stage 3: trim procedural water from passages, preserve structure
    answer = models.llm.chat(TRIMMER_SYSTEM_PROMPT, raw_answer)
    answer = answer.strip()
    reduction = (1 - len(answer) / max(len(raw_answer), 1)) * 100
    logger.info("stress-test: trimmed → %dchars (%.0f%% reduction)", len(answer), reduction)
    logger.info("stress-test: final answer:\n%s", answer)

    return {"ok": True, "answer": answer, "citations": used}


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
