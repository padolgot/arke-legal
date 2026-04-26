"""Stress-test handler — adversarial mosaic from the litigator's archive.

Pipeline:
  1. hybrid retrieval on the argument
  2. cheap gate on top similarity (drop if corpus is off-topic)
  3. per-doc LLM filter — adversarial chunk selection (parallel across docs)
  4. mosaic LLM — cluster passages into SUPPORTS/ATTACKS authorities with labels
  5. trimmer LLM — strip procedural narrative, preserving verbatim ratio
"""
import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from itertools import zip_longest

import numpy as np

from .bm25 import BM25Index
from .config import Config
from .models import LLM, Models
from .types import Chunk, Doc, SearchHit

logger = logging.getLogger(__name__)

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

MAX_DOCS = 10
DOC_MAX_TOKENS = 50000
RETRIEVAL_K = 40
MAX_WORKERS = 3
MAX_PASSAGES_PER_DOC = 6
MAX_CHUNKS_PER_PASSAGE = 2
GATE = 0.3

INSUFFICIENT_MSG = "Insufficient on-topic material in the corpus. Add more documents and try again."


def handle(
    request: dict,
    docs: dict[str, Doc],
    index: ChunkIndex,
    bm25: BM25Index,
    cfg: Config,
    models: Models,
) -> dict:
    argument = request.get("argument") or request.get("query") or ""
    if not argument:
        return {"ok": False, "error": "argument is required"}

    logger.info("stress-test: argument (%d chars)", len(argument))

    q_vec = np.array(models.embedder.embed([argument])[0], dtype=np.float32)
    hits = hybrid_search(index, bm25, q_vec, argument, RETRIEVAL_K, cfg.alpha)

    top_score = hits[0].similarity if hits else 0.0
    logger.info("stress-test: top similarity = %.3f (gate %.2f)", top_score, GATE)
    if top_score < GATE:
        return {"ok": True, "answer": INSUFFICIENT_MSG, "citations": []}

    by_doc: dict[str, float] = {}
    for h in hits:
        did = h.chunk.doc_id
        if did not in by_doc or h.similarity > by_doc[did]:
            by_doc[did] = h.similarity
    top_doc_ids = sorted(by_doc, key=lambda d: by_doc[d], reverse=True)[:MAX_DOCS]
    logger.info("stress-test: %d candidate docs from %d chunks", len(top_doc_ids), len(hits))

    fit_docs: list[Doc] = []
    for doc_id in top_doc_ids:
        doc = docs[doc_id]
        # ~4 chars per token is a rough but stable estimator; LLMs handle up
        # to ~50k cleanly, beyond that we'd hit context-window risk.
        est_tokens = sum(len(c.clean) for c in doc.chunks) // 4
        if est_tokens > DOC_MAX_TOKENS:
            logger.info("stress-test: skip %s (%dk tokens)", doc.label, est_tokens // 1000)
            continue
        fit_docs.append(doc)

    mosaics: dict[str, list[Chunk]] = {}
    if fit_docs:
        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(fit_docs))) as ex:
            results = list(ex.map(lambda d: (d, _per_doc_filter(argument, d, models.llm)), fit_docs))
        for doc, indices in results:
            if indices:
                mosaics[doc.id] = [doc.chunks[i] for i in indices]
            logger.info("stress-test: %s → %d/%d chunks", doc.label, len(indices), len(doc.chunks))

    if not mosaics:
        return {"ok": True, "answer": "", "citations": []}

    per_doc: list[list[dict]] = []
    for doc_id, chunks in mosaics.items():
        doc = docs[doc_id]
        case_name = doc.metadata.get("case_name", "") or ""
        doc_passages = [
            {"doc_id": doc.id, "filename": doc.label, "case_name": case_name, "text": text}
            for text in _merge_adjacent(chunks)[:MAX_PASSAGES_PER_DOC]
        ]
        if doc_passages:
            per_doc.append(doc_passages)

    # Round-robin so the first slots in the mosaic LLM context span all docs —
    # otherwise a single rich doc dominates and rivals get truncated.
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

    # Drop multi-source clusters (LLM occasionally merges across docs) and
    # keep only the first cluster per source doc — one authority, one point.
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
        return {"ok": True, "answer": "", "citations": []}

    parts: list[str] = []
    used: list[dict] = []
    for cluster in clusters:
        first = passages[cluster["passages"][0] - 1]
        case_name = first["case_name"]
        stance = cluster["stance"]
        label = cluster["label"]
        header = f"[{stance}] {case_name} · {label}" if case_name else f"[{stance}] {label}"
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

    answer = models.llm.chat(TRIMMER_SYSTEM_PROMPT, raw_answer).strip()
    reduction = (1 - len(answer) / max(len(raw_answer), 1)) * 100
    logger.info("stress-test: trimmed → %dchars (%.0f%% reduction)", len(answer), reduction)
    logger.info("stress-test: final answer:\n%s", answer)

    return {"ok": True, "answer": answer, "citations": used}


def _per_doc_filter(argument: str, doc: Doc, llm: LLM) -> list[int]:
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


def _merge_adjacent(chunks: list[Chunk]) -> list[str]:
    """Group chunks by contiguous chunk_index, split long runs into sub-runs
    of at most MAX_CHUNKS_PER_PASSAGE. Returns one passage text per sub-run."""
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


class ChunkIndex:
    """Dense in-RAM matrix of all chunk embeddings.

    Symmetric with BM25Index — built once at ingest end, queried per-search.
    Replaces the per-chunk Python loop in cosine retrieval with a single
    numpy.matmul; for 121k vectors at 1536 dims, ~5s → ~1ms.

    Skips chunks whose embedding is None (failed-embed docs). Those still
    surface in BM25 — cosine just doesn't see them."""

    def __init__(self) -> None:
        self.clear()

    def clear(self) -> None:
        self._matrix: np.ndarray = np.zeros((0, 0), dtype=np.float32)
        self._norms: np.ndarray = np.zeros(0, dtype=np.float32)
        self._keys: list[str] = []
        self._chunk_map: dict[str, Chunk] = {}

    def build(self, docs: dict[str, Doc]) -> None:
        rows: list[np.ndarray] = []
        keys: list[str] = []
        chunk_map: dict[str, Chunk] = {}
        for doc in docs.values():
            for chunk in doc.chunks:
                if chunk.embedding is None:
                    continue
                key = f"{chunk.doc_id}:{chunk.chunk_index}"
                rows.append(chunk.embedding)
                keys.append(key)
                chunk_map[key] = chunk
        if rows:
            self._matrix = np.stack(rows).astype(np.float32, copy=False)
            self._norms = np.linalg.norm(self._matrix, axis=1)
            # Defensive: a zero-norm row would NaN the cosine; pin to 1.0 so the
            # row's similarity stays 0 (because q.dot(0) is 0).
            self._norms[self._norms == 0] = 1.0
        else:
            self._matrix = np.zeros((0, 0), dtype=np.float32)
            self._norms = np.zeros(0, dtype=np.float32)
        self._keys = keys
        self._chunk_map = chunk_map

    def cosine(self, q_vec: np.ndarray) -> dict[str, float]:
        q_norm = float(np.linalg.norm(q_vec))
        if q_norm == 0.0 or not self._keys:
            return {}
        sims = (self._matrix @ q_vec) / (self._norms * q_norm)
        return dict(zip(self._keys, sims.tolist()))

    def chunk(self, key: str) -> Chunk | None:
        return self._chunk_map.get(key)

    def __len__(self) -> int:
        return len(self._keys)


def hybrid_search(
    index: ChunkIndex,
    bm25: BM25Index,
    q_vec: np.ndarray,
    query: str,
    k: int,
    alpha: float,
) -> list[SearchHit]:
    cosine = index.cosine(q_vec)

    bm25_raw = bm25.scores(query)
    bm25_max = max(bm25_raw.values(), default=1.0)
    bm25_norm = {k: v / bm25_max for k, v in bm25_raw.items()} if bm25_max > 0 else {}

    all_keys = set(cosine) | set(bm25_norm)
    scored: list[tuple[str, float]] = []
    for key in all_keys:
        score = alpha * cosine.get(key, 0.0) + (1 - alpha) * bm25_norm.get(key, 0.0)
        scored.append((key, score))
    scored.sort(key=lambda x: x[1], reverse=True)

    hits: list[SearchHit] = []
    for key, score in scored[:k]:
        chunk = index.chunk(key)
        if chunk:
            hits.append(SearchHit(chunk=chunk, similarity=score))
    return hits


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
