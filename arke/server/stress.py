"""Stress-test handler — adversarial mosaic from the litigator's archive.

Pipeline:
  1. hybrid retrieval on the argument
  2. cheap gate on top similarity (drop if corpus is off-topic)
  3. per-doc LLM filter — adversarial chunk selection (parallel across docs)
  4. mosaic LLM — select passages per doc (selection only, no generation)
  5. trimmer LLM — strip procedural narrative, preserving verbatim ratio
Headings come deterministically from source metadata, never the model.
"""
import json
import logging
import math
import re
from concurrent.futures import ThreadPoolExecutor

import numpy as np

from .bm25 import BM25Index
from .config import Config
from .models import LLM, Models
from .types import Chunk, Doc, SearchHit

logger = logging.getLogger(__name__)

from .prompts import MOSAIC_SYSTEM_PROMPT, PER_DOC_PROMPT, TRIMMER_SYSTEM_PROMPT

TOP_DOCS_BUFFER = 14
TOP_DOCS_FIT = 10
DOC_MAX_TOKENS = 50000
RETRIEVAL_K = 40
MAX_WORKERS = 3
MAX_PASSAGES_PER_DOC = 6
MAX_CHUNKS_PER_PASSAGE = 2
# Uncalibrated. Picked from the sky early in development. Functionally a
# no-op so far — top similarity in real tests sits at 0.6-0.7. Calibrate
# against eval_cases_sample_50.jsonl when retrieval quality becomes the
# bottleneck.
GATE = 0.3
# Multiplicative log-boost on retrieval ranking: foundational authorities
# (heavily cited inside the corpus) outrank lexically-similar but never-cited
# docs. score = cosine * (1 + α · log(1 + cite_in_count)). α=0.5 → cite_in=12
# yields ~2.28× boost; cite_in=100 → ~3.30×; cite_in=0 stays at 1.0×. Keeps
# uncited alpha-layer reachable, avoids "crush" of long-tail docs.
CITE_BOOST_ALPHA = 0.5

INSUFFICIENT_MSG = "Insufficient on-topic material in the corpus. Add more documents and try again."
NO_ADVERSARIAL_MSG = (
    "Arke surfaced no adversarial authority on this argument. Either the "
    "position is on-doctrine, or the corpus lacks contestable counter-authority "
    "on this point."
)


def _clean_title(title: str) -> str:
    """Clean EU CELLAR titles which use '.#'-separated metadata segments.
    Pick the first segment that reads as 'X v Y'; else fall back to the
    first non-empty segment. UK titles pass through unchanged."""
    if ".#" not in title:
        return title.strip()
    parts = [p.strip() for p in title.split(".#") if p.strip()]
    for p in parts:
        low = f" {p.lower()} "
        if " v " in low or " v. " in low:
            return p
    return parts[0] if parts else title


def _footer_line(p: dict) -> str:
    """Per-cluster footer: corpus_path · cited N× · date. Citation + title live in the heading."""
    bits: list[str] = [p["corpus_path"] or p["filename"]]
    cic = p.get("cite_in_count") or 0
    if cic:
        bits.append(f"cited {cic}×")
    if p.get("date"):
        bits.append(p["date"])
    return f"— {' · '.join(bits)}"


def _heading(p: dict) -> str:
    """Deterministic cluster heading from source metadata. No LLM, ever.
    Prefers party_slug (compact, abbrevs CMA/Ofcom/etc) over full case_name."""
    citation = p.get("citation", "")
    if citation and not citation.startswith("["):
        citation = f"[{citation}]"
    slug = (p.get("party_slug") or "").replace("-", " ").strip()
    title = slug or _clean_title(p.get("case_name") or "")
    if citation and title:
        return f"{citation} · {title}"
    return citation or title or p.get("filename", "")


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

    def _doc_rank(doc_id: str) -> float:
        cite_in = docs[doc_id].metadata.get("cite_in_count", 0) or 0
        return by_doc[doc_id] * (1 + CITE_BOOST_ALPHA * math.log(1 + cite_in))

    top_doc_ids = sorted(by_doc, key=_doc_rank, reverse=True)[:TOP_DOCS_BUFFER]
    logger.info("stress-test: %d candidate docs from %d chunks", len(top_doc_ids), len(hits))

    fit_docs: list[Doc] = []
    for doc_id in top_doc_ids:
        if len(fit_docs) >= TOP_DOCS_FIT:
            break
        doc = docs[doc_id]
        # ~4 chars per token is a rough but stable estimator. Dirty hack —
        # Pasha-approved while we sit on OpenAI Tier 1 TPM: some judgments
        # run 100-200k tokens and would blow per-doc-filter's context budget.
        # The TOP_DOCS_BUFFER above oversamples so this drop still leaves
        # ~TOP_DOCS_FIT survivors. Lift the cap when the tier upgrades.
        est_tokens = sum(len(c.clean) for c in doc.chunks) // 4
        if est_tokens > DOC_MAX_TOKENS:
            logger.info("stress-test: skip %s (%dk tokens, TPM hack)", doc.label, est_tokens // 1000)
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
        return {"ok": True, "answer": NO_ADVERSARIAL_MSG, "citations": []}

    # Build per-doc passages with rich metadata. Each doc keeps its own
    # passage list — single-source preserved structurally through the rest
    # of the pipeline (no flat pool, no round-robin, no cross-doc merging).
    doc_passages: dict[str, list[dict]] = {}
    for doc_id, chunks in mosaics.items():
        doc = docs[doc_id]
        meta = doc.metadata
        passages = [
            {
                "doc_id": doc.id,
                "filename": doc.label,
                "case_name": meta.get("title") or meta.get("case_name") or "",
                "party_slug": meta.get("party_slug", "") or "",
                "corpus_path": meta.get("corpus_path", "") or doc.source or doc.label,
                "citation": (
                    meta.get("neutral_citation")
                    or meta.get("celex")
                    or meta.get("ecli")
                    or ""
                ),
                "date": meta.get("date", "") or "",
                "cite_in_count": meta.get("cite_in_count", 0) or 0,
                "text": text,
            }
            for text in _merge_adjacent(chunks)[:MAX_PASSAGES_PER_DOC]
        ]
        if passages:
            doc_passages[doc.id] = passages

    if not doc_passages:
        return {"ok": True, "answer": NO_ADVERSARIAL_MSG, "citations": []}

    # Curate: strong LLM sees ALL docs at once, drops noise/duplicates/
    # off-key passages. Selection only — no labels, no generation. Headings
    # are written deterministically from metadata downstream.
    key_to_doc: dict[str, str] = {}
    curate_input: dict[str, list[str]] = {}
    for i, doc_id in enumerate(doc_passages, start=1):
        key = f"doc_{i}"
        key_to_doc[key] = doc_id
        curate_input[key] = [p["text"] for p in doc_passages[doc_id]]

    user_msg = (
        f"Argument:\n{argument}\n\n"
        "Candidate passages by document — curate (drop, never reorder, never "
        "merge across docs). Output JSON:\n"
        + json.dumps(curate_input, ensure_ascii=False, indent=2)
    )
    raw = models.strong_llm.chat(MOSAIC_SYSTEM_PROMPT, user_msg)
    curated = _parse_curate(raw, curate_input)
    logger.info(
        "stress-test: curate — kept %d/%d docs",
        len(curated), len(curate_input),
    )

    if not curated:
        logger.info("stress-test: raw curate output (kept 0):\n%s", raw[:2000])
        return {"ok": True, "answer": NO_ADVERSARIAL_MSG, "citations": []}

    parts: list[str] = []
    used: list[dict] = []
    for key, decision in curated.items():
        doc_id = key_to_doc[key]
        passages = doc_passages[doc_id]
        keep = [i for i in decision["keep"] if 0 <= i < len(passages)]
        if not keep:
            continue
        parts.append(f"## {_heading(passages[keep[0]])}")
        for i in keep:
            passage = passages[i]
            parts.append(f"> {passage['text']}")
            used.append(passage)
        parts.append(_footer_line(passages[keep[0]]))
        logger.info(
            "  %s (%s): keep=%s",
            key,
            (passages[0]["case_name"] or "(no name)")[:60],
            keep,
        )

    if not parts:
        return {"ok": True, "answer": NO_ADVERSARIAL_MSG, "citations": []}

    raw_answer = "\n\n".join(parts)
    logger.info(
        "stress-test: mosaic — %d clusters, %d passages, raw=%d chars",
        len(curated), len(used), len(raw_answer),
    )

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


def _parse_curate(raw: str, curate_input: dict[str, list[str]]) -> dict[str, dict]:
    """Parse strong-LLM curation output: {doc_key: {keep: [int]}}.
    Validates keys against curate_input, indices against per-doc passage count."""
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if not match:
        return {}
    try:
        data = json.loads(match.group(0))
    except (json.JSONDecodeError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    out: dict[str, dict] = {}
    for key, val in data.items():
        if key not in curate_input or not isinstance(val, dict):
            continue
        keep = val.get("keep")
        if not isinstance(keep, list):
            continue
        n = len(curate_input[key])
        valid_keep = [i for i in keep if isinstance(i, int) and 0 <= i < n]
        if valid_keep:
            out[key] = {"keep": valid_keep}
    return out
