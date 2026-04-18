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
import logging
import time
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
    "You are a legal research assistant. "
    "Answer based only on the provided documents. "
    "Be concise and cite the source for every claim."
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

    citations = [
        {
            "source": _source_label(docs[h.chunk.doc_id]),
            "text": h.chunk.clean[:200],
            "score": round(h.similarity, 3),
        }
        for h in hits
    ]
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
