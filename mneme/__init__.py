from __future__ import annotations

import hashlib

import httpx

from .core.chunker import chunk
from .core.config import Config
from .core.db import Db
from .core.loader import load_docs
from .core.models import chat, embed
from .core.types import Chunk, SearchHit


class Mneme:
    """RAG engine with built-in eval. Lifecycle: Mneme(cfg) → open() → work → close().
    Or use `async with Mneme(cfg) as m:` for automatic cleanup."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg.resolved()

    async def open(self) -> None:
        self.db = Db(self.cfg.database_url, self.cfg.embedding_dim)
        self.http = httpx.AsyncClient(timeout=60.0)
        await self.db.open()
        await self.db.init_schema()

    async def close(self) -> None:
        await self.db.close()
        await self.http.aclose()

    async def __aenter__(self) -> Mneme:
        await self.open()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def reset(self) -> None:
        await self.db.truncate()

    async def ingest(self, source_path: str) -> None:
        docs = load_docs(source_path)

        all_pieces = []
        piece_origins = []
        for doc in docs:
            pieces = chunk(doc.content, self.cfg.chunk_size, self.cfg.overlap)
            for i, p in enumerate(pieces):
                all_pieces.append(p)
                piece_origins.append((doc, i))

        texts = [p.overlapped() for p in all_pieces]
        vectors = await embed(self.cfg, self.http, texts)
        print(f"embedded {len(texts)} chunks in one call")

        chunks = [
            Chunk(
                id=hashlib.md5(f"{doc.source}:{idx}:{p.clean}".encode()).hexdigest(),
                source=doc.source,
                chunk_index=idx,
                content=p.clean,
                embedding=vectors[i],
                metadata=doc.metadata,
                created_at=doc.created_at,
            )
            for i, (p, (doc, idx)) in enumerate(zip(all_pieces, piece_origins))
        ]
        await self.db.insert(chunks)
        print(f"ingest done: {len(chunks)} chunks from {len(docs)} docs")

    async def ask(self, query: str) -> str:
        vectors = await embed(self.cfg, self.http, [query])
        hits = await self.db.search(vectors[0], query, self.cfg.alpha, self.cfg.k)

        if hits:
            return await self._answer_with_context(query, hits)
        else:
            return await self._answer_without_context(query)

    async def _answer_with_context(self, query: str, hits: list[SearchHit]) -> str:
        prompt = "You are a personal knowledge assistant. You answer questions based ONLY on the provided context. If the context doesn't contain enough information, say so honestly. Answer in the same language as the question. Be concise and direct."

        parts: list[str] = []
        for i, h in enumerate(hits):
            c = h.chunk
            date = c.created_at.date().isoformat()
            parts.append(f"[{i + 1}] ({date}, {c.source}, sim={h.similarity:.3f})\n{c.content}")
        context = "\n\n".join(parts)

        return await chat(self.cfg, self.http, prompt, f"Context:\n{context}\n\nQuestion: {query}")

    async def _answer_without_context(self, query: str) -> str:
        prompt = "You are a knowledge assistant. Answer the question directly based on your general knowledge. Answer in the same language as the question. Be concise and direct."
        return await chat(self.cfg, self.http, prompt, query)


# eval imports go after Mneme class to avoid circular import:
# eval/sweep.py and eval/gen.py import Mneme from this module
from .eval import Eval, EvalMetrics, SweepRow  # noqa: E402

__all__ = [
    "Mneme",
    "Config",
    "Eval",
    "SweepRow",
    "EvalMetrics",
]
