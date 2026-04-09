import httpx

from .core.chunker import chunk
from .core.config import MnemeConfig, resolve_config
from .core.db import Db
from .core.loader import load_docs
from .core.models import chat, embed
from .core.types import Chunk, SearchHit


SYSTEM_WITH_CONTEXT = "You are a personal knowledge assistant. You answer questions based ONLY on the provided context. If the context doesn't contain enough information, say so honestly. Answer in the same language as the question. Be concise and direct."
SYSTEM_NO_CONTEXT = "You are a knowledge assistant. Answer the question directly based on your general knowledge. Answer in the same language as the question. Be concise and direct."


class Mneme:
    """RAG engine with built-in eval. Lifecycle: Mneme(cfg) → open() → work → close().
    Or use `async with Mneme(cfg) as m:` for automatic cleanup."""

    def __init__(self, cfg: MnemeConfig) -> None:
        self.cfg = resolve_config(cfg)
        self.db = Db(cfg.database_url, cfg.embedding_dim)
        self._http = httpx.AsyncClient(timeout=60.0)

    async def open(self) -> None:
        await self.db.open()

    async def close(self) -> None:
        await self.db.close()
        await self._http.aclose()

    async def __aenter__(self) -> "Mneme":
        await self.open()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def create_schema(self) -> None:
        await self.db.init_schema()
        print(f"schema ready, embedding dim={self.cfg.embedding_dim}")

    async def reset(self) -> None:
        """Drops all ingested chunks. Paired with re-ingest in eval sweeps."""
        await self.db.truncate()

    async def ingest(self, source_path: str) -> None:
        docs = load_docs(source_path)
        print(
            f"ingest: {len(docs)} docs from {source_path} | "
            f"chunk_size={self.cfg.chunk_size} overlap={self.cfg.overlap}"
        )

        total = 0
        for doc in docs:
            pieces = chunk(doc.content, self.cfg.chunk_size, self.cfg.overlap)
            # Embed the overlapped text (neighbor context), store only the clean part.
            vectors = await embed(
                self._http, self.cfg.embedder_url, self.cfg.embedder_model,
                [p.overlapped() for p in pieces],
            )
            chunks = [
                Chunk(
                    source=doc.source,
                    chunk_index=i,
                    content=p.clean,
                    embedding=vectors[i],
                    metadata=doc.metadata,
                    created_at=doc.created_at,
                )
                for i, p in enumerate(pieces)
            ]
            await self.db.insert(chunks)
            total += len(chunks)

        print(f"ingest: done, {total} chunks total")

    async def ask(self, query: str) -> str:
        vectors = await embed(self._http, self.cfg.embedder_url, self.cfg.embedder_model, [query])
        hits = await self.db.search(vectors[0], query, self.cfg.alpha, self.cfg.k)

        if hits:
            context = _format_context(hits)
            return await chat(
                self._http, self.cfg.inference_url, self.cfg.inference_model,
                SYSTEM_WITH_CONTEXT, f"Context:\n{context}\n\nQuestion: {query}",
            )
        return await chat(
            self._http, self.cfg.inference_url, self.cfg.inference_model,
            SYSTEM_NO_CONTEXT, query,
        )


def _format_context(hits: list[SearchHit]) -> str:
    """Formats search hits as a numbered context block with date, source
    and similarity — helps the LLM cite sources and gauge confidence."""
    parts: list[str] = []
    for i, h in enumerate(hits):
        c = h.chunk
        date = c.created_at.date().isoformat()
        parts.append(f"[{i + 1}] ({date}, {c.source}, sim={h.similarity:.3f})\n{c.content}")
    return "\n\n".join(parts)
