import json

import asyncpg
import pgvector.asyncpg

from .types import Chunk, SearchHit


# Trade-off between round-trips and request size. Not benchmarked.
BATCH_SIZE = 50


class Db:
    def __init__(self, dsn: str, embedding_dim: int) -> None:
        self._dsn = dsn
        self._embedding_dim = embedding_dim
        self._pool: asyncpg.Pool | None = None

    async def open(self) -> None:
        self._pool = await asyncpg.create_pool(dsn=self._dsn, init=_init_conn)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def init_schema(self) -> None:
        dim = self._embedding_dim
        async with self._pool.acquire() as conn:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS chunks (
                    id          SERIAL PRIMARY KEY,
                    source      TEXT NOT NULL CHECK (length(source) > 0),
                    chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),
                    content     TEXT NOT NULL CHECK (length(content) > 0),
                    embedding   vector({dim}) NOT NULL,
                    metadata    JSONB NOT NULL DEFAULT '{{}}' CHECK (jsonb_typeof(metadata) = 'object'),
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                    tsv         tsvector GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED NOT NULL
                )
            """)
            # md5(content) unique index protects against duplicates on re-ingest.
            await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS chunks_content_unique ON chunks (md5(content))")
            await conn.execute("CREATE INDEX IF NOT EXISTS chunks_tsv_idx ON chunks USING GIN (tsv)")

    async def insert(self, chunks: list[Chunk]) -> None:
        # asyncpg sends JSONB as a string; we serialize here rather than
        # registering a codec — metadata is small and this path is hot enough.
        rows = [
            (c.source, c.chunk_index, c.content, c.embedding, json.dumps(c.metadata), c.created_at)
            for c in chunks
        ]
        async with self._pool.acquire() as conn:
            for offset in range(0, len(rows), BATCH_SIZE):
                await conn.executemany(
                    """
                    INSERT INTO chunks (source, chunk_index, content, embedding, metadata, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (md5(content)) DO NOTHING
                    """,
                    rows[offset : offset + BATCH_SIZE],
                )

    async def search(self, query_vec: list[float], query_text: str, alpha: float, k: int) -> list[SearchHit]:
        """Hybrid search in one query. The `scored` CTE computes raw cosine
        and raw BM25 per row; `bounds` takes max(bm25) to normalize into
        [0, 1]; the final SELECT blends them as alpha*cosine + (1-alpha)*bm25_norm."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                WITH scored AS (
                    SELECT id, content, source, chunk_index, metadata, created_at, embedding,
                           1 - (embedding <=> $1) AS cosine_raw,
                           ts_rank(tsv, plainto_tsquery('simple', $3)) AS bm25_raw
                    FROM chunks
                ),
                bounds AS (SELECT max(bm25_raw) AS max_bm25 FROM scored)
                SELECT s.id, s.content, s.source, s.chunk_index, s.metadata, s.created_at, s.embedding,
                       ($4 * s.cosine_raw +
                        (1.0 - $4) * CASE WHEN b.max_bm25 > 0 THEN s.bm25_raw / b.max_bm25 ELSE 0 END
                       ) AS similarity
                FROM scored s, bounds b
                ORDER BY similarity DESC
                LIMIT $2
                """,
                query_vec, k, query_text, alpha,
            )
        return [
            SearchHit(
                chunk=Chunk(
                    id=r["id"],
                    source=r["source"],
                    chunk_index=r["chunk_index"],
                    content=r["content"],
                    embedding=list(r["embedding"]),
                    metadata=json.loads(r["metadata"]) if isinstance(r["metadata"], str) else r["metadata"],
                    created_at=r["created_at"],
                ),
                similarity=float(r["similarity"]),
            )
            for r in rows
        ]

    async def truncate(self) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute("TRUNCATE chunks")

    async def sample(self, limit: int) -> list[tuple[int, str]]:
        """Random chunk sample used by sweep to auto-generate eval cases."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT id, content FROM chunks ORDER BY random() LIMIT $1", limit)
        return [(r["id"], r["content"]) for r in rows]


async def _init_conn(conn: asyncpg.Connection) -> None:
    # Enables pgvector and registers the vector type adapter so list[float]
    # can be passed as a query parameter.
    await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
    await pgvector.asyncpg.register_vector(conn)
