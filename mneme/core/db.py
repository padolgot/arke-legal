import json

import asyncpg
import pgvector.asyncpg

from .types import Chunk, SearchHit


BATCH_SIZE = 50


def _chunk_to_row(c: Chunk) -> tuple:
    return (c.id, c.source, c.chunk_index, c.content, c.embedding, json.dumps(c.metadata), c.created_at)


class Db:
    def __init__(self, dsn: str, embedding_dim: int) -> None:
        self._dsn = dsn
        self._embedding_dim = embedding_dim

    async def open(self) -> None:
        self._pool = await asyncpg.create_pool(dsn=self._dsn, init=_init_conn)

    async def close(self) -> None:
        await self._pool.close()

    async def init_schema(self) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS chunks (
                    id          TEXT PRIMARY KEY,
                    source      TEXT NOT NULL CHECK (length(source) > 0),
                    chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),
                    content     TEXT NOT NULL CHECK (length(content) > 0),
                    embedding   vector({self._embedding_dim}) NOT NULL,
                    metadata    JSONB NOT NULL DEFAULT '{{}}' CHECK (jsonb_typeof(metadata) = 'object'),
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                    tsv         tsvector GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED NOT NULL
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS chunks_tsv_idx ON chunks USING GIN (tsv)")
            await conn.execute(f"CREATE INDEX IF NOT EXISTS chunks_embedding_idx ON chunks USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64)")

    async def insert(self, chunks: list[Chunk]) -> None:
        rows = [_chunk_to_row(c) for c in chunks]
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                for offset in range(0, len(rows), BATCH_SIZE):
                    await conn.executemany(
                        """
                        INSERT INTO chunks (id, source, chunk_index, content, embedding, metadata, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (id) DO NOTHING
                        """,
                        rows[offset : offset + BATCH_SIZE],
                    )

    async def search(self, query_vec: list[float], query_text: str, alpha: float, k: int) -> list[SearchHit]:
        """Hybrid search: alpha*cosine + (1-alpha)*bm25_normalized."""
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
            SearchHit(chunk=Chunk.from_dict(dict(r)), similarity=float(r["similarity"]))
            for r in rows
        ]

    async def fetch_all(self) -> list[Chunk]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT id, source, chunk_index, content, embedding, metadata, created_at FROM chunks")
        return [Chunk.from_dict(dict(r)) for r in rows]

    async def truncate(self) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute("TRUNCATE chunks")

    async def sample(self, limit: int) -> list[Chunk]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT id, source, chunk_index, content, embedding, metadata, created_at FROM chunks ORDER BY random() LIMIT $1", limit)
        return [Chunk.from_dict(dict(r)) for r in rows]


async def _init_conn(conn: asyncpg.Connection) -> None:
    await pgvector.asyncpg.register_vector(conn)
