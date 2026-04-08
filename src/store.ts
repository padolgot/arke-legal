// Хранилище чанков в Postgres + pgvector + tsvector.
// Знает только про базу. Не знает ни про embedder, ни про LLM.

import pg from "pg"
import {type SearchConfig} from "./defaults.js"

const BATCH_SIZE = 50

export interface SearchResult
{
    id: string
    content: string
    source: string
    chunk_index: number
    metadata: Record<string, unknown>
    created_at: string
    similarity: number
}

export interface ChunkRow
{
    source: string
    chunk_index: number
    content: string
    embedding: number[]
    metadata: Record<string, unknown>
    created_at: string
}

export interface SampleRow
{
    id: string
    content: string
}

export class ChunkStore
{
    readonly pool: pg.Pool
    readonly embeddingDim: number

    constructor(databaseUrl: string, embeddingDim: number)
    {
        this.pool = new pg.Pool({connectionString: databaseUrl})
        this.embeddingDim = embeddingDim
    }

    async init(): Promise<void>
    {
        await this.pool.query(`CREATE EXTENSION IF NOT EXISTS vector`)

        await this.pool.query(`
            CREATE TABLE IF NOT EXISTS chunks (
                id          SERIAL PRIMARY KEY,
                source      TEXT NOT NULL CHECK (length(source) > 0),
                chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),
                content     TEXT NOT NULL CHECK (length(content) > 0),
                embedding   vector(${this.embeddingDim}) NOT NULL,
                metadata    JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(metadata) = 'object'),
                created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                tsv         tsvector GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED NOT NULL
            )
        `)

        await this.pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS chunks_content_unique ON chunks (md5(content))`)
        await this.pool.query(`CREATE INDEX IF NOT EXISTS chunks_tsv_idx ON chunks USING GIN (tsv)`)
    }

    async insert(rows: ChunkRow[]): Promise<void>
    {
        for (let offset = 0; offset < rows.length; offset += BATCH_SIZE)
        {
            const end = Math.min(offset + BATCH_SIZE, rows.length)
            const placeholders: string[] = []
            const params: unknown[] = []
            let p = 1

            for (let i = offset; i < end; i++)
            {
                const r = rows[i]
                placeholders.push(`($${p}, $${p + 1}, $${p + 2}, $${p + 3}, $${p + 4}, $${p + 5})`)
                params.push(r.source, r.chunk_index, r.content, `[${r.embedding.join(",")}]`, JSON.stringify(r.metadata), r.created_at)
                p += 6
            }

            await this.pool.query(
                `INSERT INTO chunks (source, chunk_index, content, embedding, metadata, created_at)
                 VALUES ${placeholders.join(", ")}
                 ON CONFLICT (md5(content)) DO NOTHING`,
                params,
            )
        }
    }

    async search(queryVec: number[], queryText: string, cfg: SearchConfig): Promise<SearchResult[]>
    {
        const vectorStr = `[${queryVec.join(",")}]`

        const res = await this.pool.query(`
            WITH scored AS (
                SELECT id,
                       content,
                       source,
                       chunk_index,
                       metadata,
                       created_at,
                       1 - (embedding <=> $1::vector) AS cosine_raw,
                       ts_rank(tsv, plainto_tsquery('simple', $3)) AS bm25_raw
                FROM chunks
            ),
            bounds AS (
                SELECT max(bm25_raw) AS max_bm25
                FROM scored
            )
            SELECT s.id,
                   s.content,
                   s.source,
                   s.chunk_index,
                   s.metadata,
                   s.created_at,
                   (
                       $4::float * s.cosine_raw +
                       (1.0 - $4::float) * CASE
                           WHEN b.max_bm25 > 0 THEN s.bm25_raw / b.max_bm25
                           ELSE 0
                       END
                   ) AS similarity
            FROM scored s, bounds b
            ORDER BY similarity DESC
            LIMIT $2
        `, [vectorStr, cfg.k, queryText, cfg.alpha])

        return res.rows
    }

    async truncate(): Promise<void>
    {
        await this.pool.query("TRUNCATE chunks")
    }

    async sample(limit: number): Promise<SampleRow[]>
    {
        const res = await this.pool.query(
            `SELECT id, content FROM chunks ORDER BY random() LIMIT $1`,
            [limit],
        )
        return res.rows
    }

    async close(): Promise<void>
    {
        await this.pool.end()
    }
}
