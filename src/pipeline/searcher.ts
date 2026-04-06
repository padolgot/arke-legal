import {pool} from "../db.js"
import {embed} from "./embedder.js"
import {type SearchConfig} from "./config.js"

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

export async function search(query: string, cfg: SearchConfig): Promise<SearchResult[]>
{
    const [vec] = await embed([query])
    const vectorStr = `[${vec.join(",")}]`

    const res = await pool.query(`
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
    `, [vectorStr, cfg.k, query, cfg.alpha])

    return res.rows
}
