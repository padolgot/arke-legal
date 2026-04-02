import {pool} from "../db.js"
import {embed} from "./embedder.js"

export interface SearchResult
{
    content: string
    source: string
    metadata: Record<string, unknown>
    created_at: string
    similarity: number
}

export async function search(query: string, limit: number = 5): Promise<SearchResult[]>
{
    const [vec] = await embed([query])
    const vectorStr = `[${vec.join(",")}]`

    const res = await pool.query(`
        SELECT content,
               source,
               metadata,
               created_at,
               1 - (embedding <=> $1::vector) as similarity
        FROM chunks
        ORDER BY embedding <=> $1::vector
        LIMIT $2
    `, [vectorStr, limit])

    return res.rows
}
