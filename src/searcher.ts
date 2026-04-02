import pg from "pg"
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

    const db = new pg.Client(process.env.DATABASE_URL)
    await db.connect()

    const res = await db.query(`
        SELECT content,
               source,
               metadata,
               created_at,
               1 - (embedding <=> $1::vector) as similarity
        FROM chunks
        ORDER BY embedding <=> $1::vector
        LIMIT $2
    `, [vectorStr, limit])

    await db.end()
    return res.rows
}
