import pg from "pg"
import {chunk} from "./chunker.js"
import {embed} from "./embedder.js"

export interface Doc
{
    content: string
    source?: string
    created_at?: string
    metadata?: Record<string, unknown>
}

export async function ingest(docs: Doc[])
{
    const db = new pg.Client(process.env.DATABASE_URL)
    await db.connect()

    try
    {
        for (const doc of docs)
        {
            const chunks = chunk(doc.content)
            if (chunks.length === 0) continue

            const vectors = await embed(chunks)

            for (let i = 0; i < chunks.length; i++)
            {
                await db.query(
                    "INSERT INTO chunks (source, chunk_index, content, embedding, metadata, created_at) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (md5(content)) DO NOTHING",
                    [
                        doc.source ?? "unknown",
                        i,
                        chunks[i],
                        `[${vectors[i].join(",")}]`,
                        JSON.stringify(doc.metadata ?? {}),
                        doc.created_at ?? new Date().toISOString(),
                    ]
                )
            }

            console.log(`${doc.source}: ${chunks.length} chunks`)
        }
    }
    finally
    {
        await db.end()
    }
}
