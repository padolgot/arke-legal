import {pool} from "../db.js"
import {chunk} from "./chunker.js"
import {embed} from "./embedder.js"

export interface Doc
{
    content: string
    source?: string
    created_at?: string
    metadata?: Record<string, unknown>
}

const BATCH_SIZE = 50

export async function ingest(docs: Doc[])
{
    for (const doc of docs)
    {
        const chunks = chunk(doc.content)
        if (chunks.length === 0) continue

        const vectors = await embed(chunks)

        for (let offset = 0; offset < chunks.length; offset += BATCH_SIZE)
        {
            const end = Math.min(offset + BATCH_SIZE, chunks.length)
            const values: string[] = []
            const params: unknown[] = []
            let p = 1

            for (let i = offset; i < end; i++)
            {
                values.push(`($${p}, $${p + 1}, $${p + 2}, $${p + 3}, $${p + 4}, $${p + 5})`)
                params.push(
                    doc.source ?? "unknown",
                    i,
                    chunks[i],
                    `[${vectors[i].join(",")}]`,
                    JSON.stringify(doc.metadata ?? {}),
                    doc.created_at ?? new Date().toISOString(),
                )
                p += 6
            }

            await pool.query(
                `INSERT INTO chunks (source, chunk_index, content, embedding, metadata, created_at) VALUES ${values.join(", ")} ON CONFLICT (md5(content)) DO NOTHING`,
                params,
            )
        }

        console.log(`${doc.source}: ${chunks.length} chunks`)
    }
}
