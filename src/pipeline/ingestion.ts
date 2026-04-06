import {readFileSync, readdirSync, statSync} from "fs"
import {basename} from "path"
import {pool} from "../db.js"
import {chunk} from "./chunker.js"
import {embed} from "./embedder.js"
import {type ChunkConfig} from "./config.js"

export interface Doc
{
    content: string
    source: string
    created_at: string
    metadata: Record<string, unknown>
}

const BATCH_SIZE = 50

export async function ingest(sourcePath: string, cfg: ChunkConfig)
{
    const docs = loadDocs(sourcePath)
    console.log(`ingest: ${docs.length} docs from ${sourcePath} | chunkSize=${cfg.chunkSize} overlap=${cfg.overlap}`)

    let totalChunks = 0

    for (const doc of docs)
    {
        const chunks = chunk(doc.content, cfg)
        if (chunks.length === 0) continue

        const vectors = await embed(chunks.map(c => c.overlapped()))
        const metadataJson = JSON.stringify(doc.metadata)

        for (let offset = 0; offset < chunks.length; offset += BATCH_SIZE)
        {
            const end = Math.min(offset + BATCH_SIZE, chunks.length)
            const placeholders: string[] = []
            const params: unknown[] = []
            let p = 1

            for (let i = offset; i < end; i++)
            {
                placeholders.push(`($${p}, $${p + 1}, $${p + 2}, $${p + 3}, $${p + 4}, $${p + 5})`)
                params.push(doc.source, i, chunks[i].clean, `[${vectors[i].join(",")}]`, metadataJson, doc.created_at)
                p += 6
            }

            await pool.query(
                `INSERT INTO chunks (source, chunk_index, content, embedding, metadata, created_at)
                 VALUES ${placeholders.join(", ")}
                 ON CONFLICT (md5(content)) DO NOTHING`,
                params,
            )
        }

        totalChunks += chunks.length
    }

    console.log(`ingest: done, ${totalChunks} chunks total`)
}

function loadDocs(sourcePath: string): Doc[]
{
    const files: string[] = statSync(sourcePath).isDirectory()
        ? readdirSync(sourcePath).filter(f => f.endsWith(".jsonl")).map(f => `${sourcePath}/${f}`)
        : [sourcePath]

    const docs: Doc[] = []

    for (const file of files)
    {
        const source = basename(file, ".jsonl")
        const lines = readFileSync(file, "utf-8").split("\n").filter(l => l.trim())

        for (const line of lines)
        {
            let raw: unknown
            try { raw = JSON.parse(line) }
            catch { continue }

            const d = validateDoc(raw, source)
            if (!d) continue

            docs.push(d)
        }
    }

    return docs
}

function isPlainObject(x: unknown): x is Record<string, unknown>
{
    return typeof x === "object" && x !== null && !Array.isArray(x)
}

function validateDoc(raw: unknown, fallbackSource: string): Doc | null
{
    if (!isPlainObject(raw)) return null
    if (typeof raw.content !== "string" || raw.content.trim().length === 0) return null

    const source = (typeof raw.source === "string" && raw.source.length > 0)
        ? raw.source
        : fallbackSource

    let created_at = new Date().toISOString()
    if (typeof raw.created_at === "string")
    {
        const t = Date.parse(raw.created_at)
        if (!Number.isNaN(t)) created_at = new Date(t).toISOString()
    }

    const metadata = isPlainObject(raw.metadata) ? raw.metadata : {}

    return { content: raw.content, source, created_at, metadata }
}
