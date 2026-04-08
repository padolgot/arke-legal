import {readFileSync, readdirSync, statSync} from "fs"
import {basename} from "path"
import {chunk} from "./chunker.js"
import {resolveConfig, type MnemeConfig, type ChunkConfig} from "./defaults.js"
import {Embedder} from "./embedder.js"
import {Llm} from "./llm.js"
import {ChunkStore, type SearchResult} from "./store.js"
import {score, type EvalCase, type EvalMetrics} from "./eval.js"
import {type Preset} from "./presets.js"

// Публичные типы данных, которые пересекают границу класса.

export interface Doc
{
    content: string
    source: string
    created_at: string
    metadata: Record<string, unknown>
}

export interface SweepRow
{
    cfg: Preset
    metrics: EvalMetrics
}

export {type SearchResult} from "./store.js"
export {type EvalMetrics, type EvalCase} from "./eval.js"

interface ChunkGroup
{
    chunk: ChunkConfig
    searches: Preset["search"][]
}

// Mneme — тонкий оркестратор. Сам ничего не делает руками,
// только связывает Embedder, Llm и ChunkStore в сценарии ingest/ask/sweep.

export class Mneme
{
    readonly cfg: MnemeConfig
    readonly embedder: Embedder
    readonly llm: Llm
    readonly store: ChunkStore

    constructor(input: {
        databaseUrl: string
        embedderUrl?: string
        embedderModel?: string
        embeddingDim?: number
        inferenceUrl?: string
        inferenceModel?: string
        chunk?: {chunkSize?: number; overlap?: number}
        search?: {alpha?: number; k?: number}
    })
    {
        this.cfg = resolveConfig(input)
        this.embedder = new Embedder(this.cfg.embedderUrl, this.cfg.embedderModel)
        this.llm = new Llm(this.cfg.inferenceUrl, this.cfg.inferenceModel)
        this.store = new ChunkStore(this.cfg.databaseUrl, this.cfg.embeddingDim)
    }

    async init(): Promise<void>
    {
        await this.store.init()
        console.log(`init: schema ready, embedding dim=${this.cfg.embeddingDim}`)
    }

    async ingest(sourcePath: string): Promise<void>
    {
        await this.ingestWith(sourcePath, this.cfg.chunk)
    }

    async ask(query: string): Promise<string>
    {
        const [vec] = await this.embedder.embed([query])
        const results = await this.store.search(vec, query, this.cfg.search)
        const context = results.length > 0 ? formatContext(results) : null
        return this.llm.answer(query, context)
    }

    async sweep(presets: Preset[], limit: number, sourcePath: string): Promise<SweepRow[]>
    {
        const groups = groupByChunk(presets)
        const rows: SweepRow[] = []
        let i = 0

        for (const group of groups)
        {
            await this.store.truncate()
            await this.ingestWith(sourcePath, group.chunk)

            const cases = await this.makeCases(limit)
            console.log(`sweep: generated ${cases.length} cases from ${limit} chunks`)

            for (const search of group.searches)
            {
                i++
                console.log(`\nsweep [${i}/${presets.length}] chunkSize=${group.chunk.chunkSize} overlap=${group.chunk.overlap} alpha=${search.alpha} k=${search.k}`)

                const perCase: {results: SearchResult[]; expected_ids: string[]}[] = []
                for (const c of cases)
                {
                    const [vec] = await this.embedder.embed([c.query])
                    const results = await this.store.search(vec, c.query, search)
                    perCase.push({results, expected_ids: c.expected_ids})
                }
                const metrics = score(perCase)
                rows.push({cfg: {chunk: group.chunk, search}, metrics})

                console.log(`sweep: P=${metrics.precision.toFixed(3)} R=${metrics.recall.toFixed(3)} MRR=${metrics.mrr.toFixed(3)}`)
            }
        }

        rows.sort((a, b) => b.metrics.mrr - a.metrics.mrr)
        printTable(rows)

        return rows
    }

    async close(): Promise<void>
    {
        await this.store.close()
    }

    private async ingestWith(sourcePath: string, chunkCfg: ChunkConfig): Promise<void>
    {
        const docs = loadDocs(sourcePath)
        console.log(`ingest: ${docs.length} docs from ${sourcePath} | chunkSize=${chunkCfg.chunkSize} overlap=${chunkCfg.overlap}`)

        let totalChunks = 0

        for (const doc of docs)
        {
            const chunks = chunk(doc.content, chunkCfg)
            if (chunks.length === 0) continue

            const vectors = await this.embedder.embed(chunks.map(c => c.overlapped()))

            const rows = chunks.map((c, i) => ({
                source: doc.source,
                chunk_index: i,
                content: c.clean,
                embedding: vectors[i],
                metadata: doc.metadata,
                created_at: doc.created_at,
            }))

            await this.store.insert(rows)
            totalChunks += chunks.length
        }

        console.log(`ingest: done, ${totalChunks} chunks total`)
    }

    private async makeCases(limit: number): Promise<EvalCase[]>
    {
        const samples = await this.store.sample(limit)
        const cases: EvalCase[] = []

        for (const row of samples)
        {
            const questions = await this.llm.generateQuestions(row.content)
            for (const q of questions)
            {
                cases.push({query: q, expected_ids: [row.id]})
            }
        }

        return cases
    }
}

// Stateless-хелперы.

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

function validateDoc(raw: unknown, fallbackSource: string): Doc | null
{
    if (typeof raw !== "object" || raw === null || Array.isArray(raw)) return null
    const r = raw as Record<string, unknown>

    if (typeof r.content !== "string" || r.content.trim().length === 0) return null

    const source = (typeof r.source === "string" && r.source.length > 0)
        ? r.source
        : fallbackSource

    let created_at = new Date().toISOString()
    if (typeof r.created_at === "string")
    {
        const t = Date.parse(r.created_at)
        if (!Number.isNaN(t)) created_at = new Date(t).toISOString()
    }

    const metadata = (typeof r.metadata === "object" && r.metadata !== null && !Array.isArray(r.metadata))
        ? r.metadata as Record<string, unknown>
        : {}

    return {content: r.content, source, created_at, metadata}
}

function groupByChunk(presets: Preset[]): ChunkGroup[]
{
    const groups: ChunkGroup[] = []

    for (const p of presets)
    {
        const existing = groups.find(g => g.chunk.chunkSize === p.chunk.chunkSize && g.chunk.overlap === p.chunk.overlap)
        if (existing)
        {
            existing.searches.push(p.search)
        }
        else
        {
            groups.push({chunk: p.chunk, searches: [p.search]})
        }
    }

    return groups
}

function formatContext(results: SearchResult[]): string
{
    return results
        .map((r, i) =>
        {
            const date = new Date(r.created_at).toISOString().slice(0, 10)
            return `[${i + 1}] (${date}, ${r.source}, sim=${r.similarity.toFixed(3)})\n${r.content}`
        })
        .join("\n\n")
}

function printTable(rows: SweepRow[]): void
{
    console.log(`\nRESULTS (sorted by MRR desc):`)
    console.log(`chunkSize | overlap | alpha |  k | precision | recall |  MRR`)
    console.log(`----------|---------|-------|----|-----------|--------|------`)
    for (const r of rows)
    {
        console.log(
            `${r.cfg.chunk.chunkSize.toString().padStart(9)}`
            + ` | ${r.cfg.chunk.overlap.toString().padStart(7)}`
            + ` | ${r.cfg.search.alpha.toFixed(1).padStart(5)}`
            + ` | ${r.cfg.search.k.toString().padStart(2)}`
            + ` | ${r.metrics.precision.toFixed(3).padStart(9)}`
            + ` | ${r.metrics.recall.toFixed(3).padStart(6)}`
            + ` | ${r.metrics.mrr.toFixed(3)}`
        )
    }
}
