import {ingest} from "../pipeline/index.js"
import {pool} from "../db.js"
import {type PipelineConfig, type ChunkConfig} from "../pipeline/config.js"
import {generate} from "./generation.js"
import {evaluate, type EvalMetrics} from "./evaluation.js"

interface SweepRow
{
    chunkSize: number
    overlap: number
    alpha: number
    k: number
    precision: number
    recall: number
    mrr: number
}

export async function sweep(presets: PipelineConfig[], limit: number, sourcePath?: string): Promise<SweepRow[]>
{
    const rows: SweepRow[] = []
    let lastChunk: ChunkConfig | null = null
    let cases: Awaited<ReturnType<typeof generate>> = []

    for (let i = 0; i < presets.length; i++)
    {
        const cfg = presets[i]
        console.log(`\nsweep [${i + 1}/${presets.length}] chunkSize=${cfg.chunk.chunkSize} overlap=${cfg.chunk.overlap} alpha=${cfg.search.alpha} k=${cfg.search.k}`)

        if (!sameChunk(cfg.chunk, lastChunk))
        {
            if (!sourcePath) throw new Error("sweep requires SOURCE_PATH in .env when chunk config changes between presets")

            console.log(`sweep: TRUNCATE + re-ingest`)
            await pool.query("TRUNCATE chunks")
            await ingest(sourcePath, cfg.chunk)

            cases = await generate(limit)
            console.log(`sweep: ${cases.length} eval cases generated`)

            lastChunk = cfg.chunk
        }

        const result = await evaluate(cases, cfg.search)
        const avg: EvalMetrics = result.average

        rows.push({
            chunkSize: cfg.chunk.chunkSize,
            overlap: cfg.chunk.overlap,
            alpha: cfg.search.alpha,
            k: cfg.search.k,
            precision: avg.precision,
            recall: avg.recall,
            mrr: avg.mrr,
        })

        console.log(`sweep: P=${avg.precision.toFixed(3)} R=${avg.recall.toFixed(3)} MRR=${avg.mrr.toFixed(3)}`)
    }

    rows.sort((a, b) => b.mrr - a.mrr)
    printTable(rows)

    return rows
}

function sameChunk(a: ChunkConfig, b: ChunkConfig | null): boolean
{
    if (b === null) return false
    return a.chunkSize === b.chunkSize && a.overlap === b.overlap
}

function printTable(rows: SweepRow[])
{
    console.log(`\nRESULTS (sorted by MRR desc):`)
    console.log(`chunkSize | overlap | alpha |  k | precision | recall |  MRR`)
    console.log(`----------|---------|-------|----|-----------|--------|------`)
    for (const r of rows)
    {
        console.log(
            `${r.chunkSize.toString().padStart(9)}`
            + ` | ${r.overlap.toString().padStart(7)}`
            + ` | ${r.alpha.toFixed(1).padStart(5)}`
            + ` | ${r.k.toString().padStart(2)}`
            + ` | ${r.precision.toFixed(3).padStart(9)}`
            + ` | ${r.recall.toFixed(3).padStart(6)}`
            + ` | ${r.mrr.toFixed(3)}`
        )
    }
}
