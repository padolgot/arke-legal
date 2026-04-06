import {ingest} from "../pipeline/index.js"
import {pool} from "../db.js"
import {type PipelineConfig, type ChunkConfig, type SearchConfig} from "../pipeline/config.js"
import {generate} from "./generation.js"
import {evaluate, type EvalMetrics} from "./evaluation.js"

interface SweepRow
{
    cfg: PipelineConfig
    metrics: EvalMetrics
}

interface ChunkGroup
{
    chunk: ChunkConfig
    searches: SearchConfig[]
}

export async function sweep(presets: PipelineConfig[], limit: number, sourcePath: string): Promise<SweepRow[]>
{
    const groups = groupByChunk(presets)
    const rows: SweepRow[] = []
    let i = 0

    for (const group of groups)
    {
        await pool.query("TRUNCATE chunks")
        await ingest(sourcePath, group.chunk)

        const cases = await generate(limit)
        console.log(`sweep: generated ${cases.length} cases from ${limit} chunks`)

        for (const search of group.searches)
        {
            i++
            console.log(`\nsweep [${i}/${presets.length}] chunkSize=${group.chunk.chunkSize} overlap=${group.chunk.overlap} alpha=${search.alpha} k=${search.k}`)

            const metrics = (await evaluate(cases, search)).average
            rows.push({cfg: {chunk: group.chunk, search}, metrics})

            console.log(`sweep: P=${metrics.precision.toFixed(3)} R=${metrics.recall.toFixed(3)} MRR=${metrics.mrr.toFixed(3)}`)
        }
    }

    rows.sort((a, b) => b.metrics.mrr - a.metrics.mrr)
    printTable(rows)

    return rows
}

function groupByChunk(presets: PipelineConfig[]): ChunkGroup[]
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

function printTable(rows: SweepRow[])
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
