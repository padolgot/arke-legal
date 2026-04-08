import {validateChunk, validateSearch, type ChunkConfig, type SearchConfig, type MnemeConfig} from "./defaults.js"

export interface Preset
{
    chunk: ChunkConfig
    search: SearchConfig
}

interface SweepAxes
{
    chunkSize: number[]
    overlap: number[]
    alpha: number[]
    k: number[]
}

export function getPreset(name: string, base: MnemeConfig): Preset[]
{
    if (name === "fast") return expand({
        chunkSize: [base.chunk.chunkSize],
        overlap:   [base.chunk.overlap],
        alpha:     [base.search.alpha],
        k:         [base.search.k],
    })
    if (name === "medium") return expand({
        chunkSize: [base.chunk.chunkSize],
        overlap:   [base.chunk.overlap],
        alpha:     [0.0, 0.3, 0.5, 0.7, 1.0],
        k:         [5, 10, 20],
    })
    if (name === "thorough") return expand({
        chunkSize: [500, 1000],
        overlap:   [0, 0.2],
        alpha:     [0.5, 0.7],
        k:         [5],
    })
    throw new Error(`unknown sweep level: ${name} (expected: fast | medium | thorough)`)
}

function expand(axes: SweepAxes): Preset[]
{
    const out: Preset[] = []
    for (const chunkSize of axes.chunkSize)
        for (const overlap of axes.overlap)
            for (const alpha of axes.alpha)
                for (const k of axes.k)
                {
                    const p: Preset = {chunk: {chunkSize, overlap}, search: {alpha, k}}
                    validateChunk(p.chunk)
                    validateSearch(p.search)
                    out.push(p)
                }
    return out
}
