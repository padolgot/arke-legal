import {loadConfig, validate, type PipelineConfig} from "../pipeline/config.js"

export function getPreset(name: string): PipelineConfig[]
{
    if (name === "fast") return FAST
    if (name === "medium") return MEDIUM
    if (name === "thorough") return THOROUGH
    throw new Error(`unknown sweep level: ${name} (expected: fast | medium | thorough)`)
}

interface SweepAxes
{
    chunkSize: number[]
    overlap: number[]
    alpha: number[]
    k: number[]
}

function expand(axes: SweepAxes): PipelineConfig[]
{
    const out: PipelineConfig[] = []
    for (const chunkSize of axes.chunkSize)
        for (const overlap of axes.overlap)
            for (const alpha of axes.alpha)
                for (const k of axes.k)
                {
                    const cfg: PipelineConfig = {
                        chunk: {chunkSize, overlap},
                        search: {alpha, k},
                    }
                    validate(cfg)
                    out.push(cfg)
                }
    return out
}

const base = loadConfig()

const FAST: PipelineConfig[] = expand({
    chunkSize: [base.chunk.chunkSize],
    overlap:   [base.chunk.overlap],
    alpha:     [base.search.alpha],
    k:         [base.search.k],
})

const MEDIUM: PipelineConfig[] = expand({
    chunkSize: [base.chunk.chunkSize],
    overlap:   [base.chunk.overlap],
    alpha:     [0.0, 0.3, 0.5, 0.7, 1.0],
    k:         [5, 10, 20],
})

const THOROUGH: PipelineConfig[] = expand({
    chunkSize: [500, 1000],
    overlap:   [0, 0.2],
    alpha:     [0.5, 0.7],
    k:         [5],
})
