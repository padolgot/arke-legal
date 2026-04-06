import {readFileSync} from "fs"

export interface ChunkConfig
{
    chunkSize: number
    overlap: number
}

export interface SearchConfig
{
    alpha: number
    k: number
}

export interface PipelineConfig
{
    chunk: ChunkConfig
    search: SearchConfig
}

export function loadConfig(path: string = ".config"): PipelineConfig
{
    const raw = readFileSync(path, "utf-8")
    const cfg = JSON.parse(raw) as PipelineConfig
    validate(cfg)
    return cfg
}

export function validate(cfg: PipelineConfig): void
{
    const {chunkSize, overlap} = cfg.chunk
    const {alpha, k} = cfg.search

    if (!Number.isInteger(chunkSize) || chunkSize < 100 || chunkSize > 10000)
        throw new Error(`config: chunkSize must be integer 100..10000, got ${chunkSize}`)
    if (overlap < 0 || overlap > 0.5)
        throw new Error(`config: overlap must be 0..0.5 (fraction of chunkSize), got ${overlap}`)
    if (alpha < 0 || alpha > 1)
        throw new Error(`config: alpha must be 0..1, got ${alpha}`)
    if (!Number.isInteger(k) || k < 1 || k > 20)
        throw new Error(`config: k must be integer 1..20, got ${k}`)
}
