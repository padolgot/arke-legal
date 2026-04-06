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
    return JSON.parse(raw) as PipelineConfig
}
