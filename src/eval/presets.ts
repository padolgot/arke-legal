import {loadConfig, type PipelineConfig, type ChunkConfig, type SearchConfig} from "../pipeline/config.js"

export enum SweepLevel
{
    Fast,
    Medium,
    Thorough,
}

export function parseSweepLevel(s: string): SweepLevel
{
    if (s === "fast") return SweepLevel.Fast
    if (s === "medium") return SweepLevel.Medium
    if (s === "thorough") return SweepLevel.Thorough
    throw new Error(`unknown sweep level: ${s} (expected: fast | medium | thorough)`)
}

export function getPreset(level: SweepLevel): PipelineConfig[]
{
    if (level === SweepLevel.Fast) return FAST
    if (level === SweepLevel.Medium) return MEDIUM
    return THOROUGH
}

const base = loadConfig()

function withSearch(search: SearchConfig): PipelineConfig
{
    return { chunk: base.chunk, search }
}

function withChunk(chunk: ChunkConfig, search: SearchConfig): PipelineConfig
{
    return { chunk, search }
}

const FAST: PipelineConfig[] = [
    withSearch({ alpha: 0.7, k: 5 }),
]

const MEDIUM: PipelineConfig[] = [
    withSearch({ alpha: 0.0, k: 5 }),
    withSearch({ alpha: 0.3, k: 5 }),
    withSearch({ alpha: 0.5, k: 5 }),
    withSearch({ alpha: 0.7, k: 5 }),
    withSearch({ alpha: 1.0, k: 5 }),
    withSearch({ alpha: 0.5, k: 10 }),
    withSearch({ alpha: 0.7, k: 10 }),
    withSearch({ alpha: 0.5, k: 20 }),
    withSearch({ alpha: 0.7, k: 20 }),
]

const THOROUGH: PipelineConfig[] = [
    withChunk({ chunkSize: 400, overlap: 0 },  { alpha: 0.5, k: 5 }),
    withChunk({ chunkSize: 400, overlap: 0 },  { alpha: 0.7, k: 5 }),
    withChunk({ chunkSize: 400, overlap: 50 }, { alpha: 0.5, k: 5 }),
    withChunk({ chunkSize: 400, overlap: 50 }, { alpha: 0.7, k: 5 }),
    withChunk({ chunkSize: 600, overlap: 0 },  { alpha: 0.5, k: 5 }),
    withChunk({ chunkSize: 600, overlap: 0 },  { alpha: 0.7, k: 5 }),
    withChunk({ chunkSize: 600, overlap: 50 }, { alpha: 0.5, k: 5 }),
    withChunk({ chunkSize: 600, overlap: 50 }, { alpha: 0.7, k: 5 }),
    withChunk({ chunkSize: 800, overlap: 0 },  { alpha: 0.5, k: 5 }),
    withChunk({ chunkSize: 800, overlap: 0 },  { alpha: 0.7, k: 5 }),
    withChunk({ chunkSize: 800, overlap: 100 },{ alpha: 0.5, k: 5 }),
    withChunk({ chunkSize: 800, overlap: 100 },{ alpha: 0.7, k: 5 }),
]
