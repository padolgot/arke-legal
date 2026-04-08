// Единственный источник правды для конфигурации.
// Наружу экспортируется только MnemeConfig (резолвнутый).
// Входной тип для конструктора и resolveConfig описан inline в сигнатурах,
// чтобы не плодить «Options»-близнецов для каждого Config.

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

export interface MnemeConfig
{
    databaseUrl: string
    embedderUrl: string
    embedderModel: string
    embeddingDim: number
    inferenceUrl: string
    inferenceModel: string
    chunk: ChunkConfig
    search: SearchConfig
}

// Дефолты

const DEFAULT_EMBEDDER_URL = "http://localhost:11434"
const DEFAULT_EMBEDDER_MODEL = "bge-m3"
const DEFAULT_EMBEDDING_DIM = 1024
const DEFAULT_INFERENCE_URL = "http://localhost:11434"
const DEFAULT_INFERENCE_MODEL = "llama3:8b-instruct-q4_K_M"
const DEFAULT_CHUNK_SIZE = 600
const DEFAULT_OVERLAP = 0
const DEFAULT_ALPHA = 0.7
const DEFAULT_K = 5

export function resolveConfig(input: {
    databaseUrl: string
    embedderUrl?: string
    embedderModel?: string
    embeddingDim?: number
    inferenceUrl?: string
    inferenceModel?: string
    chunk?: {chunkSize?: number; overlap?: number}
    search?: {alpha?: number; k?: number}
}): MnemeConfig
{
    if (!input.databaseUrl || input.databaseUrl.length === 0)
        throw new Error("Mneme: databaseUrl is required")

    const cfg: MnemeConfig =
    {
        databaseUrl: input.databaseUrl,
        embedderUrl: input.embedderUrl ?? DEFAULT_EMBEDDER_URL,
        embedderModel: input.embedderModel ?? DEFAULT_EMBEDDER_MODEL,
        embeddingDim: input.embeddingDim ?? DEFAULT_EMBEDDING_DIM,
        inferenceUrl: input.inferenceUrl ?? DEFAULT_INFERENCE_URL,
        inferenceModel: input.inferenceModel ?? DEFAULT_INFERENCE_MODEL,
        chunk:
        {
            chunkSize: input.chunk?.chunkSize ?? DEFAULT_CHUNK_SIZE,
            overlap: input.chunk?.overlap ?? DEFAULT_OVERLAP,
        },
        search:
        {
            alpha: input.search?.alpha ?? DEFAULT_ALPHA,
            k: input.search?.k ?? DEFAULT_K,
        },
    }

    validateChunk(cfg.chunk)
    validateSearch(cfg.search)

    return cfg
}

export function validateChunk(c: ChunkConfig): void
{
    if (!Number.isInteger(c.chunkSize) || c.chunkSize < 100 || c.chunkSize > 10000)
        throw new Error(`config: chunkSize must be integer 100..10000, got ${c.chunkSize}`)
    if (c.overlap < 0 || c.overlap > 0.5)
        throw new Error(`config: overlap must be 0..0.5 (fraction of chunkSize), got ${c.overlap}`)
}

export function validateSearch(s: SearchConfig): void
{
    if (s.alpha < 0 || s.alpha > 1)
        throw new Error(`config: alpha must be 0..1, got ${s.alpha}`)
    if (!Number.isInteger(s.k) || s.k < 1 || s.k > 20)
        throw new Error(`config: k must be integer 1..20, got ${s.k}`)
}
