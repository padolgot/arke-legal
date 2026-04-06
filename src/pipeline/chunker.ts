import {type ChunkConfig} from "./config.js"

const SEPARATORS = ["\n\n", "\n", ". ", ", ", " ", ""]

export class ChunkData
{
    readonly clean: string
    readonly head: string
    readonly tail: string

    constructor(clean: string, head: string, tail: string)
    {
        this.clean = clean
        this.head = head
        this.tail = tail
    }

    overlapped(): string
    {
        return this.head + this.clean + this.tail
    }
}

export function chunk(text: string, cfg: ChunkConfig): ChunkData[]
{
    if (!text) return []

    const clean = merge(separate(text.trim(), cfg.chunkSize), cfg.chunkSize)
    const result: ChunkData[] = []

    for (let i = 0; i < clean.length; i++)
    {
        const head = (cfg.overlap > 0 && i > 0)
            ? clean[i - 1].slice(-cfg.overlap)
            : ""
        const tail = (cfg.overlap > 0 && i < clean.length - 1)
            ? clean[i + 1].slice(0, cfg.overlap)
            : ""
        result.push(new ChunkData(clean[i], head, tail))
    }

    return result
}

function separate(text: string, chunkSize: number, depth: number = 0): string[]
{
    if (depth >= SEPARATORS.length) return [text]

    const sep = SEPARATORS[depth]
    const parts = text.split(sep)

    const result: string[] = []

    for (let i = 0; i < parts.length; i++)
    {
        const part = parts[i]

        if (part.length < chunkSize)
        {
            const suffix = (i < parts.length - 1) ? sep : ""
            result.push(part + suffix)
        }
        else
        {
            result.push(...separate(part, chunkSize, depth + 1))
        }
    }

    return result
}

function merge(splits: string[], chunkSize: number): string[]
{
    const raw: string[] = []

    for (const s of splits)
    {
        if (s.length === 0) continue

        const last = raw.length - 1

        if (last >= 0 && raw[last].length + s.length < chunkSize)
        {
            raw[last] += s
        }
        else
        {
            raw.push(s)
        }
    }

    return raw
}
