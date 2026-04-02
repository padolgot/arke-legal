const MAX = 600
const SEPARATORS = ["\n\n", "\n", ". ", ", ", " ", ""]

export function chunk(text: string): string[]
{
    if (!text) return []
    return merge(separate(text.trim()))
}

function separate(text: string, depth: number = 0): string[]
{
    if (depth >= SEPARATORS.length) return [text]

    const sep = SEPARATORS[depth]
    const parts = text.split(sep)

    const result: string[] = []

    for (let i = 0; i < parts.length; i++)
    {
        const part = parts[i]

        if (part.length < MAX)
        {
            const suffix = (i < parts.length - 1) ? sep : ""
            result.push(part + suffix)
        }
        else
        {
            result.push(...separate(part, depth + 1))
        }
    }

    return result
}

function merge(splits: string[]): string[]
{
    const chunks: string[] = []

    for (const s of splits)
    {
        if (s.length === 0) continue

        const last = chunks.length - 1

        if (last >= 0 && chunks[last].length + s.length < MAX)
        {
            chunks[last] += s
        }
        else
        {
            chunks.push(s)
        }
    }

    return chunks
}

