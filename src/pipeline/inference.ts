import {type SearchResult} from "./searcher.js"

const SYSTEM_WITH_CONTEXT = `You are a personal knowledge assistant. You answer questions based ONLY on the provided context. If the context doesn't contain enough information, say so honestly. Answer in the same language as the question. Be concise and direct.`

const SYSTEM_NO_CONTEXT = `You are a knowledge assistant. Answer the question directly based on your general knowledge. Answer in the same language as the question. Be concise and direct.`

export async function infer(query: string, context?: SearchResult[]): Promise<string>
{
    const hasContext = context !== undefined && context.length > 0

    const system = hasContext ? SYSTEM_WITH_CONTEXT : SYSTEM_NO_CONTEXT

    const userContent = hasContext
        ? `Context:\n${formatContext(context!)}\n\nQuestion: ${query}`
        : query

    const res = await fetch(`${process.env.INFERENCE_URL}/api/chat`, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({
            model: process.env.INFERENCE_MODEL,
            stream: false,
            messages: [
                {role: "system", content: system},
                {role: "user", content: userContent},
            ],
        }),
    })

    if (!res.ok)
    {
        const body = await res.text()
        throw new Error(`Inference ${res.status}: ${body}`)
    }

    const json = await res.json() as { message: { content: string } }
    return json.message.content
}

function formatContext(results: SearchResult[]): string
{
    return results
        .map((r, i) =>
        {
            const date = new Date(r.created_at).toISOString().slice(0, 10)
            return `[${i + 1}] (${date}, ${r.source}, sim=${r.similarity.toFixed(3)})\n${r.content}`
        })
        .join("\n\n")
}
