import {search} from "./searcher.js"

const SYSTEM = `You are a personal knowledge assistant. You answer questions based ONLY on the provided context. If the context doesn't contain enough information, say so honestly. Answer in the same language as the question. Be concise and direct.`

export async function ask(query: string, limit: number = 10): Promise<string>
{
    const results = await search(query, limit)

    const context = results
        .map((r, i) =>
        {
            const date = new Date(r.created_at).toISOString().slice(0, 10)
            return `[${i + 1}] (${date}, ${r.source}, sim=${r.similarity.toFixed(3)})\n${r.content}`
        })
        .join("\n\n")

    const res = await fetch(`${process.env.INFERENCE_URL}/api/chat`, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({
            model: process.env.INFERENCE_MODEL,
            stream: false,
            messages: [
                {role: "system", content: SYSTEM},
                {role: "user", content: `Context:\n${context}\n\nQuestion: ${query}`},
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
