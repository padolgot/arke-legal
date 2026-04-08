// Клиент к ollama-совместимому chat API.
// Знает только про "system + user → text". Промпты держит здесь же,
// потому что они часть его контракта с моделью.

const SYSTEM_WITH_CONTEXT = `You are a personal knowledge assistant. You answer questions based ONLY on the provided context. If the context doesn't contain enough information, say so honestly. Answer in the same language as the question. Be concise and direct.`

const SYSTEM_NO_CONTEXT = `You are a knowledge assistant. Answer the question directly based on your general knowledge. Answer in the same language as the question. Be concise and direct.`

const GEN_PROMPT = `You will receive a text chunk from a personal knowledge base. Generate 1 to 3 questions that ONLY this specific chunk can answer. Questions must include specific details from the text — names, numbers, dates, unique terms. Avoid generic questions. Return ONLY a JSON array of strings, nothing else. Example: ["question 1", "question 2"]

Chunk:
`

export class Llm
{
    readonly url: string
    readonly model: string

    constructor(url: string, model: string)
    {
        this.url = url
        this.model = model
    }

    async answer(query: string, context: string | null): Promise<string>
    {
        const system = context !== null ? SYSTEM_WITH_CONTEXT : SYSTEM_NO_CONTEXT
        const userContent = context !== null
            ? `Context:\n${context}\n\nQuestion: ${query}`
            : query

        return this.chat(system, userContent)
    }

    async generateQuestions(content: string): Promise<string[]>
    {
        const raw = (await this.chat(null, GEN_PROMPT + content)).trim()

        const match = raw.match(/\[[\s\S]*]/)
        if (!match) return []

        let parsed: unknown
        try { parsed = JSON.parse(match[0]) }
        catch { return [] }
        if (!Array.isArray(parsed)) return []

        return parsed.filter((q): q is string => typeof q === "string" && q.length > 0)
    }

    private async chat(system: string | null, user: string): Promise<string>
    {
        const messages: {role: string; content: string}[] = []
        if (system !== null) messages.push({role: "system", content: system})
        messages.push({role: "user", content: user})

        const res = await fetch(`${this.url}/api/chat`, {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify({model: this.model, stream: false, messages}),
        })

        if (!res.ok)
        {
            const body = await res.text()
            throw new Error(`Inference ${res.status}: ${body}`)
        }

        const json = await res.json() as {message: {content: string}}
        return json.message.content
    }
}
