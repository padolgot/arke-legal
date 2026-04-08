// Клиент к ollama-совместимому embedder API.

export class Embedder
{
    readonly url: string
    readonly model: string

    constructor(url: string, model: string)
    {
        this.url = url
        this.model = model
    }

    async embed(texts: string[]): Promise<number[][]>
    {
        if (texts.length === 0) return []

        const res = await fetch(`${this.url}/api/embed`, {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify({model: this.model, input: texts}),
        })

        if (!res.ok)
        {
            const body = await res.text()
            throw new Error(`Embedder ${res.status}: ${body}`)
        }

        const json = await res.json() as {embeddings: number[][]}
        return json.embeddings
    }
}
