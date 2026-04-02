export async function embed(texts: string[]): Promise<number[][]>
{
    if (texts.length === 0) return []

    const res = await fetch(`${process.env.EMBEDDER_URL}/api/embed`, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({model: process.env.EMBEDDER_MODEL, input: texts}),
    })

    if (!res.ok)
    {
        const body = await res.text()
        throw new Error(`Embedder ${res.status}: ${body}`)
    }

    const json = await res.json() as { embeddings: number[][] }
    return json.embeddings
}
