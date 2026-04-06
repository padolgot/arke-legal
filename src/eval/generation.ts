import {pool} from "../db.js"

export interface EvalCase
{
    query: string
    expected_ids: string[]
}

const PROMPT = `You will receive a text chunk from a personal knowledge base. Generate 1 to 3 questions that ONLY this specific chunk can answer. Questions must include specific details from the text — names, numbers, dates, unique terms. Avoid generic questions. Return ONLY a JSON array of strings, nothing else. Example: ["question 1", "question 2"]

Chunk:
`

async function generateQuestions(content: string): Promise<string[]>
{
    const res = await fetch(`${process.env.INFERENCE_URL}/api/chat`, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({
            model: process.env.INFERENCE_MODEL,
            stream: false,
            messages: [
                {role: "user", content: PROMPT + content},
            ],
        }),
    })

    if (!res.ok)
    {
        const body = await res.text()
        throw new Error(`Inference ${res.status}: ${body}`)
    }

    const json = await res.json() as { message: { content: string } }
    const raw = json.message.content.trim()

    const match = raw.match(/\[[\s\S]*\]/)
    if (!match) return []

    const parsed = JSON.parse(match[0]) as unknown
    if (!Array.isArray(parsed)) return []

    return parsed.filter((q): q is string => typeof q === "string" && q.length > 0)
}

export async function generate(limit: number = 50): Promise<EvalCase[]>
{
    const res = await pool.query(
        `SELECT id, content FROM chunks ORDER BY random() LIMIT $1`,
        [limit],
    )

    const cases: EvalCase[] = []

    for (const row of res.rows)
    {
        const questions = await generateQuestions(row.content)

        for (const q of questions)
        {
            cases.push({ query: q, expected_ids: [row.id] })
        }

        console.log(`  [${cases.length}] ${questions.length} questions from chunk ${row.id.slice(0, 8)}`)
    }

    return cases
}
