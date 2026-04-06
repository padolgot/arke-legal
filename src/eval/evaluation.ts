import {search} from "../pipeline/searcher.js"
import {type SearchConfig} from "../pipeline/config.js"
import {type EvalCase} from "./generation.js"

export interface EvalMetrics
{
    precision: number
    recall: number
    mrr: number
}

export interface EvalQueryResult
{
    query: string
    precision: number
    recall: number
    rr: number
}

export interface EvalResult
{
    per_query: EvalQueryResult[]
    average: EvalMetrics
}

export async function evaluate(cases: EvalCase[], cfg: SearchConfig): Promise<EvalResult>
{
    const per_query: EvalQueryResult[] = []

    for (const c of cases)
    {
        const results = await search(c.query, cfg)
        const expectedSet = new Set(c.expected_ids)

        let hits = 0
        for (const r of results)
        {
            if (expectedSet.has(r.id)) hits++
        }

        const precision = results.length > 0 ? hits / results.length : 0
        const recall = expectedSet.size > 0 ? hits / expectedSet.size : 0

        let rr = 0
        for (let i = 0; i < results.length; i++)
        {
            if (expectedSet.has(results[i].id))
            {
                rr = 1 / (i + 1)
                break
            }
        }

        per_query.push({ query: c.query, precision, recall, rr })
    }

    const n = per_query.length
    const average: EvalMetrics = {
        precision: n > 0 ? per_query.reduce((s, q) => s + q.precision, 0) / n : 0,
        recall: n > 0 ? per_query.reduce((s, q) => s + q.recall, 0) / n : 0,
        mrr: n > 0 ? per_query.reduce((s, q) => s + q.rr, 0) / n : 0,
    }

    return { per_query, average }
}
