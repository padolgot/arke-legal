// Метрики качества поиска. Чистая логика, без I/O.

import {type SearchResult} from "./store.js"

export interface EvalCase
{
    query: string
    expected_ids: string[]
}

export interface EvalMetrics
{
    precision: number
    recall: number
    mrr: number
}

export function score(perCase: {results: SearchResult[]; expected_ids: string[]}[]): EvalMetrics
{
    let sumP = 0
    let sumR = 0
    let sumRR = 0
    const n = perCase.length

    for (const c of perCase)
    {
        const expectedSet = new Set(c.expected_ids)

        let hits = 0
        for (const r of c.results)
        {
            if (expectedSet.has(r.id)) hits++
        }

        sumP += c.results.length > 0 ? hits / c.results.length : 0
        sumR += expectedSet.size > 0 ? hits / expectedSet.size : 0

        for (let i = 0; i < c.results.length; i++)
        {
            if (expectedSet.has(c.results[i].id))
            {
                sumRR += 1 / (i + 1)
                break
            }
        }
    }

    return {
        precision: n > 0 ? sumP / n : 0,
        recall: n > 0 ? sumR / n : 0,
        mrr: n > 0 ? sumRR / n : 0,
    }
}
