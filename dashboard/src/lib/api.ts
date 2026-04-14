const BASE = import.meta.env.VITE_MNEME_URL || "http://localhost:8000";

export async function ask(query: string): Promise<{ answer: string }>
{
    const res = await fetch(`${BASE}/ask`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query }),
    });
    return res.json();
}

export interface SweepRow
{
    chunk_size: number;
    overlap: number;
    alpha: number;
    k: number;
    precision: number;
    recall: number;
    mrr: number;
}

export async function sweep(level: string, limit: number): Promise<{ rows: SweepRow[] }>
{
    const res = await fetch(`${BASE}/sweep`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ level, limit }),
    });
    return res.json();
}
