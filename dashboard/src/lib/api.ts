const BASE = import.meta.env.VITE_API_URL || "http://localhost:8000";

export interface Source
{
    source: string;
    content: string;
    similarity: number;
}

export interface AskResponse
{
    answer: string;
    sources: Source[];
}

export async function ask(query: string): Promise<AskResponse>
{
    const res = await fetch(`${BASE}/ask`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query }),
    });
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    return res.json();
}

export async function openFile(source: string): Promise<string>
{
    const res = await fetch(`${BASE}/open/${encodeURIComponent(source)}`);
    const data = await res.json();
    return data.path;
}
