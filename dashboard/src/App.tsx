import { useState } from "react"
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from "recharts"
import { Sun, Moon } from "lucide-react"
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table"
import { ask, sweep, type SweepRow } from "@/lib/api"

function mrrColor(v: number): string
{
    if (v >= 0.8) return "oklch(0.72 0.19 142)"   // зелёный
    if (v >= 0.5) return "oklch(0.80 0.18 85)"    // жёлтый
    return "oklch(0.63 0.24 25)"                    // красный
}

function fmt(v: number): string
{
    return (v * 100).toFixed(1) + "%"
}

export default function App()
{
    const [dark, setDark] = useState(() => document.documentElement.classList.contains("dark"))

    function toggleTheme()
    {
        const next = !dark
        setDark(next)
        document.documentElement.classList.toggle("dark", next)
        localStorage.setItem("theme", next ? "dark" : "light")
    }

    const [query, setQuery] = useState("")
    const [answer, setAnswer] = useState("")
    const [asking, setAsking] = useState(false)

    const [rows, setRows] = useState<SweepRow[]>([])
    const [sweeping, setSweeping] = useState(false)
    const [level, setLevel] = useState("fast")

    async function handleAsk()
    {
        if (!query.trim()) return
        setAsking(true)
        try
        {
            const res = await ask(query)
            setAnswer(res.answer)
        }
        catch (e)
        {
            setAnswer("error: " + (e instanceof Error ? e.message : String(e)))
        }
        finally
        {
            setAsking(false)
        }
    }

    async function handleSweep()
    {
        setSweeping(true)
        try
        {
            const limits: Record<string, number> = { fast: 10, medium: 30, full: 100 }
            const res = await sweep(level, limits[level] ?? 30)
            setRows(res.rows)
        }
        catch (e)
        {
            setRows([])
            setAnswer("sweep error: " + (e instanceof Error ? e.message : String(e)))
        }
        finally
        {
            setSweeping(false)
        }
    }

    const sorted = [...rows].sort((a, b) => b.mrr - a.mrr)
    const best = sorted[0]
    const worst = sorted[sorted.length - 1]
    const gap = best && worst ? best.mrr - worst.mrr : 0

    return (
        <div className="min-h-screen bg-background text-foreground">
            <div className="mx-auto max-w-6xl p-6 space-y-6">
                <div className="flex items-center justify-between">
                    <h1 className="text-2xl font-bold">Nerva Iris</h1>
                    <button
                        onClick={toggleTheme}
                        className="rounded-md p-2 hover:bg-accent transition-colors"
                    >
                        {dark ? <Sun size={20} /> : <Moon size={20} />}
                    </button>
                </div>

                {/* Ask */}
                <Card>
                    <CardHeader>
                        <CardTitle>Ask</CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-3">
                        <div className="flex gap-2">
                            <input
                                className="flex-1 rounded-md border border-input bg-background px-3 py-2 text-sm"
                                placeholder="Ask a question..."
                                value={query}
                                onChange={(e) => setQuery(e.target.value)}
                                onKeyDown={(e) => e.key === "Enter" && handleAsk()}
                            />
                            <Button onClick={handleAsk} disabled={asking}>
                                {asking ? "..." : "Ask"}
                            </Button>
                        </div>
                        {answer && (
                            <div className="rounded-md border border-border bg-muted p-3 text-sm whitespace-pre-wrap">
                                {answer}
                            </div>
                        )}
                    </CardContent>
                </Card>

                {/* Sweep controls */}
                <Card>
                    <CardHeader>
                        <CardTitle>Sweep</CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-4">
                        <div className="flex gap-3 items-center">
                            <select
                                className="rounded-md border border-input bg-background px-3 py-2 text-sm"
                                value={level}
                                onChange={(e) => setLevel(e.target.value)}
                            >
                                <option value="fast">fast</option>
                                <option value="medium">medium</option>
                                <option value="full">full</option>
                            </select>
                            <Button onClick={handleSweep} disabled={sweeping}>
                                {sweeping ? "Running..." : "Run Sweep"}
                            </Button>
                        </div>

                        {/* Summary cards */}
                        {rows.length > 0 && (
                            <>
                                <div className="grid grid-cols-3 gap-3">
                                    <Card>
                                        <CardContent className="p-4">
                                            <div className="text-xs text-muted-foreground">Best MRR</div>
                                            <div className="text-2xl font-bold" style={{ color: mrrColor(best.mrr) }}>
                                                {fmt(best.mrr)}
                                            </div>
                                            <div className="text-xs text-muted-foreground">
                                                chunk={best.chunk_size} k={best.k}
                                            </div>
                                        </CardContent>
                                    </Card>
                                    <Card>
                                        <CardContent className="p-4">
                                            <div className="text-xs text-muted-foreground">Worst MRR</div>
                                            <div className="text-2xl font-bold" style={{ color: mrrColor(worst.mrr) }}>
                                                {fmt(worst.mrr)}
                                            </div>
                                            <div className="text-xs text-muted-foreground">
                                                chunk={worst.chunk_size} k={worst.k}
                                            </div>
                                        </CardContent>
                                    </Card>
                                    <Card>
                                        <CardContent className="p-4">
                                            <div className="text-xs text-muted-foreground">Gap</div>
                                            <div className="text-2xl font-bold">{fmt(gap)}</div>
                                        </CardContent>
                                    </Card>
                                </div>

                                {/* Bar chart */}
                                <div className="h-64">
                                    <ResponsiveContainer width="100%" height="100%">
                                        <BarChart data={sorted}>
                                            <XAxis
                                                dataKey="chunk_size"
                                                tick={{ fill: "var(--foreground)", fontSize: 12 }}
                                                tickFormatter={(v, i) => `${v}/${sorted[i]?.k ?? ""}`}
                                                label={{ value: "chunk / k", position: "insideBottom", offset: -2, fill: "var(--muted-foreground)", fontSize: 12 }}
                                            />
                                            <YAxis
                                                domain={[0, 1]}
                                                tick={{ fill: "var(--foreground)", fontSize: 12 }}
                                                tickFormatter={(v: number) => fmt(v)}
                                            />
                                            <Tooltip
                                                contentStyle={{ backgroundColor: "var(--card)", border: "1px solid var(--border)", color: "var(--foreground)" }}
                                                formatter={(v) => [fmt(Number(v)), "MRR"]}
                                                labelFormatter={(_v, payload) =>
                                                {
                                                    const d = payload?.[0]?.payload as SweepRow | undefined
                                                    return d ? `chunk=${d.chunk_size} overlap=${d.overlap} alpha=${d.alpha} k=${d.k}` : ""
                                                }}
                                            />
                                            <Bar dataKey="mrr">
                                                {sorted.map((row, i) => (
                                                    <Cell key={i} fill={mrrColor(row.mrr)} />
                                                ))}
                                            </Bar>
                                        </BarChart>
                                    </ResponsiveContainer>
                                </div>

                                {/* Table */}
                                <Table>
                                    <TableHeader>
                                        <TableRow>
                                            <TableHead>Chunk</TableHead>
                                            <TableHead>Overlap</TableHead>
                                            <TableHead>Alpha</TableHead>
                                            <TableHead>K</TableHead>
                                            <TableHead>Precision</TableHead>
                                            <TableHead>Recall</TableHead>
                                            <TableHead>MRR</TableHead>
                                        </TableRow>
                                    </TableHeader>
                                    <TableBody>
                                        {sorted.map((row, i) => (
                                            <TableRow key={i}>
                                                <TableCell>{row.chunk_size}</TableCell>
                                                <TableCell>{row.overlap}</TableCell>
                                                <TableCell>{row.alpha}</TableCell>
                                                <TableCell>{row.k}</TableCell>
                                                <TableCell>{fmt(row.precision)}</TableCell>
                                                <TableCell>{fmt(row.recall)}</TableCell>
                                                <TableCell>
                                                    <Badge
                                                        variant={row.mrr >= 0.8 ? "default" : row.mrr >= 0.5 ? "secondary" : "outline"}
                                                    >
                                                        {fmt(row.mrr)}
                                                    </Badge>
                                                </TableCell>
                                            </TableRow>
                                        ))}
                                    </TableBody>
                                </Table>
                            </>
                        )}
                    </CardContent>
                </Card>
            </div>
        </div>
    )
}
