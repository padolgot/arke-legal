import { useState, useRef, useEffect, useCallback } from "react"
import { Sun, Moon, Search, FileText, FileSearch } from "lucide-react"
import { ask, openFile, type Source } from "@/lib/api"

interface Message
{
    role: "user" | "assistant";
    text: string;
    sources?: Source[];
}

function SourceTable({ sources }: { sources: Source[] })
{
    async function handleClick(source: string)
    {
        await openFile(source)
    }

    return (
        <div className="mt-3 border border-border rounded-md overflow-hidden">
            {sources.map((s, i) => (
                <button
                    key={i}
                    onClick={() => handleClick(s.source)}
                    className="w-full text-left px-4 py-3 flex gap-3 items-start hover:bg-accent/50 transition-colors border-b border-border last:border-b-0 cursor-pointer"
                >
                    <FileText size={16} className="mt-0.5 shrink-0 text-muted-foreground" />
                    <div className="min-w-0 flex-1">
                        <div className="flex items-center gap-2">
                            <span className="font-medium text-sm truncate">{s.source}</span>
                            <span className="text-xs text-muted-foreground shrink-0">
                                {(s.similarity * 100).toFixed(1)}%
                            </span>
                        </div>
                        <p className="text-xs text-muted-foreground mt-1 line-clamp-2">{s.content}</p>
                    </div>
                </button>
            ))}
        </div>
    )
}

export default function App()
{
    const [dark, setDark] = useState(() => document.documentElement.classList.contains("dark"))
    const [messages, setMessages] = useState<Message[]>([])
    const [input, setInput] = useState("")
    const [loading, setLoading] = useState(false)
    const bottomRef = useRef<HTMLDivElement>(null)
    const textareaRef = useRef<HTMLTextAreaElement>(null)

    const autoResize = useCallback(() =>
    {
        const el = textareaRef.current
        if (!el) return
        el.style.height = "auto"
        el.style.height = el.scrollHeight + "px"
    }, [])

    useEffect(() =>
    {
        bottomRef.current?.scrollIntoView({ behavior: "smooth" })
    }, [messages])

    function toggleTheme()
    {
        const next = !dark
        setDark(next)
        document.documentElement.classList.toggle("dark", next)
        localStorage.setItem("theme", next ? "dark" : "light")
    }

    async function handleSend()
    {
        const q = input.trim()
        if (!q || loading) return

        setInput("")
        if (textareaRef.current) textareaRef.current.style.height = "auto"
        setMessages(prev => [...prev, { role: "user", text: q }])
        setLoading(true)

        try
        {
            const res = await ask(q)
            setMessages(prev => [
                ...prev,
                { role: "assistant", text: res.answer, sources: res.sources },
            ])
        }
        catch (e)
        {
            setMessages(prev => [
                ...prev,
                { role: "assistant", text: "Error: " + (e instanceof Error ? e.message : String(e)) },
            ])
        }
        finally
        {
            setLoading(false)
        }
    }

    return (
        <div className="min-h-screen bg-background text-foreground flex flex-col">
            {/* Header */}
            <header className="sticky top-0 z-10 border-b border-border bg-background px-6 py-3 flex items-center justify-between shrink-0">
                <div className="flex items-center gap-2">
                    <FileSearch size={20} />
                    <h1 className="text-lg font-bold">Arke Terminal</h1>
                </div>
                <button
                    onClick={toggleTheme}
                    className="rounded-md p-2 hover:bg-accent transition-colors"
                >
                    {dark ? <Sun size={18} /> : <Moon size={18} />}
                </button>
            </header>

            {/* Messages */}
            <div className="flex-1 overflow-y-auto px-6 py-4">
                <div className="mx-auto max-w-3xl space-y-4">
                    {messages.length === 0 && (
                        <div className="text-center text-muted-foreground mt-32">
                            <p className="text-lg">Search your documents</p>
                        </div>
                    )}
                    {messages.map((m, i) => (
                        <div key={i} className={m.role === "user" ? "flex justify-end" : ""}>
                            <div
                                className={
                                    m.role === "user"
                                        ? "bg-primary text-primary-foreground rounded-2xl rounded-br-sm px-4 py-2 max-w-[80%]"
                                        : "max-w-full"
                                }
                            >
                                <p className="text-sm whitespace-pre-wrap">{m.text}</p>
                                {m.sources && m.sources.length > 0 && (
                                    <SourceTable sources={m.sources} />
                                )}
                            </div>
                        </div>
                    ))}
                    {loading && (
                        <div className="text-muted-foreground text-sm animate-pulse">
                            Thinking...
                        </div>
                    )}
                    <div ref={bottomRef} />
                </div>
            </div>

            {/* Input */}
            <div className="sticky bottom-0 z-10 px-6 py-4 shrink-0">
                <div className="mx-auto max-w-3xl">
                    <div className="rounded-3xl bg-muted border border-border dark:border-white/15 overflow-hidden">
                        <textarea
                            ref={textareaRef}
                            rows={1}
                            className="w-full bg-transparent text-sm focus:outline-none placeholder:text-muted-foreground resize-none leading-6 px-5 pt-4 pb-2 max-h-72 overflow-y-auto"
                            placeholder="Search your documents..."
                            value={input}
                            onChange={(e) => { setInput(e.target.value); autoResize() }}
                            onKeyDown={(e) =>
                            {
                                if (e.key === "Enter" && !e.shiftKey)
                                {
                                    e.preventDefault()
                                    handleSend()
                                }
                            }}
                            disabled={loading}
                        />
                        <div className="flex items-center justify-end px-4 pb-3">
                            <button
                                onClick={handleSend}
                                disabled={loading || !input.trim()}
                                className="rounded-lg bg-primary text-primary-foreground p-2 hover:opacity-90 transition-opacity disabled:opacity-50"
                            >
                                <Search size={16} />
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    )
}
