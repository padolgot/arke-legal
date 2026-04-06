import type {Request, Response, NextFunction} from "express"

const WINDOW = 60_000
const MAX = 30

const windows = new Map<string, number[]>()

export function rateLimit(req: Request, res: Response, next: NextFunction)
{
    const ip = req.ip ?? "unknown"
    const now = Date.now()
    const cutoff = now - WINDOW

    let timestamps = windows.get(ip)

    if (!timestamps)
    {
        timestamps = []
        windows.set(ip, timestamps)
    }

    while (timestamps.length > 0 && timestamps[0] < cutoff)
    {
        timestamps.shift()
    }

    if (timestamps.length >= MAX)
    {
        const retryAfter = Math.ceil((timestamps[0] + WINDOW - now) / 1000)
        res.set("Retry-After", String(retryAfter))
        res.status(429).json({error: "Too many requests", retry_after: retryAfter})
        return
    }

    timestamps.push(now)
    res.set("X-RateLimit-Remaining", String(MAX - timestamps.length))
    next()
}
