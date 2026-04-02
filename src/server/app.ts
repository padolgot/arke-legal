import express, {type Request, type Response, type NextFunction} from "express"
import {rateLimit} from "./rate-limit.js"
import {router} from "./routes.js"

const REQUEST_TIMEOUT = 60_000

export const app = express()

app.use(express.json({limit: "10mb"}))
app.use(rateLimit)

app.use((_req: Request, res: Response, next: NextFunction) =>
{
    res.setTimeout(REQUEST_TIMEOUT, () =>
    {
        res.status(408).json({error: "Request timeout"})
    })
    next()
})

app.use((req: Request, _res: Response, next: NextFunction) =>
{
    console.log(`${new Date().toISOString()} ${req.method} ${req.path}`)
    next()
})

app.use(router)

app.use((_req: Request, res: Response) =>
{
    res.status(404).json({error: "Not found"})
})

app.use((err: Error, _req: Request, res: Response, _next: NextFunction) =>
{
    console.error(`ERROR: ${err.message}`)
    res.status(500).json({error: "Internal server error"})
})
