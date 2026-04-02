import {Router, type Request, type Response, type NextFunction} from "express"
import {SearchBody, AskBody, IngestBody} from "./schemas.js"
import {search, ask, ingest, type Doc} from "../pipeline/index.js"
import {pool} from "../db.js"

export const router = Router()

router.post("/search", async (req: Request, res: Response, next: NextFunction) =>
{
    try
    {
        const parsed = SearchBody.safeParse(req.body)
        if (!parsed.success)
        {
            res.status(400).json({error: parsed.error.issues})
            return
        }

        const results = await search(parsed.data.query, parsed.data.limit)
        res.json({results})
    }
    catch (err)
    {
        next(err)
    }
})

router.post("/ask", async (req: Request, res: Response, next: NextFunction) =>
{
    try
    {
        const parsed = AskBody.safeParse(req.body)
        if (!parsed.success)
        {
            res.status(400).json({error: parsed.error.issues})
            return
        }

        const answer = await ask(parsed.data.query, parsed.data.limit)
        res.json({answer})
    }
    catch (err)
    {
        next(err)
    }
})

router.post("/ingest", async (req: Request, res: Response, next: NextFunction) =>
{
    try
    {
        const parsed = IngestBody.safeParse(req.body)
        if (!parsed.success)
        {
            res.status(400).json({error: parsed.error.issues})
            return
        }

        await ingest(parsed.data.docs as Doc[])
        res.json({ok: true, count: parsed.data.docs.length})
    }
    catch (err)
    {
        next(err)
    }
})

router.get("/health", async (_req: Request, res: Response, next: NextFunction) =>
{
    try
    {
        const result = await pool.query("SELECT count(*) FROM chunks")
        res.json({status: "ok", chunks: Number(result.rows[0].count)})
    }
    catch (err)
    {
        next(err)
    }
})
