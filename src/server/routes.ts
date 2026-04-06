import {Router, type Request, type Response, type NextFunction} from "express"
import {SearchBody, AskBody, IngestBody} from "./schemas.js"
import {loadConfig, search, infer, ingest} from "../pipeline/index.js"
import {pool} from "../db.js"

const cfg = loadConfig()

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

        const results = await search(parsed.data.query, cfg.search)
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

        const results = await search(parsed.data.query, cfg.search)
        const answer = await infer(parsed.data.query, results)
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

        await ingest(parsed.data.sourcePath, cfg.chunk)
        res.json({ok: true})
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
