import {Router, type Request, type Response, type NextFunction} from "express"
import {requireString, isValidationError} from "./validate.js"
import {loadConfig, search, infer, ingest} from "../pipeline/index.js"
import {pool} from "../db.js"

const cfg = loadConfig()

export const router = Router()

router.post("/search", async (req: Request, res: Response, next: NextFunction) =>
{
    try
    {
        const query = requireString(req.body, "query")
        if (isValidationError(query)) { res.status(400).json(query); return }

        const results = await search(query, cfg.search)
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
        const query = requireString(req.body, "query")
        if (isValidationError(query)) { res.status(400).json(query); return }

        const results = await search(query, cfg.search)
        const answer = await infer(query, results)
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
        const sourcePath = requireString(req.body, "sourcePath")
        if (isValidationError(sourcePath)) { res.status(400).json(sourcePath); return }

        await ingest(sourcePath, cfg.chunk)
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
