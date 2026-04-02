import {z} from "zod/v4"

export const SearchBody = z.object({
    query: z.string().min(1),
    limit: z.number().int().positive().optional().default(5),
})

export const AskBody = z.object({
    query: z.string().min(1),
    limit: z.number().int().positive().optional().default(10),
})

export const IngestBody = z.object({
    docs: z.array(z.object({
        content: z.string().min(1),
        source: z.string().optional(),
        created_at: z.string().optional(),
        metadata: z.record(z.string(), z.unknown()).optional(),
    })).min(1),
})
