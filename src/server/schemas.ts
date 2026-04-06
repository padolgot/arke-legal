import {z} from "zod/v4"

export const SearchBody = z.object({
    query: z.string().min(1),
})

export const AskBody = z.object({
    query: z.string().min(1),
})

export const IngestBody = z.object({
    sourcePath: z.string().min(1),
})
