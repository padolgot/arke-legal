import "dotenv/config"
import {readFileSync, readdirSync, statSync} from "fs"
import {basename} from "path"
import {ingest, type Doc} from "./ingestion.js"
import {search} from "./searcher.js"
import {ask} from "./inference.js"

const command = process.argv[2]

if (command === "ingest")
{
    const target = process.argv[3]
    if (!target)
    {
        console.error("Usage: npx tsx src/cli.ts ingest <file.jsonl | directory>")
        process.exit(1)
    }

    const files: string[] = statSync(target).isDirectory()
        ? readdirSync(target).filter(f => f.endsWith(".jsonl")).map(f => `${target}/${f}`)
        : [target]

    for (const file of files)
    {
        const source = basename(file, ".jsonl")
        const lines = readFileSync(file, "utf-8").split("\n").filter(l => l.trim())
        const docs: Doc[] = lines.map(l =>
        {
            const d = JSON.parse(l) as Doc
            d.source = d.source ?? source
            return d
        })

        console.log(`\n── ${source} (${docs.length} docs) ──`)
        await ingest(docs)
    }
    console.log("\nDone")
}
else if (command === "search")
{
    const query = process.argv[3]
    const limit = Number(process.argv[4]) || 5

    if (!query)
    {
        console.error("Usage: npx tsx src/cli.ts search \"вопрос\" [limit]")
        process.exit(1)
    }

    const results = await search(query, limit)

    console.log(`\n── "${query}" (top ${limit}) ──\n`)
    for (const row of results)
    {
        const sim = row.similarity.toFixed(4)
        const date = new Date(row.created_at).toISOString().slice(0, 10)
        console.log(`[${sim}] ${date} | ${row.source}`)
        console.log(`  ${row.content}`)
        console.log()
    }
}
else if (command === "ask")
{
    const query = process.argv[3]
    const limit = Number(process.argv[4]) || 10

    if (!query)
    {
        console.error("Usage: npx tsx src/cli.ts ask \"вопрос\" [limit]")
        process.exit(1)
    }

    console.log(`\n── Searching... ──`)
    const answer = await ask(query, limit)
    console.log(`\n${answer}\n`)
}
else
{
    console.error("Usage:")
    console.error("  npx tsx src/cli.ts ingest <file.jsonl | directory>")
    console.error("  npx tsx src/cli.ts search \"вопрос\" [limit]")
    console.error("  npx tsx src/cli.ts ask \"вопрос\" [limit]")
    process.exit(1)
}
