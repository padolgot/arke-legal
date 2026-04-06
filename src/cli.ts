import "dotenv/config"
import {loadConfig, ingest, search, infer} from "./pipeline/index.js"
import {sweep, parseSweepLevel, getPreset} from "./eval/index.js"

const cfg = loadConfig()
const command = process.argv[2]

if (command === "ingest")
{
    const target = process.argv[3]
    if (!target)
    {
        console.error("Usage: npx tsx src/cli.ts ingest <file.jsonl | directory>")
        process.exit(1)
    }

    await ingest(target, cfg.chunk)
}
else if (command === "search")
{
    const query = process.argv[3]
    if (!query)
    {
        console.error("Usage: npx tsx src/cli.ts search \"query\"")
        process.exit(1)
    }

    const results = await search(query, cfg.search)

    console.log(`search: "${query}" top ${cfg.search.k}`)
    for (const row of results)
    {
        const sim = row.similarity.toFixed(4)
        const date = new Date(row.created_at).toISOString().slice(0, 10)
        console.log(`[${sim}] ${date} | ${row.source} | ${row.content}`)
    }
}
else if (command === "ask")
{
    const query = process.argv[3]
    if (!query)
    {
        console.error("Usage: npx tsx src/cli.ts ask \"query\"")
        process.exit(1)
    }

    const results = await search(query, cfg.search)
    console.log(`ask: ${results.length} context chunks`)

    const answer = await infer(query, results)
    console.log(`\n${answer}\n`)
}
else if (command === "sweep")
{
    const levelStr = process.argv[3]
    if (!levelStr)
    {
        console.error("Usage: npx tsx src/cli.ts sweep <fast|medium|thorough> [limit]")
        process.exit(1)
    }

    const level = parseSweepLevel(levelStr)
    const limit = Number(process.argv[4]) || 30
    const sourcePath = process.env.SOURCE_PATH
    const presets = getPreset(level)

    await sweep(presets, limit, sourcePath)
}
else
{
    console.error("Usage:")
    console.error("  npx tsx src/cli.ts ingest <file.jsonl | directory>")
    console.error("  npx tsx src/cli.ts search \"query\"")
    console.error("  npx tsx src/cli.ts ask \"query\"")
    console.error("  npx tsx src/cli.ts sweep <fast|medium|thorough> [limit]")
    process.exit(1)
}
