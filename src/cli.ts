import {Mneme} from "./mneme.js"
import {getPreset} from "./presets.js"

const mneme = new Mneme({
    databaseUrl: requireEnv("DATABASE_URL"),
})

const command = process.argv[2]

try
{
    if (command === "init")
    {
        await mneme.init()
    }
    else if (command === "ingest")
    {
        const target = process.argv[3]
        if (!target)
        {
            console.error("Usage: npm run cli ingest <file.jsonl | directory>")
            process.exit(1)
        }

        await mneme.ingest(target)
    }
    else if (command === "ask")
    {
        const query = process.argv[3]
        if (!query)
        {
            console.error("Usage: npm run cli ask \"query\"")
            process.exit(1)
        }

        const answer = await mneme.ask(query)
        console.log(`\n${answer}\n`)
    }
    else if (command === "sweep")
    {
        const levelStr = process.argv[3]
        if (!levelStr)
        {
            console.error("Usage: npm run cli sweep <fast|medium|thorough> [limit]")
            process.exit(1)
        }

        const limit = Number(process.argv[4]) || 30
        const sourcePath = process.env.SOURCE_PATH
        if (!sourcePath)
        {
            console.error("sweep requires SOURCE_PATH in .env")
            process.exit(1)
        }
        const presets = getPreset(levelStr, mneme.cfg)

        await mneme.sweep(presets, limit, sourcePath)
    }
    else
    {
        console.error("Usage:")
        console.error("  npm run cli init")
        console.error("  npm run cli ingest <file.jsonl | directory>")
        console.error("  npm run cli ask \"query\"")
        console.error("  npm run cli sweep <fast|medium|thorough> [limit]")
        process.exit(1)
    }
}
finally
{
    await mneme.close()
}

function requireEnv(name: string): string
{
    const v = process.env[name]
    if (!v) throw new Error(`${name} is not set in environment`)
    return v
}
