import {app} from "./app.js"
import {pool} from "../db.js"

const port = Number(process.env.PORT)
if (!port)
{
    console.error("PORT is not set in environment")
    process.exit(1)
}

const server = app.listen(port, () =>
{
    console.log(`mneme listening on :${port}`)
})

function shutdown()
{
    console.log("Shutting down...")
    server.close(() =>
    {
        pool.end().then(() => process.exit(0))
    })
}

process.on("SIGTERM", shutdown)
process.on("SIGINT", shutdown)

process.on("uncaughtException", (err) =>
{
    console.error("UNCAUGHT:", err)
    shutdown()
})

process.on("unhandledRejection", (reason) =>
{
    console.error("UNHANDLED REJECTION:", reason)
    shutdown()
})
