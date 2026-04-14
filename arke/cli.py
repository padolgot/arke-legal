import asyncio

import click
from dotenv import load_dotenv

from . import Arke
from .config import Config


def _arke() -> Arke:
    return Arke(Config.from_env())


@click.group(help="Arke — RAG with built-in eval")
def app() -> None:
    pass


@app.command(help="Digest raw source into cached JSONL.")
@click.argument("source", default="")
def digest(source: str) -> None:
    cfg = Config.from_env()
    data_path = source or cfg.data_path
    Arke.digest(data_path)


@app.command(help="Ingest documents from a JSONL file or directory.")
@click.argument("source")
def ingest(source: str) -> None:
    async def run() -> None:
        async with _arke() as m:
            await m.ingest(source)
    asyncio.run(run())


@app.command(help="Ask a question against the ingested corpus.")
@click.argument("query")
def ask(query: str) -> None:
    async def run() -> None:
        async with _arke() as m:
            result = await m.ask(query)
            print(f"\n{result.answer}\n")
    asyncio.run(run())


@app.command(help="Run an eval sweep across preset configurations.")
@click.argument("level")
@click.option("--limit", "-l", default=30, type=int, help="Number of sample chunks for eval")
def sweep(level: str, limit: int) -> None:
    async def run() -> None:
        cfg = Config.from_env()
        await Arke.sweep(cfg, level, limit)
    asyncio.run(run())


@app.command(help="Start the REST API server.")
@click.option("--port", "-p", default=8000, type=int, help="Port to listen on")
def serve(port: int) -> None:
    import uvicorn
    from .api import app as api_app
    uvicorn.run(api_app, host="0.0.0.0", port=port)


def main() -> None:
    load_dotenv()
    try:
        app()
    except (RuntimeError, ValueError) as exc:
        print(f"error: {exc}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
