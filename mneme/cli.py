import asyncio

import click
from dotenv import load_dotenv

from . import Mneme
from .config import Config


def _mneme() -> Mneme:
    return Mneme(Config.from_env())


@click.group(help="Mneme — RAG with built-in eval")
def app() -> None:
    pass


@app.command(help="Ingest documents from a JSONL file or directory.")
@click.argument("source")
def ingest(source: str) -> None:
    async def run() -> None:
        async with _mneme() as m:
            await m.ingest(source)
    asyncio.run(run())


@app.command(help="Ask a question against the ingested corpus.")
@click.argument("query")
def ask(query: str) -> None:
    async def run() -> None:
        async with _mneme() as m:
            answer = await m.ask(query)
            print(f"\n{answer}\n")
    asyncio.run(run())


@app.command(help="Run an eval sweep across preset configurations.")
@click.argument("level")
@click.argument("source", default="")
@click.option("--limit", "-l", default=30, type=int, help="Number of sample chunks for eval")
def sweep(level: str, source: str, limit: int) -> None:
    async def run() -> None:
        cfg = Config.from_env()
        await Mneme.sweep(cfg, level, limit, source)
    asyncio.run(run())


def main() -> None:
    load_dotenv()
    app()


if __name__ == "__main__":
    main()
