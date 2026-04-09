import asyncio
import os

import click

from .core.config import MnemeConfig
from .eval import Eval
from .mneme import Mneme


def _mneme() -> Mneme:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise click.UsageError("DATABASE_URL is not set in environment")
    return Mneme(MnemeConfig(database_url=database_url))


@click.group(help="Mneme — RAG with built-in eval")
def app() -> None:
    pass


@app.command(help="Create schema and verify connection.")
def init() -> None:
    async def run() -> None:
        async with _mneme() as m:
            await m.create_schema()
    asyncio.run(run())


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
@click.option("--limit", "-l", default=30, type=int, help="Number of sample chunks for eval")
def sweep(level: str, limit: int) -> None:
    source_path = os.environ.get("SOURCE_PATH")
    if not source_path:
        raise click.UsageError("SOURCE_PATH is not set in environment")

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise click.UsageError("DATABASE_URL is not set in environment")
    base_cfg = MnemeConfig(database_url=database_url)

    async def run() -> None:
        await Eval(base_cfg).sweep(level, limit, source_path)
    asyncio.run(run())


def main() -> None:
    app()


if __name__ == "__main__":
    main()
