# Mneme

Local RAG pipeline with built-in evaluation. Postgres + pgvector for hybrid search, Ollama for embeddings and inference.

## Setup

Requires Python 3.12+, [uv](https://docs.astral.sh/uv/), Postgres with the `pgvector` extension, and a running Ollama instance.

Copy `.env.example` to `.env` and fill in your values, then install and initialize the schema:

```bash
cp .env.example .env
uv sync
uv run python -m mneme.cli init
```

## Usage

```bash
uv run python -m mneme.cli ingest <file.jsonl | dir>
uv run python -m mneme.cli ask "query"
uv run python -m mneme.cli sweep <fast|medium|thorough> --limit 30
```

`sweep` reads `SOURCE_PATH` from the environment.

## Library

```python
from mneme import Mneme, MnemeConfig, Eval

cfg = MnemeConfig(database_url="postgresql://...")

async with Mneme(cfg) as m:
    await m.ingest("./corpus")
    answer = await m.ask("What is X?")

rows = await Eval(cfg).sweep("medium", limit=30, source_path="./corpus")
for row in rows:
    print(row.cfg.chunk_size, row.cfg.alpha, row.metrics.mrr)
```

## Input format

JSONL, one document per line:

```json
{"content": "...", "source": "optional", "created_at": "2026-04-01T12:00:00Z", "metadata": {}}
```

Only `content` is required. `source` falls back to the file stem, `created_at` to the current time, `metadata` to `{}`.
