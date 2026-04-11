# Mneme

Local RAG pipeline with built-in evaluation. Postgres + pgvector for hybrid search, any /v1/-compatible LLM backend for embeddings and inference.

## Setup

Requires Python 3.12+, [uv](https://docs.astral.sh/uv/), Postgres with the `pgvector` extension.

Copy `.env.example` to `.env` and fill in your values, then install:

```bash
cp .env.example .env
uv sync
```

## Usage

```bash
uv run mneme digest              # parse DATA_PATH source into cache
uv run mneme ingest <file.jsonl>
uv run mneme ask "query"
uv run mneme sweep <fast|medium|thorough> --limit 30
```

## Library

```python
from mneme import Mneme, Config

cfg = Config(database_url="postgresql://...", api_key="sk-...")

async with Mneme(cfg) as m:
    await m.ingest("./corpus")
    answer = await m.ask("What is X?")

rows = await Mneme.sweep(cfg, "medium", limit=30)
```

## Input format

JSONL, one document per line:

```json
{"content": "...", "source": "optional", "created_at": "2026-04-01T12:00:00Z", "metadata": {}}
```

Only `content` is required. `source` falls back to the file stem, `created_at` to the current time, `metadata` to `{}`.
