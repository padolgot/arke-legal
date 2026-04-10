# Mneme

Local RAG pipeline with built-in evaluation. Postgres + pgvector for hybrid search, Ollama or OpenAI for embeddings and inference.

## Setup

Requires Python 3.12+, [uv](https://docs.astral.sh/uv/), Postgres with the `pgvector` extension.

Copy `.env.example` to `.env` and fill in your values, then install:

```bash
cp .env.example .env
uv sync
```

## Usage

```bash
uv run mneme ingest <file.jsonl | dir>
uv run mneme ask "query"
uv run mneme sweep <fast|medium|thorough> --limit 30
```

`sweep` without a source path downloads SQuAD as a test corpus. To use your own data:

```bash
uv run mneme sweep medium ./corpus
```

## Library

```python
from mneme import Mneme, Config, Eval

cfg = Config(database_url="postgresql://...", provider="openai", provider_api_key="sk-...")

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
