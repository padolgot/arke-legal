# Arke Terminal

Local RAG pipeline with built-in evaluation. Postgres + pgvector for hybrid search, Ollama for embeddings and inference.

## Install

```bash
pip install arke-terminal
```

Requires Python 3.12+, Postgres with `pgvector`, and [Ollama](https://ollama.com).

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

## Usage

```bash
arke digest
arke ingest <file.jsonl>
arke ask "query"
arke sweep <fast|medium|thorough> --limit 30
```

## Library

```python
from arke import Arke, Config

cfg = Config(database_url="postgresql://...", api_key="sk-...")

async with Arke(cfg) as m:
    await m.ingest("./corpus")
    answer = await m.ask("What is X?")

rows = await Arke.sweep(cfg, "medium", limit=30)
```

## Input format

JSONL, one document per line:

```json
{"content": "...", "source": "optional", "created_at": "2026-04-01T12:00:00Z", "metadata": {}}
```

Only `content` is required. `source` falls back to the file stem, `created_at` to the current time, `metadata` to `{}`.
