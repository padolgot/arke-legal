# Arke Terminal

AI document search for legal teams. Privilege-safe, on-premise.

![Dashboard](/.github/media/dashboard.png)

Cloud AI breaks attorney-client privilege (*United States v. Heppner*, *Hamid v SSHD*). Arke runs on your server. Your documents never leave your network.

Hybrid search (semantic + keyword) over your documents. Ask questions, get answers with source references. Click a source to open it in your default application.

## Quick start (Docker)

```bash
docker compose up --build
```

Opens dashboard at [localhost:8000](http://localhost:8000). Pulls models automatically on first run.

## Quick start (local)

Requires Python 3.12+, Postgres with `pgvector`, and [Ollama](https://ollama.com).

```bash
pip install arke-terminal
cp .env.example .env   # fill in DATABASE_URL and DATA_PATH
arke ingest ./your-documents
arke serve
```

## CLI

```bash
arke ingest <path>                       # index documents
arke ask "query"                         # search from terminal
arke serve                               # start dashboard + API
arke sweep <fast|medium|thorough> -l 30  # run eval benchmark
```

## Library

```python
from arke import Arke, Config

cfg = Config(database_url="postgresql://...", data_path="./docs")

async with Arke(cfg) as engine:
    await engine.ingest("./docs")
    result = await engine.ask("What are the termination clauses?")
    print(result.answer)
    for hit in result.hits:
        print(hit.chunk.source, hit.similarity)
```

## Input formats

**Plain text** (.txt) — loaded directly, source = relative path from root.

**JSONL** — one document per line:

```json
{"content": "...", "source": "optional", "created_at": "2026-04-01T12:00:00Z", "metadata": {}}
```

Only `content` is required.
