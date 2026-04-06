# Mneme

Local RAG pipeline.

## Setup

Set `DATABASE_URL`, `EMBEDDER_URL`, `EMBEDDER_MODEL`, `EMBEDDING_DIM`, `INFERENCE_URL`, `INFERENCE_MODEL` in `.env`, then:

```bash
npm run cli init
```

## Usage

```bash
npm run cli ingest <file.jsonl | dir>
npm run cli search "query"
npm run cli ask "query"
npm run cli sweep <fast|medium|thorough> [limit]
npm run serve
```
