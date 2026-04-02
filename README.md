# Mneme

Local RAG pipeline. No LangChain, no magic libs — built from ground zero.

## Stack

- TypeScript, Node.js, ESM
- PostgreSQL + pgvector
- Ollama (bge-m3 for embeddings, llama3 for inference)

## Pipeline

```
JSONL → chunker → embedder → pgvector → search → inference → answer
```

## Setup

```bash
# Prerequisites: PostgreSQL with pgvector, Ollama running

# Install
npm install

# Create database
psql -U postgres -c "CREATE DATABASE mneme"
psql -U postgres -d mneme -c "CREATE EXTENSION vector"
psql -U postgres -d mneme -c "
CREATE TABLE chunks (
    id          SERIAL PRIMARY KEY,
    source      TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    content     TEXT NOT NULL,
    embedding   vector(1024),
    metadata    JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ DEFAULT now()
);
CREATE UNIQUE INDEX chunks_content_unique ON chunks (md5(content));
"

# Pull models
ollama pull bge-m3
ollama pull llama3:8b-instruct-q4_K_M

# Configure
cp .env.example .env
```

## Usage

```bash
# Ingest JSONL files
npx tsx src/cli.ts ingest data/chats/

# Search
npx tsx src/cli.ts search "query" 5

# Ask (search + LLM inference)
npx tsx src/cli.ts ask "question" 10
```

## JSONL Format

```json
{"content": "text", "source": "origin", "created_at": "2024-01-01", "metadata": {}}
```
