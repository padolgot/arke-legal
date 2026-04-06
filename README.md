# Mneme

Local RAG pipeline.

## Schema

```sql
CREATE EXTENSION vector;

CREATE TABLE chunks (
    id          SERIAL PRIMARY KEY,
    source      TEXT NOT NULL CHECK (length(source) > 0),
    chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),
    content     TEXT NOT NULL CHECK (length(content) > 0),
    embedding   vector(1024) NOT NULL,
    metadata    JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(metadata) = 'object'),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    tsv         tsvector GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED NOT NULL
);

CREATE UNIQUE INDEX chunks_content_unique ON chunks (md5(content));
CREATE INDEX chunks_tsv_idx ON chunks USING GIN (tsv);
```

## Usage

```bash
npx tsx src/cli.ts ingest <file.jsonl | dir>
npx tsx src/cli.ts search "query"
npx tsx src/cli.ts ask "query"
npx tsx src/cli.ts sweep <fast|medium|thorough> [limit]
```
