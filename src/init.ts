import {pool} from "./db.js"

export async function init()
{
    const dim = Number(process.env.EMBEDDING_DIM)
    if (!Number.isInteger(dim) || dim < 1)
        throw new Error(`init: EMBEDDING_DIM must be positive integer, got ${process.env.EMBEDDING_DIM}`)

    await pool.query(`CREATE EXTENSION IF NOT EXISTS vector`)

    await pool.query(`
        CREATE TABLE IF NOT EXISTS chunks (
            id          SERIAL PRIMARY KEY,
            source      TEXT NOT NULL CHECK (length(source) > 0),
            chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),
            content     TEXT NOT NULL CHECK (length(content) > 0),
            embedding   vector(${dim}) NOT NULL,
            metadata    JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(metadata) = 'object'),
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            tsv         tsvector GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED NOT NULL
        )
    `)

    await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS chunks_content_unique ON chunks (md5(content))`)
    await pool.query(`CREATE INDEX IF NOT EXISTS chunks_tsv_idx ON chunks USING GIN (tsv)`)

    console.log(`init: schema ready, embedding dim=${dim}`)
}
