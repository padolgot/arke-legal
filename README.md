# Arke Terminal

AI document search for legal teams. Privilege-safe, on-premise.

Cloud AI breaks attorney-client privilege (*United States v. Heppner*, *Hamid v SSHD*). Arke runs on your server. Your documents never leave your network.

---

## How it works

Arke is a single Python process with two pipes.

**Vertical pipe — digestion.** Drop documents into `digest/`. Arke detects the change, loads every file (PDF, DOCX, MSG, TXT), chunks and embeds it, keeps everything in memory and on local disk. On the next question, the answers are already there.

**Horizontal pipe — query.** A question arrives — from email, terminal, or CLI. Arke embeds the query, runs hybrid search (cosine + BM25), passes the top results to the LLM, and returns a cited answer. No round-trips, no external services: the LLM runs in the same process via `llama-cpp-python`.

The document is a seed on disk (plain JSON) and a tree in memory (chunks, embeddings, full text). The tree is always rebuilt from the seed. Nothing clever lives on disk.

---

## Install

```bash
pip install arke-terminal
cp .env.example .env   # fill in model paths or cloud API key
arke-server            # start the engine
```

No Docker. No Postgres. No Ollama daemon.

---

## Interfaces

**Email (Microsoft 365)**

Arke connects to a shared mailbox via Graph API webhooks. A lawyer sends a question to `ask@yourfirm.legal`, Arke replies with an answer and citations. Runs with `arke-mail`.

Requires: Azure AD app registration, `cloudflared` tunnel for the webhook endpoint.

```
M365_TENANT_ID, M365_CLIENT_ID, M365_CLIENT_SECRET
M365_MAILBOX, M365_WEBHOOK_URL
```

**Terminal**

```bash
arke ask "What are the termination clauses?"
```

**Eval sweep** — find the best retrieval config (chunk size, overlap, alpha, k) by running MRR benchmarks across presets:

```bash
python -m arke.eval.sweep --space legalbench --level medium --limit 50
```

---

## Backends

| Backend | When to use |
|---------|-------------|
| `local` | Production. GPU server, `.gguf` models. Zero external calls. |
| `cloud` | Eval and development. OpenAI-compatible API. |

```bash
# local
BACKEND=local
EMBED_MODEL_PATH=/models/nomic-embed.gguf
INFERENCE_MODEL_PATH=/models/mistral.gguf

# cloud
BACKEND=cloud
CLOUD_API_KEY=sk-...
CLOUD_EMBED_MODEL=text-embedding-3-small
CLOUD_INFERENCE_MODEL=gpt-4o
```

---

## Input formats

PDF, DOCX, MSG (Outlook), TXT, Markdown. Drop files into `digest/`, Arke picks them up automatically.

---

## Configuration

All via environment variables (`.env`):

| Variable | Default | Description |
|----------|---------|-------------|
| `ARKE_SPACE` | `default` | Dataspace name (isolates documents per client) |
| `CHUNK_SIZE` | `600` | Characters per chunk |
| `OVERLAP` | `0.0` | Overlap fraction (0–0.5) |
| `ALPHA` | `0.7` | Blend: 1.0 = pure semantic, 0.0 = pure keyword |
| `K` | `5` | Top-k results passed to LLM |
