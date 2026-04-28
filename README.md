# 🏺 Arke

**Authority retrieval over your firm's corpus. Privately. Via email.**

Arke reads your firm's corpus and answers your questions with direct citations — nothing else. No summarisation, no LLM paraphrase, no cloud round-trip. Built for litigators and dispute partners who want to surface the counter-case already hiding in their own documents.

Arke never edits your documents — it only caches them, read-only. Worst case, retrieval comes back thin; your data stays on your server and never leaves your network. Best case, you have an always-available retrieval layer surfacing the counter-authority you'd want to know about before court.

## How it's built

A single Python process. Three one-way pipes in the shape of a **T** with a hat.

**Vertical — data.** Your source of truth (OneDrive, SharePoint, iManage, NetDocuments, or a local folder) is mirrored read-only into cold storage by hash. Arke never writes back. On start, changed documents bloom into RAM as chunks and embeddings.

**Horizontal — signal.** A question arrives via email, TUI, or CLI — same mailbox, same pipe. It crosses the vertical at the RAM junction, picks up the citations it needs, and leaves as an answer in the outbox.

**Eval loop.** A third pipe that feeds the T with LegalBench-RAG question/answer pairs and measures the output. The system sees itself and calibrates.

**Citations are the answer.** The LLM doesn't speak for Arke — it only helps select the right quotes from your corpus. Arke is mute; it speaks through your documents.

No Postgres. No Docker. No vector DB. No third-party framework. Everything lives in RAM while running and in plain JSON on disk.

## Interfaces

**Email** — primary. Self-hosted, on your server. A lawyer sends a question to `arke@yourfirm.com`; Arke replies with citations.

```bash
arke-mail
```

**TUI** — for the legal quant who wants to iterate fast on their own corpus, chat-style, like a coding agent.

**CLI** — for other agents talking directly to the engine.

```bash
arke ask "What are the termination clauses?"
```

**Eval sweep** — find the best retrieval config (chunk size, overlap, alpha, k) across presets:

```bash
arke-eval --space legalbench --level medium --limit 50
```

**Source sync** — one-way mirror of your document source into cold storage:

```bash
arke-sync
```

## Backends

| Backend | When to use                                                    |
|---------|----------------------------------------------------------------|
| `local` | Production. GPU server, `.gguf` models. Zero external calls.   |
| `cloud` | BAILII/LegalBench eval and development. OpenAI-compatible API. |

## Input formats

PDF, DOCX, MSG (Outlook), TXT, Markdown.

## Configuration

All via environment variables (`.env`):

| Variable | Default | Description |
|----------|---------|-------------|
| `ARKE_WORKSPACE` | `default` | Workspace name |
| `CHUNK_SIZE` | `600` | Characters per chunk |
| `OVERLAP` | `0.0` | Overlap fraction (0–0.5) |
| `ALPHA` | `0.7` | Blend: 1.0 = pure semantic, 0.0 = pure keyword |
| `K` | `5` | Top-k results passed to LLM |

## Getting it into your firm

Arke is open source under MIT. Clone it, run it, own it.

## Public demo

`ask@arke.legal` — a live instance running on a UK CAT + EU competition law corpus. Send a pleading, get the counter-authorities back.
