"""Digest: parse raw source into cached JSONL.

Converts any supported source (local JSONL, URL) into a normalized
cache file that sweep and ingest can consume. This is the single
entry point for all data — no silent downloads, no fallbacks.
"""
from .cache import Cache, corpus_hash
from .corpus import download_legalbench
from .loader import load_docs
from .types import Doc


def digest(data_path: str) -> str:
    """Parse source into cached JSONL. Returns path to cache file.
    Idempotent — if cache exists for this source, returns immediately."""
    if not data_path:
        raise ValueError("data_path is required — set DATA_PATH in .env")

    if data_path.startswith("http"):
        return _digest_url(data_path)
    return _digest_local(data_path)


def _digest_url(url: str) -> str:
    corpus_path = download_legalbench()
    return _digest_local(corpus_path)


def _digest_local(path: str) -> str:
    chash = corpus_hash(path)
    cache = Cache(local=chash)
    if cache.exists():
        return str(cache.path)

    docs = load_docs(path)
    rows = [_doc_to_dict(d) for d in docs]
    cache.save(rows)
    print(f"digested {len(rows)} docs from {path}")
    return str(cache.path)


def _doc_to_dict(doc: Doc) -> dict:
    return {
        "content": doc.content,
        "source": doc.source,
        "metadata": doc.metadata,
        "created_at": doc.created_at.isoformat(),
    }
