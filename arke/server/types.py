import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from . import sdb


@dataclass
class Chunk:
    doc_id: str
    chunk_index: int
    clean: str
    head: str
    tail: str

    # Set after case-name extraction so the embedder sees the chunk anchored
    # to its document identity. Empty = no header (fallback for non-judgment docs).
    context_header: str = ""

    # Runtime only — not serialized. Loaded from sdb.get_vec or computed on GPU.
    embedding: np.ndarray | None = field(default=None, compare=False, repr=False)

    def overlapped(self) -> str:
        return self.head + self.clean + self.tail

    def baked(self) -> str:
        """Exact text the embedder consumes — overlapped chunk with the
        contextual header prepended. cache_key keys off this so any change
        to the recipe (header content, format) auto-invalidates the cache."""
        if self.context_header:
            return f"{self.context_header}\n\n{self.overlapped()}"
        return self.overlapped()

    def cache_key(self, model_id: str, model_version: str) -> str:
        # Strip path so a model relocated on disk doesn't invalidate the cache.
        model_short = Path(model_id).name
        raw = f"{model_short}:{model_version}:{self.baked()}"
        return hashlib.md5(raw.encode()).hexdigest()

    def save_embedding(self, model_id: str, model_version: str) -> None:
        if self.embedding is None:
            return
        sdb.put_vec("embeddings", self.cache_key(model_id, model_version), self.embedding)

    def load_embedding(self, model_id: str, model_version: str) -> bool:
        vec = sdb.get_vec("embeddings", self.cache_key(model_id, model_version))
        if vec is None:
            return False
        self.embedding = vec
        return True


@dataclass
class Doc:
    id: str
    source: str
    created: int
    modified: int
    metadata: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    chunks: list[Chunk] = field(default_factory=list, compare=False, repr=False)

    @property
    def label(self) -> str:
        return self.metadata.get("filename") or self.source or self.id[:8]


@dataclass(frozen=True)
class SearchHit:
    chunk: Chunk
    similarity: float
