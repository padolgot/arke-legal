from dataclasses import dataclass, field
from typing import Any

import numpy as np


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

    # Runtime only — not serialized. Populated by the (caching) embedder
    # at ingest time; cleared between server restarts.
    embedding: np.ndarray | None = field(default=None, compare=False, repr=False)

    def overlapped(self) -> str:
        return self.head + self.clean + self.tail

    def baked(self) -> str:
        """Exact text the embedder consumes — overlapped chunk with the
        contextual header prepended. The CachingEmbedder hashes this to
        key its cache, so any change to the recipe (header content, format)
        auto-invalidates the cache."""
        if self.context_header:
            return f"{self.context_header}\n\n{self.overlapped()}"
        return self.overlapped()


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
