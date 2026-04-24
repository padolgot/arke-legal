import hashlib
from dataclasses import dataclass, field
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

    # Runtime only — not serialized. Loaded from sdb.get_vec or computed on GPU.
    embedding: np.ndarray | None = field(default=None, compare=False, repr=False)

    def overlapped(self) -> str:
        return self.head + self.clean + self.tail

    def cache_key(self, model_id: str, model_version: str) -> str:
        raw = f"{model_id}:{model_version}:{self.overlapped()}"
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


@dataclass(frozen=True)
class SearchHit:
    chunk: Chunk
    similarity: float


@dataclass(frozen=True)
class SearchAnswer:
    answer: str
    hits: list[SearchHit]
