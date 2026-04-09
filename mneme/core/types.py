from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class Doc:
    content: str
    source: str
    created_at: datetime
    metadata: dict[str, Any]


@dataclass(frozen=True)
class Chunk:
    """A row in the `chunks` table. `id` is None before insert (serial),
    assigned by Postgres."""
    source: str
    chunk_index: int
    content: str
    embedding: list[float]
    created_at: datetime
    metadata: dict[str, Any] = field(default_factory=dict)
    id: int | None = None


@dataclass(frozen=True)
class SearchHit:
    chunk: Chunk
    similarity: float  # alpha*cosine + (1-alpha)*bm25_normalized
