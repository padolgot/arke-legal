import json
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
    id: str
    source: str
    chunk_index: int
    content: str
    embedding: list[float]
    created_at: datetime
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "source": self.source,
            "chunk_index": self.chunk_index,
            "content": self.content,
            "embedding": [float(x) for x in self.embedding],
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
        }

    @staticmethod
    def from_dict(d: dict) -> "Chunk":
        created = d["created_at"]
        if isinstance(created, str):
            created = datetime.fromisoformat(created)

        metadata = d.get("metadata", {})
        if isinstance(metadata, str):
            metadata = json.loads(metadata)

        return Chunk(
            id=d["id"],
            source=d["source"],
            chunk_index=d["chunk_index"],
            content=d["content"],
            embedding=list(d["embedding"]),
            metadata=metadata,
            created_at=created,
        )


@dataclass(frozen=True)
class SearchHit:
    chunk: Chunk
    similarity: float  # alpha*cosine + (1-alpha)*bm25_normalized
