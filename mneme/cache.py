"""File-based JSONL cache for expensive operations."""
import hashlib
import json
from pathlib import Path

CACHE_DIR = Path(".cache")


def corpus_hash(source_path: str) -> str:
    """Deterministic hash of source file(s) content."""
    h = hashlib.md5()
    p = Path(source_path)
    if p.is_file():
        h.update(p.read_bytes())
    elif p.is_dir():
        for f in sorted(p.rglob("*.jsonl")):
            h.update(f.read_bytes())
    return h.hexdigest()[:12]


class Cache:
    """Generic JSONL cache. Doesn't know what it stores — just dicts."""

    def __init__(self, **params: object) -> None:
        raw = json.dumps(params, sort_keys=True, default=str)
        key = hashlib.md5(raw.encode()).hexdigest()[:16]
        self._path = CACHE_DIR / f"{key}.jsonl"

    @property
    def path(self) -> Path:
        return self._path

    def exists(self) -> bool:
        return self._path.exists()

    def load(self) -> list[dict] | None:
        if not self._path.exists():
            return None
        return [json.loads(line) for line in self._path.read_text().splitlines()]

    def save(self, rows: list[dict]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._path, "w") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")
