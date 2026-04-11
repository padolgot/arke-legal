import json
from datetime import datetime, timezone
from pathlib import Path

from .types import Doc


def load_docs(source_path: str) -> list[Doc]:
    path = Path(source_path)
    files = sorted(path.glob("*.jsonl")) if path.is_dir() else [path]
    docs: list[Doc] = []
    for file in files:
        docs.extend(_load_file(file))
    return docs


def _load_file(file: Path) -> list[Doc]:
    """Reads a JSONL file and returns valid docs. System boundary: invalid
    lines and invalid records are dropped silently. The file stem serves
    as a source fallback when a record omits its own."""
    fallback = file.stem
    docs: list[Doc] = []
    for line in file.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue

        try:
            raw = json.loads(line)
        except json.JSONDecodeError:
            continue

        if not isinstance(raw, dict):
            continue

        content = raw.get("content")
        if not isinstance(content, str) or not content.strip():
            continue

        source_raw = raw.get("source")
        source = source_raw if isinstance(source_raw, str) and source_raw else fallback

        raw_date = raw.get("created_at")
        created_at = datetime.now(timezone.utc)
        if isinstance(raw_date, str):
            try:
                created_at = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
            except ValueError:
                pass

        metadata_raw = raw.get("metadata")
        metadata = metadata_raw if isinstance(metadata_raw, dict) else {}

        docs.append(Doc(content=content, source=source, created_at=created_at, metadata=metadata))

    return docs
