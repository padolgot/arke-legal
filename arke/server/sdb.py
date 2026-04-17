"""Unified file-based store for all of Arke's cached state.

Everything that passes through Arke is cache — documents, embeddings, source files,
sessions. Microsoft (or the user's filesystem) is the source of truth; sdb is the
gut that digests and holds the processed form. Blow it away at any time — it
rebuilds from the source.

Layout:
    <root>/<table>/<id[:2]>/<id>.<ext>

Sharding by first two characters of the id keeps any single directory small
regardless of corpus size.
"""
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
import json
import os
import shutil

import numpy as np


_root: Path | None = None


def mount(path: str | Path) -> None:
    global _root
    _root = Path(path).expanduser()
    _root.mkdir(parents=True, exist_ok=True)
    for tmp in _root.rglob("*.tmp"):
        tmp.unlink(missing_ok=True)


def _table(table: str) -> Path:
    return _root / table


def _shard(table: str, id: str) -> Path:
    return _table(table) / id[:2]


def _path(table: str, id: str, ext: str) -> Path:
    suffix = f".{ext}" if ext else ""
    return _shard(table, id) / f"{id}{suffix}"


@contextmanager
def _atomic_open(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "wb") as f:
        yield f
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)


# JSON — documents, sessions, any structured record -----------------------

def put_json(table: str, id: str, data: dict) -> None:
    with _atomic_open(_path(table, id, "json")) as f:
        f.write(json.dumps(data, indent=2).encode())


def get_json(table: str, id: str) -> dict | None:
    p = _path(table, id, "json")
    if not p.exists():
        return None
    return json.loads(p.read_text())


def scan_json(table: str) -> Iterator[tuple[str, dict]]:
    table_dir = _table(table)
    if not table_dir.exists():
        return
    for f in table_dir.rglob("*.json"):
        yield f.stem, json.loads(f.read_text())


# Vectors — embeddings --------------------------------------------------------

def put_vec(table: str, id: str, vec: np.ndarray) -> None:
    with _atomic_open(_path(table, id, "npy")) as f:
        np.save(f, vec)


def get_vec(table: str, id: str) -> np.ndarray | None:
    p = _path(table, id, "npy")
    if not p.exists():
        return None
    return np.load(p)


# Raw bytes — source files (PDF, DOCX, MSG), any opaque blob ------------------

def put_bin(table: str, id: str, data: bytes) -> None:
    with _atomic_open(_path(table, id, "")) as f:
        f.write(data)


def get_bin(table: str, id: str) -> bytes | None:
    p = _path(table, id, "")
    if not p.exists():
        return None
    return p.read_bytes()


# Delete ----------------------------------------------------------------------

def delete(table: str, id: str) -> None:
    """Remove a single record (any extension) from the table."""
    shard = _shard(table, id)
    if not shard.exists():
        return
    for f in shard.iterdir():
        if f.name == id or f.name.startswith(f"{id}."):
            f.unlink(missing_ok=True)


def wipe(table: str) -> None:
    """Remove the entire table — every record under every shard."""
    table_dir = _table(table)
    if table_dir.exists():
        shutil.rmtree(table_dir)
