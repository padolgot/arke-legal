"""Space — an isolated container for all Arke state.

Each space has its own sdb root and its own config. Switching spaces
is just calling mount() with a different name — the old in-memory
objects become invalid, the new space's disk state takes over.

Layout:
    <home>/<name>/
        config.json    — chunk params, model, source_dirs
        data/          — sdb root (documents, chunks, embeddings, sessions)
"""
from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

from . import sdb

ARKE_HOME = Path("~/.arke")


@dataclass(frozen=True)
class Space:
    name: str
    path: Path

    @property
    def data(self) -> Path:
        return self.path / "data"

    @property
    def config_path(self) -> Path:
        return self.path / "config.json"

    def wipe(self) -> None:
        """Erase all indexed data. Config is preserved."""
        if self.data.exists():
            shutil.rmtree(self.data)
        sdb.mount(self.data)


def mount(name: str, home: str | Path = ARKE_HOME) -> Space:
    """Mount a space by name. Sets the sdb root. Config is loaded separately."""
    path = Path(home).expanduser() / "dataspaces" / name
    path.mkdir(parents=True, exist_ok=True)
    space = Space(name=name, path=path)
    sdb.mount(space.data)
    return space
