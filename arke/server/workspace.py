"""Workspace — an isolated container for all Arke state.

Each workspace has its own sdb root. Switching workspaces is just
calling mount() with a different name.

Layout:
    ~/.arke/workspaces/<name>/
        data/      — sdb root (documents, embeddings, sources)
        digest/    — drop documents here; Arke ingests on change
        inbox/     — incoming requests
        outbox/    — responses
"""
from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path

from . import sdb

ARKE_HOME = Path(os.environ.get("ARKE_ROOT") or (Path.home() / ".arke"))


@dataclass(frozen=True)
class Workspace:
    name: str
    path: Path

    @property
    def data(self) -> Path:
        return self.path / "data"

    @property
    def inbox(self) -> Path:
        return self.path / "inbox"

    @property
    def outbox(self) -> Path:
        return self.path / "outbox"

    def wipe(self) -> None:
        """Erase all indexed data."""
        if self.data.exists():
            shutil.rmtree(self.data)
        sdb.mount(self.data)


def mount(name: str, home: str | Path = ARKE_HOME) -> Workspace:
    """Mount a workspace by name. Sets the sdb root."""
    path = Path(home).expanduser() / "workspaces" / name
    path.mkdir(parents=True, exist_ok=True)
    ws = Workspace(name=name, path=path)
    sdb.mount(ws.data)
    return ws
