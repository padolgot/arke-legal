"""Inbox/outbox file queue. Arke's only coordination primitive.

Layout:
    ~/.arke/workspaces/<name>/inbox/<uuid>.json   — incoming request
    ~/.arke/workspaces/<name>/outbox/<uuid>.json  — response written by Arke

Writers drop a file in inbox and poll outbox for the response.
Arke scans inbox on every tick, processes, writes outbox.
"""
import json
import time
import uuid
from pathlib import Path

POLL_INTERVAL = 0.1   # seconds between outbox polls
POLL_TIMEOUT  = 120.0 # max seconds to wait for a response

_inbox:  Path | None = None
_outbox: Path | None = None


def setup(inbox: Path, outbox: Path) -> None:
    global _inbox, _outbox
    _inbox  = inbox
    _outbox = outbox
    _inbox.mkdir(parents=True, exist_ok=True)
    _outbox.mkdir(parents=True, exist_ok=True)


def send(request: dict, workspace_path: Path) -> str:
    """Write a request to inbox. Returns the message id."""
    msg_id = str(uuid.uuid4())
    inbox = workspace_path / "inbox"
    inbox.mkdir(parents=True, exist_ok=True)
    _atomic_write(inbox / f"{msg_id}.json", request)
    return msg_id


def receive(msg_id: str, workspace_path: Path) -> dict | None:
    """Poll outbox for a response. Blocks until response arrives or timeout."""
    path = workspace_path / "outbox" / f"{msg_id}.json"
    deadline = time.monotonic() + POLL_TIMEOUT

    while time.monotonic() < deadline:
        if path.exists():
            data = json.loads(path.read_text())
            path.unlink(missing_ok=True)
            return data
        time.sleep(POLL_INTERVAL)

    return None


def drain() -> list[tuple[str, dict]]:
    """Return all pending inbox messages as (msg_id, request) pairs. Removes files."""
    assert _inbox is not None, "mailbox.setup() not called"
    messages: list[tuple[str, dict]] = []
    for f in sorted(_inbox.glob("*.json")):
        try:
            data = json.loads(f.read_text())
            messages.append((f.stem, data))
            f.unlink(missing_ok=True)
        except Exception:
            f.unlink(missing_ok=True)
    return messages


def reply(msg_id: str, response: dict) -> None:
    """Write a response to outbox."""
    assert _outbox is not None, "mailbox.setup() not called"
    _atomic_write(_outbox / f"{msg_id}.json", response)


def _atomic_write(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data))
    tmp.replace(path)
