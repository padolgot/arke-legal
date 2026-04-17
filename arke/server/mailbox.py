"""Inbox/outbox file queue. Arke's only coordination primitive.

Layout:
    ~/.arke/inbox/<uuid>.json   — incoming request
    ~/.arke/outbox/<uuid>.json  — response written by Arke

Writers drop a file in inbox and poll outbox for the response.
Arke scans inbox on every tick, processes, writes outbox.
"""
import json
import os
import time
import uuid
from pathlib import Path

ARKE_HOME = Path("~/.arke").expanduser()
INBOX = ARKE_HOME / "inbox"
OUTBOX = ARKE_HOME / "outbox"

POLL_INTERVAL = 0.1   # seconds between outbox polls
POLL_TIMEOUT  = 120.0 # max seconds to wait for a response


def setup() -> None:
    INBOX.mkdir(parents=True, exist_ok=True)
    OUTBOX.mkdir(parents=True, exist_ok=True)


def send(request: dict) -> str:
    """Write a request to inbox. Returns the message id."""
    msg_id = str(uuid.uuid4())
    _atomic_write(INBOX / f"{msg_id}.json", request)
    return msg_id


def receive(msg_id: str) -> dict | None:
    """Poll outbox for a response. Blocks until response arrives or timeout."""
    path = OUTBOX / f"{msg_id}.json"
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
    messages: list[tuple[str, dict]] = []
    for f in sorted(INBOX.glob("*.json")):
        try:
            data = json.loads(f.read_text())
            messages.append((f.stem, data))
            f.unlink(missing_ok=True)
        except Exception:
            f.unlink(missing_ok=True)
    return messages


def reply(msg_id: str, response: dict) -> None:
    """Write a response to outbox."""
    _atomic_write(OUTBOX / f"{msg_id}.json", response)


def _atomic_write(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data))
    tmp.replace(path)
