"""Unix socket gateway. Knows nothing about Arke internals.

Accepts a connection, reads a JSON request, drops it in inbox,
polls outbox for the response, sends it back, closes the connection.
"""
import json
import logging
import socket
from pathlib import Path

from arke.server import mailbox

logger = logging.getLogger(__name__)

SOCK_PATH = Path("~/.arke/arke.sock").expanduser()


def run(sock_path: Path = SOCK_PATH) -> None:
    """Start socket gateway. Blocks forever — run in a dedicated thread or process."""
    mailbox.setup()

    if sock_path.exists():
        sock_path.unlink()

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(sock_path))
    server.listen()
    logger.info("gateway listening on %s", sock_path)

    try:
        while True:
            conn, _ = server.accept()
            conn.settimeout(30)
            try:
                _handle(conn)
            except Exception as e:
                logger.warning("connection error: %s", e)
            finally:
                conn.close()
    finally:
        server.close()
        if sock_path.exists():
            sock_path.unlink()


def _handle(conn: socket.socket) -> None:
    f = conn.makefile("rb")
    line = f.readline()
    if not line:
        return

    try:
        request = json.loads(line)
    except json.JSONDecodeError as e:
        _send(conn, {"ok": False, "error": f"invalid JSON: {e}"})
        return

    logger.info("cmd=%s", request.get("cmd"))

    msg_id = mailbox.send(request)
    response = mailbox.receive(msg_id)

    if response is None:
        _send(conn, {"ok": False, "error": "arke did not respond in time"})
        return

    _send(conn, response)


def _send(conn: socket.socket, data: dict) -> None:
    conn.sendall(json.dumps(data).encode() + b"\n")
