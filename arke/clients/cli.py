"""CLI client. Usage: arke stress "your argument" """
import os
import sys

from arke.server import mailbox, workspace


def stress(argument: str) -> None:
    ws = workspace.path_for(os.environ.get("ARKE_WORKSPACE", "default"))
    msg_id = mailbox.send({"cmd": "stress", "argument": argument}, ws)
    response = mailbox.receive(msg_id, ws)

    if response is None:
        print("error: arke did not respond", file=sys.stderr)
        sys.exit(1)

    if not response.get("ok"):
        print(f"error: {response.get('error')}", file=sys.stderr)
        sys.exit(1)

    print(response["answer"])


def main() -> None:
    if len(sys.argv) < 3 or sys.argv[1] != "stress":
        print("usage: arke stress <argument>")
        sys.exit(1)

    stress(" ".join(sys.argv[2:]))
