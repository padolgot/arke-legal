"""CLI client. Usage: arke ask "your question" """
import sys

from arke.server import mailbox


def ask(query: str) -> None:
    msg_id = mailbox.send({"cmd": "ask", "query": query})
    response = mailbox.receive(msg_id)

    if response is None:
        print("error: arke did not respond", file=sys.stderr)
        sys.exit(1)

    if not response.get("ok"):
        print(f"error: {response.get('error')}", file=sys.stderr)
        sys.exit(1)

    print(response["answer"])

    for cite in response.get("citations", []):
        print(f"  [{cite['source']}] {cite['text'][:80]}...")


def main() -> None:
    if len(sys.argv) < 3 or sys.argv[1] != "ask":
        print("usage: arke ask <query>")
        sys.exit(1)

    ask(" ".join(sys.argv[2:]))
