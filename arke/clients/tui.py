"""TUI client. Run: python -m arke.clients.tui"""
# TODO: replace with Textual when we wire up the UI
# For now: minimal readline loop so we can test mailbox end-to-end

import sys

from arke.server import mailbox


def run() -> None:
    print("Arke TUI — type your question, Ctrl-C to exit\n")

    while True:
        try:
            query = input("> ").strip()
        except (KeyboardInterrupt, EOFError):
            print()
            break

        if not query:
            continue

        msg_id = mailbox.send({"cmd": "ask", "query": query})
        response = mailbox.receive(msg_id)

        if response is None:
            print("error: arke did not respond\n")
            continue

        if not response.get("ok"):
            print(f"error: {response.get('error')}\n")
            continue

        print(f"\n{response['answer']}\n")

        for cite in response.get("citations", []):
            print(f"  [{cite['source']}] {cite['text'][:80]}...")
        print()


if __name__ == "__main__":
    run()
