"""TUI client. Run: python -m arke.clients.tui"""
import os

from arke.server import mailbox, workspace


def run() -> None:
    ws = workspace.path_for(os.environ.get("ARKE_WORKSPACE", "default"))
    print("Arke TUI — paste your argument, Ctrl-C to exit\n")

    while True:
        try:
            argument = input("> ").strip()
        except (KeyboardInterrupt, EOFError):
            print()
            break

        if not argument:
            continue

        msg_id = mailbox.send({"cmd": "stress", "argument": argument}, ws)
        response = mailbox.receive(msg_id, ws)

        if response is None:
            print("error: arke did not respond\n")
            continue

        if not response.get("ok"):
            print(f"error: {response.get('error')}\n")
            continue

        print(f"\n{response['answer']}\n")


if __name__ == "__main__":
    run()
