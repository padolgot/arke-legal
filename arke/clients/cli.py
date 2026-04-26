"""CLI client. Usage:
    arke stress "your argument"   — full LLM mosaic pipeline (email-style answer)
    arke search "your query"      — retrieval-only, prints top-K authorities as markdown
"""
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


def search(query: str) -> None:
    ws = workspace.path_for(os.environ.get("ARKE_WORKSPACE", "default"))
    msg_id = mailbox.send({"cmd": "search", "query": query}, ws)
    response = mailbox.receive(msg_id, ws)

    if response is None:
        print("error: arke did not respond", file=sys.stderr)
        sys.exit(1)
    if not response.get("ok"):
        print(f"error: {response.get('error')}", file=sys.stderr)
        sys.exit(1)

    citations = response.get("citations", [])
    if not citations:
        print("(no results)")
        return

    for i, c in enumerate(citations, 1):
        cite_id = c.get("neutral_citation") or c.get("celex") or c.get("ecli") or c.get("canonical_id") or c["doc_id"]
        title = c.get("title") or c.get("party_slug") or "(untitled)"
        court = c.get("court", "")
        date = c.get("date", "")
        cite_in = c.get("cite_in_count")
        snippet = (c.get("snippet") or "").strip()
        if len(snippet) > 600:
            snippet = snippet[:600].rsplit(" ", 1)[0] + "…"

        header = f"**{i}. {title}** — {cite_id}"
        meta_bits = [b for b in (court, date) if b]
        if cite_in:
            meta_bits.append(f"cited {cite_in}× in corpus")
        if meta_bits:
            header += "  \n   *" + " · ".join(meta_bits) + "*"
        print(header)
        if snippet:
            print(f"   > {snippet}")
        print()


def main() -> None:
    if len(sys.argv) < 3 or sys.argv[1] not in ("stress", "search"):
        print("usage: arke {stress|search} <text>")
        sys.exit(1)

    cmd = sys.argv[1]
    text = " ".join(sys.argv[2:])
    if cmd == "stress":
        stress(text)
    else:
        search(text)
