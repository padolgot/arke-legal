"""Email client via Microsoft Graph polling.

Every POLL_INTERVAL seconds, list unread messages in the inbox, fetch each,
forward to arke-server, reply, mark read. No webhooks, no public ingress —
purely outbound HTTPS to Graph.

isRead=false is the source of truth for "needs answering". We mark-as-read
before replying so a crash between steps leaves a silent miss, never a
double reply.
"""

import base64
import logging
import os
import re
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import httpx
import msal

logger = logging.getLogger(__name__)

GRAPH = "https://graph.microsoft.com/v1.0"
SCOPE = ["https://graph.microsoft.com/.default"]
POLL_INTERVAL_SEC = 5
LIST_PAGE_SIZE = 50

# Graph inline attachments are capped at ~4MB per reply. Skip any single file
# above 3MB and stop attaching once the running total would exceed the budget —
# a lawyer gets a neat set of small files or none at all, never a 400 from Graph.
MAX_ATTACHMENT_BYTES = 3 * 1024 * 1024
TOTAL_ATTACHMENT_BUDGET = 3 * 1024 * 1024

MIME_TYPES = {
    ".pdf": "application/pdf",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".doc": "application/msword",
    ".msg": "application/vnd.ms-outlook",
    ".txt": "text/plain",
    ".md": "text/markdown",
    ".rtf": "application/rtf",
}

EMAIL_STYLE = "font-family:Georgia,serif;font-size:11pt;line-height:1.55;color:#222"
H3_STYLE = "margin:18px 0 8px 0"
PARA_STYLE = "margin:10px 0"
LIST_STYLE = "margin:8px 0;padding-left:24px"
LI_STYLE = "margin-bottom:6px"
BLOCKQUOTE_STYLE = "margin:8px 0;padding:4px 12px;border-left:3px solid #bbb;color:#555"
HR_STYLE = "margin:24px 0;border:none;border-top:1px solid #ddd"
SOURCES_HEADER_STYLE = "margin:0 0 12px 0"
SOURCE_LABEL_STYLE = "margin:14px 0 4px 0"
CITATION_QUOTE_STYLE = "margin:0 0 0 8px;padding:4px 12px;border-left:3px solid #ccc;color:#555"
FOOTER_STYLE = "margin:28px 0 0 0;padding-top:12px;border-top:1px solid #eee;font-size:9.5pt;color:#888"

FOOTER_HTML = (
    "This is a public demo of Arke on UK case law (BAILII). "
    "Arke is private legal intelligence — it runs on your firm's server. "
    "Your documents never leave your network."
)


@dataclass(frozen=True)
class EmailConfig:
    tenant_id: str
    client_id: str
    client_secret: str
    mailbox: str

    @staticmethod
    def from_env() -> "EmailConfig":
        def req(key: str) -> str:
            value = os.environ.get(key, "")
            if not value:
                raise ValueError(f"email config: {key} is required")
            return value

        return EmailConfig(
            tenant_id=req("M365_TENANT_ID"),
            client_id=req("M365_CLIENT_ID"),
            client_secret=req("M365_CLIENT_SECRET"),
            mailbox=req("M365_MAILBOX"),
        )


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _send(call: Callable[[], httpx.Response], max_tries: int = 3) -> httpx.Response:
    """Invoke a Graph request with retries on 429 and 5xx, exponential backoff.

    401 is not retried — MSAL caches a valid token, so a rejection means a
    deeper auth problem that should surface, not be masked.
    """
    delay = 1.0
    for attempt in range(max_tries):
        r = call()
        last_attempt = attempt == max_tries - 1
        if r.status_code == 429 and not last_attempt:
            time.sleep(float(r.headers.get("Retry-After", delay)))
            delay *= 2
            continue
        if 500 <= r.status_code < 600 and not last_attempt:
            time.sleep(delay)
            delay *= 2
            continue
        return r
    raise RuntimeError("unreachable: _send always returns inside the loop")


def acquire_token(app: msal.ConfidentialClientApplication) -> str:
    result = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in result:
        raise RuntimeError(f"auth: {result.get('error_description', result)}")
    return result["access_token"]


def list_unread_ids(http: httpx.Client, token: str, mailbox: str) -> list[str]:
    url = f"{GRAPH}/users/{mailbox}/mailFolders('inbox')/messages"
    params = {
        "$filter": "isRead eq false",
        "$select": "id",
        "$orderby": "receivedDateTime",
        "$top": str(LIST_PAGE_SIZE),
    }
    r = _send(lambda: http.get(url, params=params, headers=_auth(token)))
    r.raise_for_status()
    return [m["id"] for m in r.json().get("value", [])]


def fetch_message(http: httpx.Client, token: str, mailbox: str, msg_id: str) -> dict:
    url = f"{GRAPH}/users/{mailbox}/messages/{msg_id}"
    params = {"$select": "id,subject,from,body"}
    headers = {**_auth(token), "Prefer": 'outlook.body-content-type="text"'}
    r = _send(lambda: http.get(url, params=params, headers=headers))
    r.raise_for_status()
    return r.json()


def _escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _inline(text: str) -> str:
    """Escape HTML then apply a minimal set of inline formats (**bold**)."""
    out = _escape(text)
    out = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", out)
    return out


def _is_block_start(line: str) -> bool:
    return (
        line.startswith("## ")
        or line.startswith("# ")
        or line.startswith("> ")
        or line.startswith("- ")
        or re.match(r"^\d+\.\s", line) is not None
    )


def _md_to_html(md: str) -> str:
    """Minimal markdown→HTML for email bodies.

    Supported: paragraphs, **bold**, ##/# headings, ordered and unordered lists,
    > blockquotes. Deliberately narrow — we control the LLM's output format, so
    we only render what we ask for and no exotic edge cases.
    """
    lines = md.split("\n")
    parts: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i].rstrip()
        if not line:
            i += 1
            continue

        if line.startswith("## "):
            parts.append(f'<h3 style="{H3_STYLE}">{_inline(line[3:])}</h3>')
            i += 1
            continue
        if line.startswith("# "):
            parts.append(f'<h3 style="{H3_STYLE}">{_inline(line[2:])}</h3>')
            i += 1
            continue

        if re.match(r"^\d+\.\s", line):
            ol_items: list[str] = []
            while i < len(lines) and re.match(r"^\d+\.\s", lines[i].rstrip()):
                item_text = re.sub(r"^\d+\.\s", "", lines[i].rstrip())
                ol_items.append(f'<li style="{LI_STYLE}">{_inline(item_text)}</li>')
                i += 1
            parts.append(f'<ol style="{LIST_STYLE}">' + "".join(ol_items) + "</ol>")
            continue

        if line.startswith("- "):
            ul_items: list[str] = []
            while i < len(lines) and lines[i].rstrip().startswith("- "):
                ul_items.append(f'<li style="{LI_STYLE}">{_inline(lines[i].rstrip()[2:])}</li>')
                i += 1
            parts.append(f'<ul style="{LIST_STYLE}">' + "".join(ul_items) + "</ul>")
            continue

        if line.startswith("> "):
            quote_lines: list[str] = []
            while i < len(lines) and lines[i].rstrip().startswith("> "):
                quote_lines.append(_inline(lines[i].rstrip()[2:]))
                i += 1
            quote_html = " ".join(quote_lines)
            parts.append(f'<blockquote style="{BLOCKQUOTE_STYLE}">{quote_html}</blockquote>')
            continue

        para_lines: list[str] = []
        while i < len(lines) and lines[i].rstrip() and not _is_block_start(lines[i].rstrip()):
            para_lines.append(_inline(lines[i].rstrip()))
            i += 1
        parts.append(f'<p style="{PARA_STYLE}">{" ".join(para_lines)}</p>')

    return "".join(parts)


def _build_html_reply(answer_md: str, citations: list[dict]) -> str:
    """Wrap LLM answer + Sources block into a self-styled HTML fragment."""
    answer_html = _md_to_html(answer_md)
    parts = [f'<div style="{EMAIL_STYLE}">', answer_html]

    if citations:
        parts.append(f'<hr style="{HR_STYLE}">')
        parts.append(f'<h3 style="{SOURCES_HEADER_STYLE}">Sources</h3>')
        for number, citation in enumerate(citations, 1):
            label = _escape(citation.get("source", "unknown"))
            raw_text = citation.get("text", "")
            quote_html = _escape(raw_text).replace("\n", "<br>")
            parts.append(
                f'<p style="{SOURCE_LABEL_STYLE}"><strong>[{number}]</strong> '
                f'<em>{label}</em></p>'
                f'<blockquote style="{CITATION_QUOTE_STYLE}">{quote_html}</blockquote>'
            )

    parts.append(f'<p style="{FOOTER_STYLE}">{FOOTER_HTML}</p>')
    parts.append("</div>")
    return "".join(parts)


def _mime_type(filename: str) -> str:
    ext = Path(filename).suffix.lower()
    return MIME_TYPES.get(ext, "application/octet-stream")


def _source_bytes(workspace_path: Path, doc_id: str) -> bytes | None:
    """Read a source file directly from the workspace sdb layout.

    sdb shards by the first two chars of the id; the email client reads the
    file rather than mounting sdb because the server owns the mount and a
    second mount from another process would race its atomic writes.
    """
    path = workspace_path / "data" / "sources" / doc_id[:2] / doc_id
    if not path.exists():
        return None
    return path.read_bytes()


def _build_attachments(citations: list[dict], workspace_path: Path) -> list[dict]:
    """Dedupe citations by doc_id, read each source file, enforce size budget."""
    attachments: list[dict] = []
    seen: set[str] = set()
    total = 0
    for citation in citations:
        doc_id = citation.get("doc_id")
        if not doc_id or doc_id in seen:
            continue
        seen.add(doc_id)

        data = _source_bytes(workspace_path, doc_id)
        if data is None:
            logger.info("attachment skipped: doc_id=%s reason=source-missing", doc_id)
            continue
        if len(data) > MAX_ATTACHMENT_BYTES:
            logger.info("attachment skipped: doc_id=%s reason=oversize bytes=%d", doc_id, len(data))
            continue
        if total + len(data) > TOTAL_ATTACHMENT_BUDGET:
            logger.info("attachment skipped: doc_id=%s reason=budget-exceeded", doc_id)
            continue
        total += len(data)

        filename = citation.get("filename") or citation.get("source") or doc_id
        attachments.append({
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": filename,
            "contentBytes": base64.b64encode(data).decode(),
            "contentType": _mime_type(filename),
        })
    return attachments


def reply_to_message(
    http: httpx.Client,
    token: str,
    mailbox: str,
    msg_id: str,
    html_body: str,
    attachments: list[dict],
) -> None:
    url = f"{GRAPH}/users/{mailbox}/messages/{msg_id}/reply"
    message: dict = {"body": {"contentType": "html", "content": html_body}}
    if attachments:
        message["attachments"] = attachments
    body = {"message": message}
    r = _send(lambda: http.post(url, json=body, headers=_auth(token)))
    r.raise_for_status()


def mark_as_read(http: httpx.Client, token: str, mailbox: str, msg_id: str) -> None:
    url = f"{GRAPH}/users/{mailbox}/messages/{msg_id}"
    body = {"isRead": True}
    r = _send(lambda: http.patch(url, json=body, headers=_auth(token)))
    r.raise_for_status()


def process_message(
    http: httpx.Client, token: str, mailbox: str, msg_id: str, workspace_path: Path
) -> None:
    from arke.server import mailbox as arke_mailbox

    msg = fetch_message(http, token, mailbox, msg_id)
    subject = msg.get("subject") or "(no subject)"
    sender = (msg.get("from") or {}).get("emailAddress", {}).get("address", "unknown")
    body_text = (msg.get("body") or {}).get("content", "").strip()
    logger.info("received: %s (from %s)", subject, sender)

    # Self-routed mail (a reply we sent that came back to the same mailbox)
    # must never trigger another reply — that is the infinite loop. Mark read
    # and bail. Graph's $filter cannot combine isRead with a nested sender
    # filter, so we handle this client-side.
    if sender.lower() == mailbox.lower():
        mark_as_read(http, token, mailbox, msg_id)
        return

    query = body_text or subject
    arke_msg_id = arke_mailbox.send({"cmd": "ask", "query": query}, workspace_path)
    response = arke_mailbox.receive(arke_msg_id, workspace_path)

    if response and response.get("ok"):
        answer_md = response.get("answer", "No answer available.")
        citations = response.get("citations", [])
    else:
        answer_md = "Arke could not process your request at this time."
        citations = []

    html_body = _build_html_reply(answer_md, citations)
    attachments = _build_attachments(citations, workspace_path)

    # Mark read before replying: a crash between the two calls leaves a silent
    # miss (user notices, resends) instead of a double reply (embarrassing).
    mark_as_read(http, token, mailbox, msg_id)
    reply_to_message(http, token, mailbox, msg_id, html_body, attachments)


def _install_term_handler() -> None:
    """Translate SIGTERM into KeyboardInterrupt so systemd/docker stop paths run
    the same finally block as Ctrl-C. Python's default SIGTERM kills the process
    mid-stride, skipping cleanup."""

    def handler(signum: int, frame: object) -> None:
        raise KeyboardInterrupt()

    signal.signal(signal.SIGTERM, handler)


def run(cfg: EmailConfig, workspace_path: Path) -> None:
    logger.info("email client starting (polling %ds), mailbox=%s", POLL_INTERVAL_SEC, cfg.mailbox)
    _install_term_handler()

    msal_app = msal.ConfidentialClientApplication(
        client_id=cfg.client_id,
        client_credential=cfg.client_secret,
        authority=f"https://login.microsoftonline.com/{cfg.tenant_id}",
    )

    with httpx.Client(timeout=30) as http:
        try:
            while True:
                token = acquire_token(msal_app)
                try:
                    ids = list_unread_ids(http, token, cfg.mailbox)
                except httpx.HTTPError as exc:
                    logger.error("list unread failed: %s", exc)
                    ids = []

                for msg_id in ids:
                    try:
                        process_message(http, token, cfg.mailbox, msg_id, workspace_path)
                    except httpx.HTTPError as exc:
                        logger.error("process %s failed: %s", msg_id, exc)

                time.sleep(POLL_INTERVAL_SEC)
        except KeyboardInterrupt:
            logger.info("shutting down")


def main() -> None:
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    workspace_name = os.environ.get("ARKE_WORKSPACE", "default")
    arke_root = Path(os.environ.get("ARKE_ROOT") or Path.home() / ".arke")
    workspace_path = arke_root / "workspaces" / workspace_name
    run(EmailConfig.from_env(), workspace_path)


if __name__ == "__main__":
    main()
