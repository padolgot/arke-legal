"""Email client via SendGrid — Inbound Parse webhook + SMTP relay.

Inbound: SendGrid POSTs raw RFC 822 MIME to /inbound (multipart/form-data).
We parse the email, forward to arke-server, wait for response, reply via SMTP.

Outbound: smtplib over TLS to smtp.sendgrid.net:587, login "apikey" + API key.

Threading: HTTP server spawns a thread per request so a long stress-test
(1-3 min) doesn't block concurrent webhooks. Arke's mailbox is file-based
with UUID keys — no shared state between threads.
"""

import email
import email.policy
import email.utils
import logging
import os
import re
import signal
import smtplib
import threading
from dataclasses import dataclass
from email.message import EmailMessage
from email.parser import BytesParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

logger = logging.getLogger(__name__)

SMTP_HOST = "smtp.sendgrid.net"
SMTP_PORT = 587
SMTP_USER = "apikey"
WEBHOOK_PORT = 8080

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
    api_key: str
    mailbox: str
    workspace_path: Path

    @staticmethod
    def from_env() -> "EmailConfig":
        def req(key: str) -> str:
            value = os.environ.get(key, "")
            if not value:
                raise ValueError(f"email config: {key} is required")
            return value

        arke_root = Path(os.environ.get("ARKE_ROOT") or Path.home() / ".arke")
        workspace = os.environ.get("ARKE_WORKSPACE", "default")
        return EmailConfig(
            api_key=req("SENDGRID_API_KEY"),
            mailbox=req("ARKE_MAILBOX"),
            workspace_path=arke_root / "workspaces" / workspace,
        )


# --- multipart + email parsing ------------------------------------------------

def _parse_multipart(content_type: str, body: bytes) -> dict[str, bytes]:
    """Parse multipart/form-data body → {field name: raw bytes}."""
    header = f"Content-Type: {content_type}\n\n".encode()
    msg = BytesParser(policy=email.policy.default).parsebytes(header + body)
    fields: dict[str, bytes] = {}
    for part in msg.iter_parts():
        disposition = part.get("Content-Disposition", "")
        name = None
        for item in disposition.split(";"):
            item = item.strip()
            if item.startswith("name="):
                name = item[5:].strip().strip('"')
                break
        if name:
            payload = part.get_payload(decode=True)
            if payload is not None:
                fields[name] = payload
    return fields


def _parse_rfc822(raw: bytes) -> tuple[str, str, str]:
    """Extract (sender_addr, subject, plain_body) from raw RFC 822."""
    msg = BytesParser(policy=email.policy.default).parsebytes(raw)
    sender_raw = msg.get("From", "")
    sender_addr = email.utils.parseaddr(sender_raw)[1]
    subject = msg.get("Subject", "(no subject)")

    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                body = part.get_content()
                break
        if not body:
            for part in msg.walk():
                if part.get_content_type() == "text/html":
                    html = part.get_content()
                    body = re.sub(r"<[^>]+>", "", html)
                    break
    else:
        body = msg.get_content()

    return sender_addr, subject, body.strip()


# --- rendering ----------------------------------------------------------------

def _escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _inline(text: str) -> str:
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


# --- SMTP ---------------------------------------------------------------------

def _send_reply(cfg: EmailConfig, to: str, subject: str, html: str) -> None:
    msg = EmailMessage()
    msg["From"] = cfg.mailbox
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content("Please view this message in an HTML-capable email client.")
    msg.add_alternative(html, subtype="html")

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
        s.starttls()
        s.login(SMTP_USER, cfg.api_key)
        s.send_message(msg)


# --- processing pipeline ------------------------------------------------------

def _process_inbound(cfg: EmailConfig, raw_mime: bytes) -> None:
    from arke.server import mailbox as arke_mailbox

    try:
        sender, subject, body_text = _parse_rfc822(raw_mime)
    except Exception as e:
        logger.exception("rfc822 parse failed: %s", e)
        return

    logger.info("received: %s (from %s)", subject, sender)

    if sender.lower() == cfg.mailbox.lower():
        logger.info("self-loop, dropped")
        return

    try:
        query = body_text or subject
        msg_id = arke_mailbox.send({"cmd": "stress", "argument": query}, cfg.workspace_path)
        response = arke_mailbox.receive(msg_id, cfg.workspace_path)

        if response and response.get("ok"):
            answer_md = response.get("answer", "No answer available.")
            citations = response.get("citations", [])
        else:
            answer_md = "Arke could not process your request at this time."
            citations = []

        html_body = _build_html_reply(answer_md, citations)
        reply_subject = subject if subject.lower().startswith("re:") else f"Re: {subject}"
        _send_reply(cfg, to=sender, subject=reply_subject, html=html_body)
        logger.info("reply sent to %s", sender)
    except Exception as e:
        logger.exception("processing failed for %s: %s", sender, e)


# --- HTTP server --------------------------------------------------------------

def _make_handler(cfg: EmailConfig):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            if self.path != "/inbound":
                self.send_response(404)
                self.end_headers()
                return
            raw: bytes | None = None
            try:
                content_type = self.headers.get("Content-Type", "")
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length)
                fields = _parse_multipart(content_type, body)
                raw = fields.get("email")
            except Exception as e:
                logger.exception("inbound parse error: %s", e)

            # ACK fast so SendGrid does not retry (default webhook timeout ~30s,
            # stress-test takes 1-3 min). Processing runs in a background thread.
            self.send_response(200)
            self.end_headers()

            if raw:
                t = threading.Thread(target=_process_inbound, args=(cfg, raw), daemon=True)
                t.start()
            else:
                logger.warning("inbound: no 'email' field in payload")

        def do_GET(self):
            if self.path == "/health":
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"ok")
                return
            self.send_response(404)
            self.end_headers()

        def log_message(self, format, *args):
            logger.info("%s %s", self.client_address[0], format % args)

    return Handler


def _install_term_handler() -> None:
    def handler(signum, frame):
        raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, handler)


def run(cfg: EmailConfig) -> None:
    logger.info("sendgrid client starting on :%d, mailbox=%s", WEBHOOK_PORT, cfg.mailbox)
    _install_term_handler()
    handler_cls = _make_handler(cfg)
    server = ThreadingHTTPServer(("127.0.0.1", WEBHOOK_PORT), handler_cls)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("shutting down")
        server.shutdown()


def main() -> None:
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(EmailConfig.from_env())


if __name__ == "__main__":
    main()
