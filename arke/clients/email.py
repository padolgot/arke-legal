"""Email client via SendGrid — Inbound Parse webhook + SMTP relay.

Inbound: SendGrid POSTs raw RFC 822 MIME to /inbound (multipart/form-data).
We parse the email, forward to arke-server, wait for response, reply via SMTP.

Outbound: smtplib over TLS to smtp.sendgrid.net:587, login "apikey" + API key.

Threading: HTTP server spawns a thread per request so a long stress-test
(1-3 min) doesn't block concurrent webhooks. Arke's mailbox is file-based
with UUID keys — no shared state between threads.
"""

import datetime
import email
import email.policy
import email.utils
import json
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

EMAIL_STYLE = "font-family:Georgia,serif;font-size:12pt;line-height:1.3;text-align:justify"
H3_STYLE = "margin:18px 0 8px 0;text-align:left"
PARA_STYLE = "margin:6px 0"
LIST_STYLE = "margin:6px 0;padding-left:24px"
LI_STYLE = "margin-bottom:4px"
BLOCKQUOTE_STYLE = "margin:6px 0;padding:4px 12px;border-left:3px solid #bbb"
SOURCE_LINE_STYLE = "margin:2px 0 14px 0;font-size:10pt;color:#888;font-style:italic;text-align:left"
FOOTER_STYLE = "margin:28px 0 0 0;padding-top:12px;border-top:1px solid #eee;font-size:9.5pt;color:#888"

FOOTER_HTML = (
    'Public demo on UK &amp; EU competition law<br>'
    'Source code: <a href="https://github.com/padolgot/arke" style="color:#888">github.com/padolgot/arke</a><br>'
    'Built by: <a href="https://linkedin.com/in/padolgot" style="color:#888">linkedin.com/in/padolgot</a>'
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

        from arke.server import workspace
        ws_name = os.environ.get("ARKE_WORKSPACE", "default")
        return EmailConfig(
            api_key=req("SENDGRID_API_KEY"),
            mailbox=req("ARKE_MAILBOX"),
            workspace_path=workspace.path_for(ws_name),
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


def _parse_rfc822(raw: bytes) -> tuple[str, str, str, str, str]:
    """Extract (sender, subject, body, message_id, references) from raw RFC 822."""
    msg = BytesParser(policy=email.policy.default).parsebytes(raw)
    sender_raw = msg.get("From", "")
    sender_addr = email.utils.parseaddr(sender_raw)[1]
    subject = msg.get("Subject", "(no subject)")
    message_id = msg.get("Message-ID", "").strip()
    references = msg.get("References", "").strip()

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

    return sender_addr, subject, body.strip(), message_id, references


# --- rendering ----------------------------------------------------------------

def _escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _inline(text: str) -> str:
    out = _escape(text)
    out = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", out)
    out = re.sub(r"\*([^*]+)\*", r"<em>\1</em>", out)
    return out


def _is_block_start(line: str) -> bool:
    return (
        line.startswith("## ")
        or line.startswith("# ")
        or line.startswith("> ")
        or line.startswith("- ")
        or line.startswith("— ")
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

        if line.startswith("— "):
            parts.append(f'<p style="{SOURCE_LINE_STYLE}">{_inline(line)}</p>')
            i += 1
            continue

        para_lines: list[str] = []
        while i < len(lines) and lines[i].rstrip() and not _is_block_start(lines[i].rstrip()):
            para_lines.append(_inline(lines[i].rstrip()))
            i += 1
        parts.append(f'<p style="{PARA_STYLE}">{" ".join(para_lines)}</p>')

    return "".join(parts)


def _build_html_reply(answer_md: str, citations: list[dict]) -> str:
    answer_html = _md_to_html(answer_md)
    return "".join([
        f'<div style="{EMAIL_STYLE}">',
        answer_html,
        f'<p style="{FOOTER_STYLE}">{FOOTER_HTML}</p>',
        "</div>",
    ])


# --- SMTP ---------------------------------------------------------------------

def _send_reply(
    cfg: EmailConfig,
    to: str,
    subject: str,
    html: str,
    in_reply_to: str = "",
    references: str = "",
) -> str:
    """Send reply via SendGrid SMTP. Returns the RFC-822 Message-ID for log correlation."""
    msg = EmailMessage()
    message_id = email.utils.make_msgid(domain="arke.legal")
    msg["Message-ID"] = message_id
    msg["From"] = cfg.mailbox
    msg["Reply-To"] = cfg.mailbox
    msg["To"] = to
    msg["Subject"] = subject
    msg["List-Unsubscribe"] = f"<mailto:{cfg.mailbox}?subject=unsubscribe>"
    if in_reply_to:
        msg["In-Reply-To"] = in_reply_to
        # References chain: prior chain + original message-id
        chain = f"{references} {in_reply_to}".strip() if references else in_reply_to
        msg["References"] = chain
    msg.set_content("Please view this message in an HTML-capable email client.")
    msg.add_alternative(html, subtype="html")

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
        ehlo_code, _ = s.ehlo()
        s.starttls()
        s.ehlo()
        login_code, _ = s.login(SMTP_USER, cfg.api_key)
        refused = s.send_message(msg)
    logger.info(
        "smtp sent msg-id=%s to=%s bytes=%d ehlo=%d login=%d refused=%s",
        message_id, to, len(msg.as_bytes()), ehlo_code, login_code, refused or {},
    )
    return message_id


# --- processing pipeline ------------------------------------------------------

def _log_transcript(workspace_path: Path, sender: str, subject: str, query: str, answer: str) -> None:
    """Append one JSON record per chat to chat.jsonl. Tail with jq for live view."""
    path = workspace_path / "chat.jsonl"
    record = {
        "ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "from": sender,
        "subject": subject,
        "query": query.strip(),
        "answer": answer.strip(),
    }
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def _process_inbound(cfg: EmailConfig, raw_mime: bytes) -> None:
    from arke.server import mailbox as arke_mailbox

    try:
        sender, subject, body_text, orig_msg_id, orig_refs = _parse_rfc822(raw_mime)
    except Exception as e:
        logger.exception("rfc822 parse failed: %s", e)
        return

    logger.info("inbound: from=%s subj=%r body=%d chars orig-msg-id=%s",
                sender, subject, len(body_text), orig_msg_id or "(none)")

    if sender.lower() == cfg.mailbox.lower():
        logger.info("self-loop, dropped")
        return

    try:
        query = body_text or subject
        msg_id = arke_mailbox.send({"cmd": "stress", "argument": query}, cfg.workspace_path)
        logger.info("dispatched: mailbox-id=%s", msg_id)
        response = arke_mailbox.receive(msg_id, cfg.workspace_path)

        if response and response.get("ok"):
            answer_md = response.get("answer", "No answer available.")
            citations = response.get("citations", [])
        else:
            answer_md = "Arke could not process your request at this time."
            citations = []

        _log_transcript(cfg.workspace_path, sender, subject, query, answer_md)

        html_body = _build_html_reply(answer_md, citations)
        reply_subject = subject if subject.lower().startswith("re:") else f"Re: {subject}"
        message_id = _send_reply(
            cfg, to=sender, subject=reply_subject, html=html_body,
            in_reply_to=orig_msg_id, references=orig_refs,
        )
        logger.info("reply sent msg-id=%s to=%s subj=%r", message_id, sender, reply_subject)
    except Exception as e:
        logger.exception("processing failed for %s: %s", sender, e)


# --- HTTP server --------------------------------------------------------------

class _Handler(BaseHTTPRequestHandler):
    # Set by run() before serve_forever — the only way to plumb config into
    # BaseHTTPRequestHandler without a closure factory.
    cfg: EmailConfig

    def do_POST(self):
        if self.path == "/inbound":
            self._handle_inbound()
        elif self.path == "/events":
            self._handle_events()
        else:
            self.send_response(404)
            self.end_headers()

    def _read_inbound_email(self) -> bytes | None:
        try:
            content_type = self.headers.get("Content-Type", "")
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            fields = _parse_multipart(content_type, body)
            return fields.get("email")
        except Exception as e:
            logger.exception("inbound parse error: %s", e)
            return None

    def _handle_inbound(self) -> None:
        raw = self._read_inbound_email()
        # ACK fast so SendGrid does not retry (default webhook timeout ~30s,
        # stress-test takes 1-3 min). Processing runs in a background thread.
        self.send_response(200)
        self.end_headers()

        if raw:
            t = threading.Thread(target=_process_inbound, args=(self.cfg, raw), daemon=True)
            t.start()
        else:
            logger.warning("inbound: no 'email' field in payload")

    def _handle_events(self) -> None:
        """SendGrid Event Webhook — POSTs JSON array of delivery events.
        Enable in SG UI: Mail Settings → Event Webhook → POST URL = https://mail.arke.legal/events
        """
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        # ACK immediately — SendGrid retries on non-2xx
        self.send_response(200)
        self.end_headers()
        try:
            events = json.loads(body)
            if not isinstance(events, list):
                events = [events]
        except Exception as e:
            logger.warning("sg-event: bad JSON (%d bytes): %s", len(body), e)
            return
        for ev in events:
            logger.info(
                "sg-event: %s to=%s sg-id=%s smtp-id=%s reason=%r status=%s response=%r",
                ev.get("event", "?"),
                ev.get("email", "?"),
                ev.get("sg_message_id", "?"),
                ev.get("smtp-id", "?"),
                ev.get("reason", ""),
                ev.get("status", ""),
                ev.get("response", ""),
            )

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


def _install_term_handler() -> None:
    def handler(signum, frame):
        raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, handler)


def run(cfg: EmailConfig) -> None:
    logger.info("sendgrid client starting on :%d, mailbox=%s", WEBHOOK_PORT, cfg.mailbox)
    _install_term_handler()
    _Handler.cfg = cfg
    server = ThreadingHTTPServer(("127.0.0.1", WEBHOOK_PORT), _Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("shutting down")
        server.shutdown()


def main() -> None:
    from dotenv import find_dotenv, load_dotenv

    load_dotenv(find_dotenv())
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(EmailConfig.from_env())


if __name__ == "__main__":
    main()
