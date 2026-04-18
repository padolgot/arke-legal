"""Email client via Microsoft Graph webhooks.

Graph subscription on a shared mailbox; new mail → fetch → ask arke-server
→ reply. RAG lives behind a file-based inbox/outbox, not imported here.

Public ingress:
- demo: CLOUDFLARED_TUNNEL=<name> — we spawn `cloudflared tunnel run <name>`
  as a child. The named tunnel is created once via the Cloudflare CLI and
  routes a stable DNS name (e.g. mail.arke.legal) to localhost. See setup.md.
- prod: CLOUDFLARED_TUNNEL unset — Caddy terminates TLS on a public IP and
  M365_WEBHOOK_URL points at that stable hostname directly.

The webhook server runs on a daemon thread because Graph validates the URL
synchronously during subscription creation — it holds the POST open until
our endpoint echoes the validationToken.
"""

import json
import logging
import os
import secrets
import signal
import subprocess
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
import msal

logger = logging.getLogger(__name__)

GRAPH = "https://graph.microsoft.com/v1.0"
SCOPE = ["https://graph.microsoft.com/.default"]
SUBSCRIPTION_TTL_MIN = 60
RENEWAL_INTERVAL_SEC = 50 * 60
CLOUDFLARED_STARTUP_SEC = 5


@dataclass(frozen=True)
class EmailConfig:
    tenant_id: str
    client_id: str
    client_secret: str
    mailbox: str
    webhook_url: str
    webhook_port: int = 8080
    cloudflared_tunnel: str = ""  # named tunnel to spawn; empty = external ingress (prod)

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
            webhook_url=req("M365_WEBHOOK_URL"),
            webhook_port=int(os.environ.get("M365_WEBHOOK_PORT", "8080")),
            cloudflared_tunnel=os.environ.get("CLOUDFLARED_TUNNEL", ""),
        )


class _SeenIds:
    """Bounded FIFO of recently-processed message IDs. Graph occasionally
    redelivers a notification (e.g. on our 5xx or a transient network error);
    without dedup we would reply twice to the same email. Safe without a lock:
    HTTPServer serves one request at a time."""

    def __init__(self, max_size: int = 512) -> None:
        self._order: deque[str] = deque(maxlen=max_size)
        self._set: set[str] = set()

    def check_and_add(self, item: str) -> bool:
        """Return True if already seen; otherwise record and return False."""
        if item in self._set:
            return True
        if len(self._order) == self._order.maxlen:
            self._set.discard(self._order[0])
        self._order.append(item)
        self._set.add(item)
        return False


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


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
    return r


def acquire_token(app: msal.ConfidentialClientApplication) -> str:
    result = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in result:
        raise RuntimeError(f"auth: {result.get('error_description', result)}")
    return result["access_token"]


def create_subscription(http: httpx.Client, token: str, cfg: EmailConfig, client_state: str) -> str:
    # Scope to the Inbox folder so outbound replies (saved in Sent Items) do
    # not trigger notifications. Without this the bot receives its own sent
    # messages and can recursively reply to itself.
    expires = datetime.now(timezone.utc) + timedelta(minutes=SUBSCRIPTION_TTL_MIN)
    body = {
        "changeType": "created",
        "notificationUrl": cfg.webhook_url,
        "resource": f"/users/{cfg.mailbox}/mailFolders('inbox')/messages",
        "expirationDateTime": _iso_utc(expires),
        "clientState": client_state,
    }
    r = _send(lambda: http.post(f"{GRAPH}/subscriptions", json=body, headers=_auth(token)))
    r.raise_for_status()
    return r.json()["id"]


def renew_subscription(http: httpx.Client, token: str, sub_id: str) -> None:
    expires = datetime.now(timezone.utc) + timedelta(minutes=SUBSCRIPTION_TTL_MIN)
    body = {"expirationDateTime": _iso_utc(expires)}
    url = f"{GRAPH}/subscriptions/{sub_id}"
    r = _send(lambda: http.patch(url, json=body, headers=_auth(token)))
    r.raise_for_status()


def delete_subscription(http: httpx.Client, token: str, sub_id: str) -> None:
    url = f"{GRAPH}/subscriptions/{sub_id}"
    r = _send(lambda: http.delete(url, headers=_auth(token)))
    if r.status_code >= 400:
        logger.warning("delete subscription %s: %d", sub_id, r.status_code)


def fetch_message(http: httpx.Client, token: str, mailbox: str, msg_id: str) -> dict:
    url = f"{GRAPH}/users/{mailbox}/messages/{msg_id}"
    params = {"$select": "id,subject,from,body"}
    headers = {**_auth(token), "Prefer": 'outlook.body-content-type="text"'}
    r = _send(lambda: http.get(url, params=params, headers=headers))
    r.raise_for_status()
    return r.json()


def reply_to_message(http: httpx.Client, token: str, mailbox: str, msg_id: str, comment: str) -> None:
    url = f"{GRAPH}/users/{mailbox}/messages/{msg_id}/reply"
    body = {"comment": comment}
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

    query = body_text or subject
    arke_msg_id = arke_mailbox.send({"cmd": "ask", "query": query}, workspace_path)
    response = arke_mailbox.receive(arke_msg_id, workspace_path)

    if response and response.get("ok"):
        answer = response.get("answer", "No answer available.")
    else:
        answer = "Arke could not process your request at this time."

    reply_to_message(http, token, mailbox, msg_id, answer)
    mark_as_read(http, token, mailbox, msg_id)


def _build_handler(
    http: httpx.Client,
    msal_app: msal.ConfidentialClientApplication,
    cfg: EmailConfig,
    client_state: str,
    workspace_path: Path,
) -> type[BaseHTTPRequestHandler]:
    seen = _SeenIds()

    class WebhookHandler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: object) -> None:
            logger.debug("http: " + fmt, *args)

        def do_POST(self) -> None:
            query = parse_qs(urlparse(self.path).query)
            if "validationToken" in query:
                token_value = query["validationToken"][0]
                payload = token_value.encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                logger.info("subscription validated")
                return

            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            self.send_response(202)
            self.end_headers()

            try:
                body = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("invalid json body")
                return

            token = acquire_token(msal_app)
            for notif in body.get("value", []):
                if notif.get("clientState") != client_state:
                    logger.warning("clientState mismatch, dropping")
                    continue
                msg_id = (notif.get("resourceData") or {}).get("id")
                if not msg_id:
                    continue
                if seen.check_and_add(msg_id):
                    logger.info("duplicate notification for %s, skipping", msg_id)
                    continue
                try:
                    process_message(http, token, cfg.mailbox, msg_id, workspace_path)
                except httpx.HTTPError as exc:
                    logger.error("process %s failed: %s", msg_id, exc)

    return WebhookHandler


def _spawn_cloudflared(tunnel_name: str) -> subprocess.Popen:
    """Spawn `cloudflared tunnel run <name>` and wait for it to dial out.

    The named tunnel is provisioned once via the Cloudflare CLI (see setup.md);
    credentials live in ~/.cloudflared/. We sleep 5s after starting so Graph's
    synchronous webhook validation has a live ingress to hit.
    """
    logger.info("starting cloudflared tunnel '%s'", tunnel_name)
    try:
        proc = subprocess.Popen(["cloudflared", "tunnel", "run", tunnel_name])
    except FileNotFoundError as exc:
        raise RuntimeError("cloudflared not found on PATH — install it or unset CLOUDFLARED_TUNNEL") from exc

    time.sleep(CLOUDFLARED_STARTUP_SEC)
    if proc.poll() is not None:
        raise RuntimeError(f"cloudflared exited early (code {proc.returncode})")
    return proc


def _terminate_cloudflared(proc: subprocess.Popen) -> None:
    logger.info("stopping cloudflared")
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2)


def _install_term_handler() -> None:
    """Translate SIGTERM into KeyboardInterrupt so systemd/docker stop paths run
    the same finally block as Ctrl-C, deleting the Graph subscription cleanly.
    Python's default SIGTERM kills the process mid-stride, skipping cleanup."""

    def handler(signum: int, frame: object) -> None:
        raise KeyboardInterrupt()

    signal.signal(signal.SIGTERM, handler)


def run(cfg: EmailConfig, workspace_path: Path) -> None:
    logger.info("email client starting, mailbox=%s, webhook=%s", cfg.mailbox, cfg.webhook_url)
    _install_term_handler()

    msal_app = msal.ConfidentialClientApplication(
        client_id=cfg.client_id,
        client_credential=cfg.client_secret,
        authority=f"https://login.microsoftonline.com/{cfg.tenant_id}",
    )

    with httpx.Client(timeout=30) as http:
        client_state = secrets.token_urlsafe(32)
        handler_cls = _build_handler(http, msal_app, cfg, client_state, workspace_path)
        server = HTTPServer(("127.0.0.1", cfg.webhook_port), handler_cls)

        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        logger.info("listening on 127.0.0.1:%d", cfg.webhook_port)

        tunnel: subprocess.Popen | None = None
        sub_id: str | None = None
        try:
            if cfg.cloudflared_tunnel:
                tunnel = _spawn_cloudflared(cfg.cloudflared_tunnel)

            token = acquire_token(msal_app)
            sub_id = create_subscription(http, token, cfg, client_state)
            logger.info("created subscription %s", sub_id)

            while True:
                time.sleep(RENEWAL_INTERVAL_SEC)
                renew_subscription(http, acquire_token(msal_app), sub_id)
                logger.info("renewed subscription")
        except KeyboardInterrupt:
            logger.info("shutting down")
        finally:
            if sub_id is not None:
                try:
                    delete_subscription(http, acquire_token(msal_app), sub_id)
                except Exception:
                    logger.exception("cleanup failed")
            if tunnel is not None:
                _terminate_cloudflared(tunnel)
            server.shutdown()


def main() -> None:
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    workspace_name = os.environ.get("ARKE_WORKSPACE", "default")
    workspace_path = Path.home() / ".arke" / "workspaces" / workspace_name
    run(EmailConfig.from_env(), workspace_path)


if __name__ == "__main__":
    main()
