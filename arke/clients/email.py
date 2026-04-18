"""Email client via Microsoft Graph polling.

Every POLL_INTERVAL seconds, list unread messages in the inbox, fetch each,
forward to arke-server, reply, mark read. No webhooks, no public ingress —
purely outbound HTTPS to Graph.

isRead=false is the source of truth for "needs answering". We mark-as-read
before replying so a crash between steps leaves a silent miss, never a
double reply.
"""

import logging
import os
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
    return r


def acquire_token(app: msal.ConfidentialClientApplication) -> str:
    result = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in result:
        raise RuntimeError(f"auth: {result.get('error_description', result)}")
    return result["access_token"]


def list_unread_ids(http: httpx.Client, token: str, mailbox: str) -> list[str]:
    # Skip messages sent by the mailbox itself — a reply from arke-mail lands
    # back in the same Inbox (sender == recipient for self-routed mails) and
    # without this filter we would reply to our own replies forever.
    url = f"{GRAPH}/users/{mailbox}/mailFolders('inbox')/messages"
    params = {
        "$filter": f"isRead eq false and from/emailAddress/address ne '{mailbox}'",
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
        citations = response.get("citations", [])
        if citations:
            lines = [f"[{i+1}] {c['source']}" for i, c in enumerate(citations)]
            answer += "\n\n---\nSources:\n" + "\n".join(lines)
    else:
        answer = "Arke could not process your request at this time."

    # Mark read before replying: a crash between the two calls leaves a silent
    # miss (user notices, resends) instead of a double reply (embarrassing).
    mark_as_read(http, token, mailbox, msg_id)
    reply_to_message(http, token, mailbox, msg_id, answer)


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
    workspace_name = os.environ.get("ARKE_WORKSPACE", "default")
    arke_root = Path(os.environ.get("ARKE_ROOT") or Path.home() / ".arke")
    workspace_path = arke_root / "workspaces" / workspace_name
    run(EmailConfig.from_env(), workspace_path)


if __name__ == "__main__":
    main()
