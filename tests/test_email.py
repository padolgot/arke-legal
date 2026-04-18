import httpx
import pytest

from arke.clients.email import EmailConfig, _SeenIds, _send, process_message


_ENV_KEYS = [
    "M365_TENANT_ID",
    "M365_CLIENT_ID",
    "M365_CLIENT_SECRET",
    "M365_MAILBOX",
    "M365_WEBHOOK_URL",
    "M365_WEBHOOK_PORT",
]


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    return monkeypatch


def test_from_env_requires_tenant_id(clean_env: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValueError, match="M365_TENANT_ID"):
        EmailConfig.from_env()


def test_from_env_requires_webhook_url(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("M365_TENANT_ID", "tid")
    clean_env.setenv("M365_CLIENT_ID", "cid")
    clean_env.setenv("M365_CLIENT_SECRET", "secret")
    clean_env.setenv("M365_MAILBOX", "ask@arke.legal")
    with pytest.raises(ValueError, match="M365_WEBHOOK_URL"):
        EmailConfig.from_env()


def test_from_env_complete(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("M365_TENANT_ID", "tid")
    clean_env.setenv("M365_CLIENT_ID", "cid")
    clean_env.setenv("M365_CLIENT_SECRET", "secret")
    clean_env.setenv("M365_MAILBOX", "ask@arke.legal")
    clean_env.setenv("M365_WEBHOOK_URL", "https://x.trycloudflare.com")

    cfg = EmailConfig.from_env()

    assert cfg.tenant_id == "tid"
    assert cfg.mailbox == "ask@arke.legal"
    assert cfg.webhook_port == 8080


def test_process_message_echoes_and_marks_read(
    tmp_path: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("arke.server.mailbox.send", lambda req, ws: "mid1")
    monkeypatch.setattr(
        "arke.server.mailbox.receive",
        lambda mid, ws: {"ok": True, "answer": "Here is the answer."},
    )

    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "id": "msg1",
                    "subject": "hello",
                    "from": {"emailAddress": {"address": "client@firm.com"}},
                    "body": {"content": "What is the case law on X?", "contentType": "text"},
                },
            )
        return httpx.Response(202)

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as http:
        process_message(http, "fake-token", "ask@arke.legal", "msg1", tmp_path)

    methods = [m for m, _ in calls]
    assert methods.count("GET") == 1
    assert methods.count("POST") == 1
    assert methods.count("PATCH") == 1
    assert any(p.endswith("/reply") for m, p in calls if m == "POST")


def test_process_message_handles_missing_fields(
    tmp_path: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("arke.server.mailbox.send", lambda req, ws: "mid1")
    monkeypatch.setattr(
        "arke.server.mailbox.receive",
        lambda mid, ws: {"ok": True, "answer": "No relevant documents found."},
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json={"id": "msg1"})
        return httpx.Response(202)

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as http:
        process_message(http, "fake-token", "ask@arke.legal", "msg1", tmp_path)


def test_seen_ids_first_sighting_records() -> None:
    seen = _SeenIds()
    assert seen.check_and_add("a") is False
    assert seen.check_and_add("a") is True


def test_seen_ids_evicts_oldest_when_full() -> None:
    seen = _SeenIds(max_size=2)
    seen.check_and_add("a")
    seen.check_and_add("b")
    seen.check_and_add("c")  # evicts "a"
    assert seen.check_and_add("a") is False  # gone, treated as new
    assert seen.check_and_add("c") is True   # still within window


def test_send_returns_immediately_on_success() -> None:
    calls = 0

    def call() -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200)

    r = _send(call)
    assert r.status_code == 200
    assert calls == 1


def test_send_retries_on_5xx(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("arke.clients.email.time.sleep", lambda _: None)
    statuses = iter([500, 503, 200])

    def call() -> httpx.Response:
        return httpx.Response(next(statuses))

    r = _send(call, max_tries=3)
    assert r.status_code == 200


def test_send_gives_up_after_max_tries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("arke.clients.email.time.sleep", lambda _: None)

    def call() -> httpx.Response:
        return httpx.Response(500)

    r = _send(call, max_tries=3)
    assert r.status_code == 500
