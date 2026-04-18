import base64
import json

import httpx
import pytest

from arke.clients.email import (
    EmailConfig,
    _build_attachments,
    _build_html_reply,
    _md_to_html,
    _send,
    list_unread_ids,
    process_message,
)


_ENV_KEYS = [
    "M365_TENANT_ID",
    "M365_CLIENT_ID",
    "M365_CLIENT_SECRET",
    "M365_MAILBOX",
]


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    return monkeypatch


def test_from_env_requires_tenant_id(clean_env: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValueError, match="M365_TENANT_ID"):
        EmailConfig.from_env()


def test_from_env_requires_mailbox(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("M365_TENANT_ID", "tid")
    clean_env.setenv("M365_CLIENT_ID", "cid")
    clean_env.setenv("M365_CLIENT_SECRET", "secret")
    with pytest.raises(ValueError, match="M365_MAILBOX"):
        EmailConfig.from_env()


def test_from_env_complete(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("M365_TENANT_ID", "tid")
    clean_env.setenv("M365_CLIENT_ID", "cid")
    clean_env.setenv("M365_CLIENT_SECRET", "secret")
    clean_env.setenv("M365_MAILBOX", "ask@arke.legal")

    cfg = EmailConfig.from_env()

    assert cfg.tenant_id == "tid"
    assert cfg.mailbox == "ask@arke.legal"


def test_list_unread_ids_returns_ids_in_order() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert "isRead+eq+false" in str(request.url) or "isRead%20eq%20false" in str(request.url)
        return httpx.Response(200, json={"value": [{"id": "m1"}, {"id": "m2"}, {"id": "m3"}]})

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as http:
        ids = list_unread_ids(http, "token", "ask@arke.legal")
    assert ids == ["m1", "m2", "m3"]


def test_process_message_marks_read_before_reply(
    tmp_path: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("arke.server.mailbox.send", lambda req, ws: "mid1")
    monkeypatch.setattr(
        "arke.server.mailbox.receive",
        lambda mid, ws: {"ok": True, "answer": "Here is the answer.", "citations": []},
    )

    calls: list[tuple[str, str, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        payload: dict = {}
        if request.method in {"POST", "PATCH"}:
            payload = json.loads(request.content.decode() or "{}")
        calls.append((request.method, request.url.path, payload))
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

    methods = [m for m, _, _ in calls]
    assert methods == ["GET", "PATCH", "POST"]
    assert calls[-1][1].endswith("/reply")

    reply_body = calls[-1][2]
    assert reply_body["message"]["body"]["contentType"] == "html"
    assert "Here is the answer." in reply_body["message"]["body"]["content"]


def test_process_message_handles_missing_fields(
    tmp_path: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("arke.server.mailbox.send", lambda req, ws: "mid1")
    monkeypatch.setattr(
        "arke.server.mailbox.receive",
        lambda mid, ws: {"ok": True, "answer": "No relevant documents found.", "citations": []},
    )

    reply_payloads: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json={"id": "msg1"})
        if request.method == "POST":
            reply_payloads.append(json.loads(request.content.decode() or "{}"))
        return httpx.Response(202)

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as http:
        process_message(http, "fake-token", "ask@arke.legal", "msg1", tmp_path)

    assert len(reply_payloads) == 1
    body = reply_payloads[0]["message"]["body"]
    assert body["contentType"] == "html"
    assert "No relevant documents found." in body["content"]


def test_md_to_html_basic_structures() -> None:
    md = (
        "Short conclusion.\n"
        "\n"
        "1. First point [1].\n"
        "2. Second point [2].\n"
        "\n"
        "**Strong** and plain.\n"
    )
    html = _md_to_html(md)
    assert "Short conclusion." in html
    assert "<p" in html
    assert "<ol" in html and "<li" in html
    assert "First point [1]." in html
    assert "<strong>Strong</strong>" in html


def test_md_to_html_escapes_angle_brackets() -> None:
    html = _md_to_html("Consider <script>alert(1)</script>.")
    assert "&lt;script&gt;" in html
    assert "<script>" not in html


def test_build_html_reply_includes_sources_block() -> None:
    citations = [
        {"doc_id": "abc123", "source": "caparo_v_dickman_1990",
         "filename": "caparo_v_dickman_1990.pdf", "text": "The three-stage test..."},
        {"doc_id": "abc123", "source": "caparo_v_dickman_1990",
         "filename": "caparo_v_dickman_1990.pdf", "text": "Foreseeability, proximity..."},
    ]
    html = _build_html_reply("Conclusion [1][2].", citations)
    assert "Sources" in html
    assert "[1]" in html and "[2]" in html
    assert "The three-stage test..." in html
    assert "caparo_v_dickman_1990" in html


def test_build_html_reply_skips_sources_when_empty() -> None:
    html = _build_html_reply("No results.", [])
    assert "Sources" not in html
    assert "No results." in html


def test_build_attachments_dedupes_by_doc_id(tmp_path) -> None:
    doc_id = "ab" + "c" * 30
    shard = tmp_path / "data" / "sources" / doc_id[:2]
    shard.mkdir(parents=True)
    (shard / doc_id).write_bytes(b"%PDF-1.4 fake pdf bytes")

    citations = [
        {"doc_id": doc_id, "source": "caparo", "filename": "caparo.pdf", "text": "..."},
        {"doc_id": doc_id, "source": "caparo", "filename": "caparo.pdf", "text": "..."},
    ]
    attachments = _build_attachments(citations, tmp_path)
    assert len(attachments) == 1
    assert attachments[0]["name"] == "caparo.pdf"
    assert attachments[0]["contentType"] == "application/pdf"
    assert base64.b64decode(attachments[0]["contentBytes"]) == b"%PDF-1.4 fake pdf bytes"


def test_build_attachments_skips_oversized(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("arke.clients.email.MAX_ATTACHMENT_BYTES", 10)
    doc_id = "cd" + "e" * 30
    shard = tmp_path / "data" / "sources" / doc_id[:2]
    shard.mkdir(parents=True)
    (shard / doc_id).write_bytes(b"much more than ten bytes of content")

    citations = [{"doc_id": doc_id, "source": "big", "filename": "big.pdf", "text": "..."}]
    attachments = _build_attachments(citations, tmp_path)
    assert attachments == []


def test_build_attachments_skips_missing_source(tmp_path) -> None:
    citations = [{"doc_id": "nonexistent", "source": "x", "filename": "x.pdf", "text": "..."}]
    attachments = _build_attachments(citations, tmp_path)
    assert attachments == []


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
