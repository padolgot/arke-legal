import json
from pathlib import Path

import pytest

pytest.skip("obsolete: old Doc.content API; rewrite against new Doc/sdb in phase 2", allow_module_level=True)

from arke.loader import load_docs  # noqa: E402


def test_loads_valid_jsonl(tmp_path):
    f = tmp_path / "docs.jsonl"
    f.write_text(json.dumps({"content": "hello world"}) + "\n")
    docs = load_docs(str(f))
    assert len(docs) == 1
    assert docs[0].content == "hello world"
    assert docs[0].source == "docs"


def test_skips_invalid_json(tmp_path):
    f = tmp_path / "docs.jsonl"
    f.write_text("not json\n" + json.dumps({"content": "valid"}) + "\n")
    docs = load_docs(str(f))
    assert len(docs) == 1
    assert docs[0].content == "valid"


def test_skips_missing_content(tmp_path):
    f = tmp_path / "docs.jsonl"
    lines = [
        json.dumps({"content": ""}),
        json.dumps({"title": "no content field"}),
        json.dumps({"content": "  "}),
        json.dumps({"content": "good"}),
    ]
    f.write_text("\n".join(lines))
    docs = load_docs(str(f))
    assert len(docs) == 1
    assert docs[0].content == "good"


def test_uses_custom_source(tmp_path):
    f = tmp_path / "docs.jsonl"
    f.write_text(json.dumps({"content": "x", "source": "custom"}) + "\n")
    docs = load_docs(str(f))
    assert docs[0].source == "custom"


def test_parses_created_at(tmp_path):
    f = tmp_path / "docs.jsonl"
    f.write_text(json.dumps({"content": "x", "created_at": "2026-01-15T10:00:00Z"}) + "\n")
    docs = load_docs(str(f))
    assert docs[0].created_at.year == 2026
    assert docs[0].created_at.month == 1


def test_loads_directory(tmp_path):
    (tmp_path / "a.jsonl").write_text(json.dumps({"content": "one"}) + "\n")
    (tmp_path / "b.jsonl").write_text(json.dumps({"content": "two"}) + "\n")
    docs = load_docs(str(tmp_path))
    assert len(docs) == 2


def test_preserves_metadata(tmp_path):
    f = tmp_path / "docs.jsonl"
    f.write_text(json.dumps({"content": "x", "metadata": {"tag": "test"}}) + "\n")
    docs = load_docs(str(f))
    assert docs[0].metadata == {"tag": "test"}


def test_loads_txt_files(tmp_path):
    sub = tmp_path / "contracts"
    sub.mkdir()
    (sub / "nda.txt").write_text("This is a non-disclosure agreement.")
    (tmp_path / "policy.txt").write_text("Privacy policy content here.")
    docs = load_docs(str(tmp_path))
    assert len(docs) == 2
    sources = {d.source for d in docs}
    assert "contracts/nda.txt" in sources
    assert "policy.txt" in sources


def test_skips_empty_txt(tmp_path):
    (tmp_path / "empty.txt").write_text("")
    (tmp_path / "good.txt").write_text("real content")
    docs = load_docs(str(tmp_path))
    assert len(docs) == 1
    assert docs[0].content == "real content"
