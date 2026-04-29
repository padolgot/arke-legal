import json

from arke.server.loader import load_corpus, load_digest, load_file


def test_load_file_txt(tmp_path):
    f = tmp_path / "policy.txt"
    f.write_text("privacy policy content")
    result = load_file(f)
    assert result is not None
    doc, text = result
    assert text == "privacy policy content"
    assert doc.source == "policy.txt"
    assert doc.metadata["suffix"] == ".txt"


def test_load_file_md(tmp_path):
    f = tmp_path / "note.md"
    f.write_text("# Heading\n\nbody")
    result = load_file(f)
    assert result is not None
    doc, text = result
    assert "Heading" in text
    assert doc.metadata["suffix"] == ".md"


def test_load_file_unsupported_returns_none(tmp_path):
    f = tmp_path / "binary.xyz"
    f.write_bytes(b"\x00\x01\x02")
    assert load_file(f) is None


def test_load_file_empty_returns_none(tmp_path):
    f = tmp_path / "empty.txt"
    f.write_text("")
    assert load_file(f) is None


def test_load_file_whitespace_only_returns_none(tmp_path):
    f = tmp_path / "blank.txt"
    f.write_text("   \n\n  \t  \n")
    assert load_file(f) is None


def test_load_file_relative_source_under_root(tmp_path):
    sub = tmp_path / "contracts"
    sub.mkdir()
    f = sub / "nda.txt"
    f.write_text("non-disclosure")
    doc, _ = load_file(f, root=tmp_path)
    assert doc.source == "contracts/nda.txt"


def test_load_digest_walks_tree(tmp_path):
    (tmp_path / "a.txt").write_text("alpha")
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "b.md").write_text("beta")
    (tmp_path / "skip.xyz").write_bytes(b"binary")
    (tmp_path / ".hidden.txt").write_text("hidden")

    results = load_digest(tmp_path)
    sources = {doc.source for doc, _ in results}
    assert sources == {"a.txt", "sub/b.md"}


def test_load_corpus_reads_manifest(tmp_path):
    text_path = tmp_path / "case.txt"
    text_path.write_text("judgment body")
    manifest = tmp_path / "manifest.jsonl"
    manifest.write_text(json.dumps({
        "doc_id": "uk/cat/2023/abc",
        "corpus_path": "case.txt",
        "title": "Smith v Jones",
    }) + "\n")

    results = load_corpus(tmp_path)
    assert len(results) == 1
    doc, text = results[0]
    assert text == "judgment body"
    assert doc.id == "uk/cat/2023/abc"
    assert doc.metadata["case_name"] == "Smith v Jones"


def test_load_corpus_skips_empty_text(tmp_path):
    (tmp_path / "empty.txt").write_text("   ")
    (tmp_path / "good.txt").write_text("real content")
    manifest = tmp_path / "manifest.jsonl"
    manifest.write_text(
        json.dumps({"doc_id": "1", "corpus_path": "empty.txt", "title": ""}) + "\n"
        + json.dumps({"doc_id": "2", "corpus_path": "good.txt", "title": "Good"}) + "\n"
    )
    results = load_corpus(tmp_path)
    assert len(results) == 1
    assert results[0][0].id == "2"
