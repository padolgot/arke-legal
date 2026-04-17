from arke.server.chunker import chunk


def test_empty_text():
    assert chunk("", 100, 0.0) == []


def test_short_text_single_chunk():
    result = chunk("hello world", 100, 0.0)
    assert len(result) == 1
    assert result[0].clean == "hello world"


def test_splits_long_text():
    text = "sentence one. sentence two. sentence three. sentence four."
    result = chunk(text, 30, 0.0)
    assert len(result) > 1
    joined = "".join(c.clean for c in result)
    assert joined == text


def test_overlap_adds_context():
    text = "A" * 100 + "\n\n" + "B" * 100 + "\n\n" + "C" * 100
    result = chunk(text, 120, 0.3)
    assert len(result) == 3
    mid = result[1]
    assert mid.head != ""
    assert mid.tail != ""
    assert mid.overlapped() == mid.head + mid.clean + mid.tail


def test_no_overlap_empty_head_tail():
    text = "first part\n\nsecond part"
    result = chunk(text, 15, 0.0)
    for c in result:
        assert c.head == ""
        assert c.tail == ""


def test_preserves_all_content():
    text = "one\n\ntwo\n\nthree\n\nfour\n\nfive"
    for size in [10, 20, 50]:
        result = chunk(text, size, 0.0)
        joined = "".join(c.clean for c in result)
        assert joined == text, f"content lost at chunk_size={size}"
