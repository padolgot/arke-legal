import pytest

pytest.skip("obsolete: Cache class removed during sdb unification; rewrite against sdb in phase 2", allow_module_level=True)

from arke.cache import Cache  # noqa: E402


def test_save_and_load(tmp_path, monkeypatch):
    monkeypatch.setattr("arke.cache.CACHE_DIR", tmp_path)
    cache = Cache(test="abc")
    assert cache.load() is None

    cache.save([{"x": 1}, {"x": 2}])
    result = cache.load()
    assert result == [{"x": 1}, {"x": 2}]


def test_same_params_same_file(tmp_path, monkeypatch):
    monkeypatch.setattr("arke.cache.CACHE_DIR", tmp_path)
    a = Cache(key="value", num=42)
    b = Cache(key="value", num=42)
    assert a.path == b.path


def test_different_params_different_file(tmp_path, monkeypatch):
    monkeypatch.setattr("arke.cache.CACHE_DIR", tmp_path)
    a = Cache(key="one")
    b = Cache(key="two")
    assert a.path != b.path


def test_corrupt_cache_returns_none(tmp_path, monkeypatch):
    monkeypatch.setattr("arke.cache.CACHE_DIR", tmp_path)
    cache = Cache(test="corrupt")
    cache.path.parent.mkdir(parents=True, exist_ok=True)
    cache.path.write_text("not valid json{{{")

    result = cache.load()
    assert result is None
    assert not cache.path.exists()
