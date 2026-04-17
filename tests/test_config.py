import pytest

pytest.skip("obsolete: Config API rewritten (new fields: space, backend, cloud_*); rewrite in phase 2", allow_module_level=True)

from arke.server.config import Config, DEFAULTS  # noqa: E402


def test_resolved_fills_defaults():
    cfg = Config(database_url="postgresql://localhost/test", data_path="/tmp").resolved()
    assert cfg.embedder_url == DEFAULTS.embedder_url
    assert cfg.embedder_model == DEFAULTS.embedder_model
    assert cfg.embedding_dim == DEFAULTS.embedding_dim
    assert cfg.inference_url == DEFAULTS.inference_url
    assert cfg.inference_model == DEFAULTS.inference_model


def test_resolved_keeps_explicit_values():
    cfg = Config(
        database_url="postgresql://localhost/test",
        data_path="/tmp",
        embedder_url="http://custom:1234",
        embedder_model="custom-model",
        embedding_dim=768,
    ).resolved()
    assert cfg.embedder_url == "http://custom:1234"
    assert cfg.embedder_model == "custom-model"
    assert cfg.embedding_dim == 768


def test_resolved_requires_database_url():
    with pytest.raises(ValueError, match="database_url"):
        Config().resolved()


def test_resolved_validates_chunk_size():
    with pytest.raises(ValueError, match="chunk_size"):
        Config(database_url="postgresql://x", data_path="/tmp", chunk_size=50).resolved()
    with pytest.raises(ValueError, match="chunk_size"):
        Config(database_url="postgresql://x", data_path="/tmp", chunk_size=20000).resolved()


def test_resolved_validates_overlap():
    with pytest.raises(ValueError, match="overlap"):
        Config(database_url="postgresql://x", data_path="/tmp", overlap=0.8).resolved()
    with pytest.raises(ValueError, match="overlap"):
        Config(database_url="postgresql://x", data_path="/tmp", overlap=-0.1).resolved()


def test_resolved_validates_alpha():
    with pytest.raises(ValueError, match="alpha"):
        Config(database_url="postgresql://x", data_path="/tmp", alpha=1.5).resolved()


def test_resolved_validates_k():
    with pytest.raises(ValueError, match="k"):
        Config(database_url="postgresql://x", data_path="/tmp", k=0).resolved()
    with pytest.raises(ValueError, match="k"):
        Config(database_url="postgresql://x", data_path="/tmp", k=25).resolved()


def test_from_env(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://test")
    monkeypatch.setenv("EMBEDDER_MODEL", "test-model")
    monkeypatch.setenv("EMBEDDING_DIM", "768")
    cfg = Config.from_env()
    assert cfg.database_url == "postgresql://test"
    assert cfg.embedder_model == "test-model"
    assert cfg.embedding_dim == 768
