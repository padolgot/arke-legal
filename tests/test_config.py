import pytest

from arke.server.config import DEFAULTS, Config


def test_resolved_fills_default_embedding_dim():
    cfg = Config(
        backend="local",
        embed_model_path="/tmp/embed.gguf",
        inference_model_path="/tmp/inf.gguf",
    ).resolved()
    assert cfg.embedding_dim == DEFAULTS.embedding_dim


def test_resolved_keeps_explicit_embedding_dim():
    cfg = Config(
        backend="local",
        embed_model_path="/tmp/embed.gguf",
        inference_model_path="/tmp/inf.gguf",
        embedding_dim=768,
    ).resolved()
    assert cfg.embedding_dim == 768


def test_local_requires_embed_model_path():
    with pytest.raises(ValueError, match="EMBED_MODEL_PATH"):
        Config(backend="local", inference_model_path="/tmp/inf.gguf").resolved()


def test_local_requires_inference_model_path():
    with pytest.raises(ValueError, match="INFERENCE_MODEL_PATH"):
        Config(backend="local", embed_model_path="/tmp/embed.gguf").resolved()


def test_cloud_requires_api_key():
    with pytest.raises(ValueError, match="CLOUD_API_KEY"):
        Config(backend="cloud").resolved()


def test_cloud_resolves_with_api_key():
    cfg = Config(backend="cloud", cloud_api_key="sk-xxx").resolved()
    assert cfg.backend == "cloud"


def test_unknown_backend_rejected():
    with pytest.raises(ValueError, match="BACKEND"):
        Config(backend="quantum").resolved()


def test_chunk_size_bounds():
    base = dict(backend="cloud", cloud_api_key="sk-x")
    with pytest.raises(ValueError, match="chunk_size"):
        Config(**base, chunk_size=50).resolved()
    with pytest.raises(ValueError, match="chunk_size"):
        Config(**base, chunk_size=20000).resolved()


def test_overlap_bounds():
    base = dict(backend="cloud", cloud_api_key="sk-x")
    with pytest.raises(ValueError, match="overlap"):
        Config(**base, overlap=0.8).resolved()
    with pytest.raises(ValueError, match="overlap"):
        Config(**base, overlap=-0.1).resolved()


def test_alpha_bounds():
    base = dict(backend="cloud", cloud_api_key="sk-x")
    with pytest.raises(ValueError, match="alpha"):
        Config(**base, alpha=1.5).resolved()
    with pytest.raises(ValueError, match="alpha"):
        Config(**base, alpha=-0.1).resolved()


def test_k_bounds():
    base = dict(backend="cloud", cloud_api_key="sk-x")
    with pytest.raises(ValueError, match="k"):
        Config(**base, k=0).resolved()
    with pytest.raises(ValueError, match="k"):
        Config(**base, k=25).resolved()


def test_from_env_reads_variables(monkeypatch):
    monkeypatch.setenv("ARKE_WORKSPACE", "demo")
    monkeypatch.setenv("BACKEND", "cloud")
    monkeypatch.setenv("CLOUD_API_KEY", "sk-test")
    monkeypatch.setenv("EMBEDDING_DIM", "768")
    monkeypatch.setenv("CHUNK_SIZE", "500")
    cfg = Config.from_env()
    assert cfg.workspace == "demo"
    assert cfg.backend == "cloud"
    assert cfg.cloud_api_key == "sk-test"
    assert cfg.embedding_dim == 768
    assert cfg.chunk_size == 500
