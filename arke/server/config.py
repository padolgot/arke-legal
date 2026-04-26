from __future__ import annotations

import os
from dataclasses import dataclass, replace


@dataclass(frozen=True)
class Config:
    # Workspace
    workspace: str = "default"

    # Backend: "local" (llama-cpp-python) or "cloud" (OpenAI)
    backend: str = "local"

    # Local backend — paths to .gguf files
    embed_model_path: str = ""
    inference_model_path: str = ""

    # Cloud backend
    cloud_api_key: str = ""
    cloud_base_url: str = "https://api.openai.com"
    cloud_embed_model: str = "text-embedding-3-small"
    cloud_fast_model: str = "gpt-4o-mini"   # fast/cheap — per-doc filter, trimmer, ingest case-names
    cloud_strong_model: str = "gpt-4o"      # judgment — mosaic clustering

    # RAG parameters
    embedding_dim: int = 0
    chunk_size: int = 600
    overlap: float = 0.0
    alpha: float = 1.0
    k: int = 10

    def resolved(self) -> Config:
        if self.backend == "local":
            if not self.embed_model_path:
                raise ValueError("config: EMBED_MODEL_PATH is required for local backend")
            if not self.inference_model_path:
                raise ValueError("config: INFERENCE_MODEL_PATH is required for local backend")
        elif self.backend == "cloud":
            if not self.cloud_api_key:
                raise ValueError("config: CLOUD_API_KEY is required for cloud backend")
        else:
            raise ValueError(f"config: BACKEND must be 'local' or 'cloud', got '{self.backend}'")

        cfg = replace(self, embedding_dim=self.embedding_dim or DEFAULTS.embedding_dim)

        if cfg.chunk_size < 100 or cfg.chunk_size > 10000:
            raise ValueError(f"config: chunk_size must be 100..10000, got {cfg.chunk_size}")
        if cfg.overlap < 0 or cfg.overlap > 0.5:
            raise ValueError(f"config: overlap must be 0..0.5, got {cfg.overlap}")
        if cfg.alpha < 0 or cfg.alpha > 1:
            raise ValueError(f"config: alpha must be 0..1, got {cfg.alpha}")
        if cfg.k < 1 or cfg.k > 20:
            raise ValueError(f"config: k must be 1..20, got {cfg.k}")

        return cfg

    @staticmethod
    def from_env() -> Config:
        return Config(
            workspace=os.environ.get("ARKE_WORKSPACE", "default"),
            backend=os.environ.get("BACKEND", "local"),
            embed_model_path=os.environ.get("EMBED_MODEL_PATH", ""),
            inference_model_path=os.environ.get("INFERENCE_MODEL_PATH", ""),
            cloud_api_key=os.environ.get("CLOUD_API_KEY", ""),
            cloud_base_url=os.environ.get("CLOUD_BASE_URL", "https://api.openai.com"),
            cloud_embed_model=os.environ.get("CLOUD_EMBED_MODEL", "text-embedding-3-small"),
            cloud_fast_model=os.environ.get("CLOUD_FAST_MODEL", "gpt-4o-mini"),
            cloud_strong_model=os.environ.get("CLOUD_STRONG_MODEL", "gpt-4o"),
            embedding_dim=int(os.environ.get("EMBEDDING_DIM", "0")),
            chunk_size=int(os.environ.get("CHUNK_SIZE", "600")),
            overlap=float(os.environ.get("OVERLAP", "0.0")),
            alpha=float(os.environ.get("ALPHA", "1.0")),
            k=int(os.environ.get("K", "10")),
        )


DEFAULTS = Config(
    embedding_dim=1024,
)
