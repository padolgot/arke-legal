from __future__ import annotations

import os
from dataclasses import dataclass, replace


@dataclass(frozen=True)
class Config:
    database_url: str = ""
    data_path: str = ""
    api_key: str = ""
    embedder_url: str = ""
    embedder_model: str = ""
    embedding_dim: int = 0
    inference_url: str = ""
    inference_model: str = ""
    chunk_size: int = 600
    overlap: float = 0.0
    alpha: float = 0.7
    k: int = 5

    def resolved(self) -> Config:
        """Fills empty fields from defaults, validates, returns new Config."""
        if not self.database_url:
            raise ValueError("config: database_url is required")
        if not self.data_path:
            raise ValueError("config: data_path is required — set DATA_PATH in .env")

        cfg = replace(
            self,
            embedder_url=self.embedder_url or DEFAULTS.embedder_url,
            embedder_model=self.embedder_model or DEFAULTS.embedder_model,
            embedding_dim=self.embedding_dim or DEFAULTS.embedding_dim,
            inference_url=self.inference_url or DEFAULTS.inference_url,
            inference_model=self.inference_model or DEFAULTS.inference_model,
        )

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
            database_url=os.environ.get("DATABASE_URL", ""),
            data_path=os.environ.get("DATA_PATH", ""),
            api_key=os.environ.get("API_KEY", ""),
            embedder_url=os.environ.get("EMBEDDER_URL", ""),
            embedder_model=os.environ.get("EMBEDDER_MODEL", ""),
            embedding_dim=int(os.environ.get("EMBEDDING_DIM", "0")),
            inference_url=os.environ.get("INFERENCE_URL", ""),
            inference_model=os.environ.get("INFERENCE_MODEL", ""),
        )


DEFAULTS = Config(
    embedder_url="http://localhost:11434",
    embedder_model="bge-m3",
    embedding_dim=1024,
    inference_url="http://localhost:11434",
    inference_model="llama3:8b-instruct-q4_K_M",
)
