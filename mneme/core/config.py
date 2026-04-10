from __future__ import annotations

import os
from dataclasses import dataclass, replace


OLLAMA = "ollama"
OPENAI = "openai"


@dataclass(frozen=True)
class Config:
    database_url: str = ""
    provider: str = OLLAMA
    provider_api_key: str = ""
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
        """Fills empty provider fields from defaults, validates, returns new Config."""
        if not self.database_url:
            raise ValueError("config: database_url is required")
        if self.provider not in PROVIDER_DEFAULTS:
            raise ValueError(f"config: unknown provider '{self.provider}'")
        if self.provider == OPENAI and not self.provider_api_key:
            raise ValueError("config: provider_api_key is required for openai")

        defaults = PROVIDER_DEFAULTS[self.provider]
        cfg = replace(
            self,
            embedder_url=self.embedder_url or defaults.embedder_url,
            embedder_model=self.embedder_model or defaults.embedder_model,
            embedding_dim=self.embedding_dim or defaults.embedding_dim,
            inference_url=self.inference_url or defaults.inference_url,
            inference_model=self.inference_model or defaults.inference_model,
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
        database_url = os.environ.get("DATABASE_URL", "")
        if not database_url:
            raise ValueError("DATABASE_URL is not set")
        return Config(
            database_url=database_url,
            provider=os.environ.get("PROVIDER", OLLAMA),
            provider_api_key=os.environ.get("PROVIDER_API_KEY", ""),
        )


PROVIDER_DEFAULTS = {
    OLLAMA: Config(
        embedder_url="http://localhost:11434",
        embedder_model="bge-m3",
        embedding_dim=1024,
        inference_url="http://localhost:11434",
        inference_model="llama3:8b-instruct-q4_K_M",
    ),
    OPENAI: Config(
        embedder_url="https://api.openai.com",
        embedder_model="text-embedding-3-small",
        embedding_dim=1536,
        inference_url="https://api.openai.com",
        inference_model="gpt-4.1-nano",
    ),
}
