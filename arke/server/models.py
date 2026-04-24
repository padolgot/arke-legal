"""Models — Embedder and LLM protocols + factory."""
from dataclasses import dataclass
from typing import Protocol

from .config import Config


class Embedder(Protocol):
    def embed(self, texts: list[str]) -> list[list[float]]: ...


class LLM(Protocol):
    def chat(self, system: str | None, user: str) -> str: ...


@dataclass
class Models:
    embedder: Embedder
    llm: LLM
    strong_llm: LLM  # used for judgment-heavy work (mosaic clustering)

    @staticmethod
    def load(cfg: Config) -> "Models":
        if cfg.backend == "cloud":
            from .backend_cloud import CloudLLM, load
            embedder, llm = load(cfg.cloud_base_url, cfg.cloud_api_key, cfg.cloud_embed_model, cfg.cloud_fast_model)
            strong_llm = CloudLLM(cfg.cloud_base_url, cfg.cloud_api_key, cfg.cloud_strong_model)
        else:
            from .backend_local import load
            embedder, llm = load(cfg.embed_model_path, cfg.inference_model_path)
            strong_llm = llm  # local backend uses the same LLM for both

        return Models(embedder=embedder, llm=llm, strong_llm=strong_llm)
