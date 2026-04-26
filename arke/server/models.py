"""Models — Embedder and LLM protocols + factory."""
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import numpy as np

from . import sdb
from .config import Config


class Embedder(Protocol):
    def embed(self, texts: list[str]) -> list[list[float]]: ...


class LLM(Protocol):
    def chat(self, system: str | None, user: str) -> str: ...


EMBED_CACHE_TABLE = "embeddings"
# Vestigial "1" preserves compatibility with the prior chunk-level cache key
# format md5(model_short:version:text) — bump if the embed recipe changes.
EMBED_CACHE_VERSION = "1"


@dataclass
class CachingEmbedder:
    """Transparent sdb-backed cache around any Embedder.

    Cache key = md5(model_short + text). Same text never re-embeds across
    runs / chunks / queries / future skill prompts. Stale cache invalidates
    automatically when text content changes (different hash)."""
    inner: Embedder
    model_id: str

    def embed(self, texts: list[str]) -> list[list[float]]:
        keys = [self._key(t) for t in texts]
        cached: dict[int, list[float]] = {}
        miss_idx: list[int] = []
        miss_texts: list[str] = []
        for i, key in enumerate(keys):
            vec = sdb.get_vec(EMBED_CACHE_TABLE, key)
            if vec is not None:
                cached[i] = vec.tolist()
            else:
                miss_idx.append(i)
                miss_texts.append(texts[i])

        if miss_texts:
            new_vecs = self.inner.embed(miss_texts)
            for idx, vec_list in zip(miss_idx, new_vecs):
                arr = np.array(vec_list, dtype=np.float32)
                sdb.put_vec(EMBED_CACHE_TABLE, keys[idx], arr)
                cached[idx] = vec_list

        return [cached[i] for i in range(len(texts))]

    def _key(self, text: str) -> str:
        # model_short keeps cache valid if the model file relocates on disk.
        model_short = Path(self.model_id).name
        raw = f"{model_short}:{EMBED_CACHE_VERSION}:{text}"
        return hashlib.md5(raw.encode()).hexdigest()


@dataclass
class Models:
    embedder: Embedder
    llm: LLM
    strong_llm: LLM  # used for judgment-heavy work (mosaic clustering)

    @staticmethod
    def load(cfg: Config) -> "Models":
        if cfg.backend == "cloud":
            from .backend_cloud import CloudLLM, load
            inner_embedder, llm = load(cfg.cloud_base_url, cfg.cloud_api_key, cfg.cloud_embed_model, cfg.cloud_fast_model)
            strong_llm = CloudLLM(cfg.cloud_base_url, cfg.cloud_api_key, cfg.cloud_strong_model)
            embed_model_id = cfg.cloud_embed_model
        else:
            from .backend_local import load
            inner_embedder, llm = load(cfg.embed_model_path, cfg.inference_model_path)
            strong_llm = llm
            embed_model_id = cfg.embed_model_path

        embedder = CachingEmbedder(inner=inner_embedder, model_id=embed_model_id)
        return Models(embedder=embedder, llm=llm, strong_llm=strong_llm)
