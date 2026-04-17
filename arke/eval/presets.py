from dataclasses import replace
from itertools import product

from arke.server.config import Config

FAST = "fast"
MEDIUM = "medium"
THOROUGH = "thorough"


def get_preset(level: str, base: Config) -> list[Config]:
    if level == FAST:
        return _expand(base, chunk_sizes=[base.chunk_size], overlaps=[base.overlap], alphas=[base.alpha], ks=[base.k])
    if level == MEDIUM:
        return _expand(base, chunk_sizes=[base.chunk_size], overlaps=[base.overlap], alphas=[0.0, 0.3, 0.5, 0.7, 1.0], ks=[5, 10, 20])
    if level == THOROUGH:
        return _expand(base, chunk_sizes=[500, 1000], overlaps=[0.0, 0.2], alphas=[0.0, 0.3, 0.5, 0.7, 1.0], ks=[5, 10, 20])

    raise ValueError(f"unknown sweep level: {level} (expected: {FAST} | {MEDIUM} | {THOROUGH})")


def _expand(base: Config, chunk_sizes: list[int], overlaps: list[float], alphas: list[float], ks: list[int]) -> list[Config]:
    out: list[Config] = []
    for chunk_size, overlap, alpha, k in product(chunk_sizes, overlaps, alphas, ks):
        cfg = replace(base, chunk_size=chunk_size, overlap=overlap, alpha=alpha, k=k)
        out.append(cfg.resolved())
    return out
