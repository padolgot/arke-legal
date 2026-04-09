from dataclasses import replace
from itertools import product

from ..core.config import MnemeConfig, resolve_config


def get_preset(level: str, base: MnemeConfig) -> list[MnemeConfig]:
    """Expands a sweep level into a list of concrete configs to evaluate.
    Each returned config is a copy of `base` with one combination of
    chunk_size/overlap/alpha/k applied."""
    if level == "fast":
        return _expand(base, [base.chunk_size], [base.overlap], [base.alpha], [base.k])
    if level == "medium":
        return _expand(base, [base.chunk_size], [base.overlap], [0.0, 0.3, 0.5, 0.7, 1.0], [5, 10, 20])
    if level == "thorough":
        return _expand(base, [500, 1000], [0.0, 0.2], [0.0, 0.3, 0.5, 0.7, 1.0], [5, 10, 20])
    raise ValueError(f"unknown sweep level: {level} (expected: fast | medium | thorough)")


def _expand(base: MnemeConfig, chunk_sizes, overlaps, alphas, ks) -> list[MnemeConfig]:
    out: list[MnemeConfig] = []
    for chunk_size, overlap, alpha, k in product(chunk_sizes, overlaps, alphas, ks):
        cfg = replace(base, chunk_size=chunk_size, overlap=overlap, alpha=alpha, k=k)
        resolve_config(cfg)
        out.append(cfg)
    return out
