from dataclasses import dataclass

from ..core.config import MnemeConfig
from ..core.models import embed
from ..mneme import Mneme
from .gen import make_cases
from .metrics import EvalMetrics, score
from .presets import get_preset


@dataclass(frozen=True)
class SweepRow:
    """One evaluated configuration: the exact cfg used and the metrics
    it produced. Self-contained — a list of these is enough to reproduce,
    compare, or render a sweep."""
    cfg: MnemeConfig
    metrics: EvalMetrics


async def run_sweep(base_cfg: MnemeConfig, level: str, limit: int, source_path: str) -> list[SweepRow]:
    configs = get_preset(level, base_cfg)
    groups = _group_by_chunking(configs)
    rows: list[SweepRow] = []
    i = 0

    for (chunk_size, overlap), group in groups.items():
        # Fresh Mneme per group: its cfg is immutable, so changing chunk
        # settings means a new instance. Pool recreation is cheap compared
        # to re-ingest (embeddings dominate).
        async with Mneme(group[0]) as m:
            await m.reset()
            await m.ingest(source_path)

            cases = await make_cases(m, limit)
            print(f"sweep: generated {len(cases)} cases from {limit} chunks")

            for cfg in group:
                i += 1
                print(
                    f"\nsweep [{i}/{len(configs)}] "
                    f"chunk_size={cfg.chunk_size} overlap={cfg.overlap} "
                    f"alpha={cfg.alpha} k={cfg.k}"
                )

                per_case = []
                for c in cases:
                    vectors = await embed(m._http, m.cfg.embedder_url, m.cfg.embedder_model, [c.query])
                    hits = await m.db.search(vectors[0], c.query, cfg.alpha, cfg.k)
                    per_case.append((hits, c.expected_ids))

                metrics = score(per_case)
                rows.append(SweepRow(cfg=cfg, metrics=metrics))

                print(f"sweep: P={metrics.precision:.3f} R={metrics.recall:.3f} MRR={metrics.mrr:.3f}")

    # Sorted by MRR — the most telling retrieval metric.
    rows.sort(key=lambda r: r.metrics.mrr, reverse=True)
    _print_table(rows)
    return rows


def _group_by_chunking(configs: list[MnemeConfig]) -> dict[tuple[int, float], list[MnemeConfig]]:
    """Groups configs that share chunk_size+overlap. Re-ingest happens once
    per group; within a group only cheap search params (alpha, k) change."""
    groups: dict[tuple[int, float], list[MnemeConfig]] = {}
    for cfg in configs:
        key = (cfg.chunk_size, cfg.overlap)
        groups.setdefault(key, []).append(cfg)
    return groups


def _print_table(rows: list[SweepRow]) -> None:
    # Plain ASCII, no rich: the agent parses this as text.
    print("\nRESULTS (sorted by MRR desc):")
    print("chunk_size | overlap | alpha |  k | precision | recall |   MRR")
    print("-----------+---------+-------+----+-----------+--------+------")
    for r in rows:
        print(
            f"{r.cfg.chunk_size:>10}"
            f" | {r.cfg.overlap:>7}"
            f" | {r.cfg.alpha:>5.1f}"
            f" | {r.cfg.k:>2}"
            f" | {r.metrics.precision:>9.3f}"
            f" | {r.metrics.recall:>6.3f}"
            f" | {r.metrics.mrr:>5.3f}"
        )
