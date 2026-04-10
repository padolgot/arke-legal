from dataclasses import dataclass

from .. import Mneme
from ..core.config import Config
from ..core.models import embed
from ..core.types import Chunk
from .cache import Cache, corpus_hash
from .corpus import SQUAD_URL, SQUAD_LIMIT, download_squad
from .gen import make_cases
from .metrics import EvalCase, EvalMetrics, EvalResult, score
from .presets import get_preset


@dataclass(frozen=True)
class SweepRow:
    cfg: Config
    metrics: EvalMetrics


async def run_sweep(base_cfg: Config, level: str, limit: int, source_path: str = "") -> list[SweepRow]:
    source_path = _ensure_corpus(source_path)

    configs = get_preset(level, base_cfg)
    rows: list[SweepRow] = []
    chash = corpus_hash(source_path)

    for idx, cfg in enumerate(configs):
        async with Mneme(cfg) as m:
            await m.reset()
            await _ensure_chunks(m, chash, cfg.chunk_size, cfg.overlap, source_path)
            cases = await _ensure_cases(m, chash, cfg.chunk_size, cfg.overlap, limit)

            queries = [c.query for c in cases]
            vectors = await embed(m.cfg, m.http, queries)

            results: list[EvalResult] = []
            for i, case in enumerate(cases):
                hits = await m.db.search(vectors[i], case.query, cfg.alpha, cfg.k)
                results.append(EvalResult(hits=hits, expected_ids=case.expected_ids))

            metrics = score(results)
            rows.append(SweepRow(cfg=cfg, metrics=metrics))
            print(f"  eval {idx + 1}/{len(configs)}: chunk={cfg.chunk_size} overlap={cfg.overlap} alpha={cfg.alpha:.1f} k={cfg.k}")

    rows.sort(key=lambda r: r.metrics.mrr, reverse=True)
    _print_table(rows)
    return rows


def _ensure_corpus(source_path: str) -> str:
    """Returns path to corpus file. Downloads SQuAD if no source provided."""
    if source_path:
        return source_path

    cache = Cache("corpus", url=SQUAD_URL, limit=SQUAD_LIMIT)
    if not cache.exists():
        cache.save(download_squad())
    return str(cache.path)


async def _ensure_chunks(m: Mneme, chash: str, chunk_size: int, overlap: float, source_path: str) -> None:
    cache = Cache("embeddings", corpus=chash, chunk_size=chunk_size, overlap=overlap, model=m.cfg.embedder_model)
    cached = cache.load()
    if cached is not None:
        chunks = [Chunk.from_dict(d) for d in cached]
        await m.db.insert(chunks)
        print(f"cache hit: {len(chunks)} chunks from disk")
    else:
        await m.ingest(source_path)
        chunks = await m.db.fetch_all()
        cache.save([c.to_dict() for c in chunks])


async def _ensure_cases(m: Mneme, chash: str, chunk_size: int, overlap: float, limit: int) -> list[EvalCase]:
    cache = Cache("cases", corpus=chash, chunk_size=chunk_size, overlap=overlap, model=m.cfg.inference_model)
    cached = cache.load()
    if cached is not None:
        cases = [EvalCase(query=c["query"], expected_ids=c["expected_ids"]) for c in cached]
        print(f"cache hit: {len(cases)} eval cases from disk")
        return cases

    cases = await make_cases(m, limit)
    cache.save([{"query": c.query, "expected_ids": c.expected_ids} for c in cases])
    return cases


def _print_table(rows: list[SweepRow]) -> None:
    header = f"{'chunk':>6} {'overlap':>7} {'alpha':>6} {'k':>4} {'prec':>7} {'recall':>7} {'MRR':>7}"
    print(f"\n{'Sweep Results (sorted by MRR)':^50}")
    print(header)
    print("-" * len(header))

    best_mrr = rows[0].metrics.mrr if rows else 0
    for r in rows:
        mrr_str = f"{r.metrics.mrr:.3f}"
        if r.metrics.mrr == best_mrr:
            mrr_str = f"{mrr_str} <-- best"
        print(
            f"{r.cfg.chunk_size:>6} {r.cfg.overlap:>7.1f} {r.cfg.alpha:>6.1f} {r.cfg.k:>4}"
            f" {r.metrics.precision:>7.3f} {r.metrics.recall:>7.3f} {mrr_str:>7}"
        )
    print()
