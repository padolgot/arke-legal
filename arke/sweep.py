from dataclasses import dataclass

from . import Arke
from .config import Config
from .models import embed
from .types import Chunk, SearchHit
from .cache import Cache, corpus_hash
from .digest import digest
from .gen import make_cases, EvalCase
from .presets import get_preset


@dataclass(frozen=True)
class SweepRow:
    cfg: Config
    metrics: EvalMetrics


@dataclass(frozen=True)
class EvalResult:
    hits: list[SearchHit]
    expected_ids: list[str]


@dataclass(frozen=True)
class EvalMetrics:
    precision: float
    recall: float
    mrr: float  # Mean Reciprocal Rank: 1 / position of first correct result


async def run_sweep(base_cfg: Config, level: str, limit: int) -> list[SweepRow]:
    source_path = digest(base_cfg.data_path)

    configs = get_preset(level, base_cfg)
    rows: list[SweepRow] = []
    chash = corpus_hash(source_path)

    for idx, cfg in enumerate(configs):
        async with Arke(cfg) as m:
            await m.reset()
            await _ensure_chunks(m, chash, cfg, source_path)
            cases = await _ensure_cases(m, chash, cfg, limit)

            queries = [c.query for c in cases]
            vectors = await embed(m.cfg, m.http, queries)

            results: list[EvalResult] = []
            for i, case in enumerate(cases):
                hits = await m.db.search(cfg, vectors[i], case.query)
                results.append(EvalResult(hits=hits, expected_ids=case.expected_ids))

            metrics = _score(results)
            rows.append(SweepRow(cfg=cfg, metrics=metrics))
            print(
                f"  eval {idx + 1}/{len(configs)}: chunk={cfg.chunk_size} overlap={cfg.overlap} alpha={cfg.alpha:.1f} k={cfg.k}")

    rows.sort(key=lambda r: r.metrics.mrr, reverse=True)
    _print_table(rows)
    return rows


async def _ensure_chunks(m: Arke, chash: str, cfg: Config, source_path: str) -> None:
    cache = Cache(corpus=chash, chunk_size=cfg.chunk_size, overlap=cfg.overlap, embedder=cfg.embedder_model)
    cached = cache.load()
    if cached is not None:
        chunks = [Chunk.from_dict(d) for d in cached]
        await m.db.insert(chunks)
        print(f"cache hit: {len(chunks)} chunks from disk")
    else:
        await m.ingest(source_path)
        chunks = await m.db.fetch_all()
        cache.save([c.to_dict() for c in chunks])


async def _ensure_cases(m: Arke, chash: str, cfg: Config, limit: int) -> list[EvalCase]:
    cache = Cache(corpus=chash, chunk_size=cfg.chunk_size, overlap=cfg.overlap, inference=cfg.inference_model)
    cached = cache.load()
    if cached is not None:
        cases = [EvalCase(query=c["query"], expected_ids=c["expected_ids"]) for c in cached]
        print(f"cache hit: {len(cases)} eval cases from disk")
        return cases

    cases = await make_cases(m, limit)
    cache.save([{"query": c.query, "expected_ids": c.expected_ids} for c in cases])
    return cases


def _score(results: list[EvalResult]) -> EvalMetrics:
    """Averages precision, recall and MRR across eval results."""
    n = len(results)

    if n == 0:
        return EvalMetrics(precision=0.0, recall=0.0, mrr=0.0)

    sum_p = 0.0
    sum_r = 0.0
    sum_rr = 0.0

    for r in results:
        expected = set(r.expected_ids)
        matched = sum(1 for h in r.hits if h.chunk.id in expected)

        sum_p += matched / len(r.hits) if r.hits else 0.0
        sum_r += matched / len(expected) if expected else 0.0

        for i, h in enumerate(r.hits):
            if h.chunk.id in expected:
                sum_rr += 1.0 / (i + 1)
                break

    return EvalMetrics(precision=sum_p / n, recall=sum_r / n, mrr=sum_rr / n)


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
