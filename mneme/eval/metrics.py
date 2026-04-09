from dataclasses import dataclass

from ..core.types import SearchHit


@dataclass(frozen=True)
class EvalCase:
    query: str
    expected_ids: list[int]


@dataclass(frozen=True)
class EvalMetrics:
    precision: float
    recall: float
    mrr: float  # Mean Reciprocal Rank: 1 / position of first correct result


def score(per_case: list[tuple[list[SearchHit], list[int]]]) -> EvalMetrics:
    """Averages precision, recall and MRR across a list of (hits, expected) pairs."""
    n = len(per_case)
    if n == 0:
        return EvalMetrics(precision=0.0, recall=0.0, mrr=0.0)

    sum_p = 0.0
    sum_r = 0.0
    sum_rr = 0.0

    for hits, expected_ids in per_case:
        expected = set(expected_ids)
        matched = sum(1 for h in hits if h.chunk.id in expected)

        sum_p += matched / len(hits) if hits else 0.0
        sum_r += matched / len(expected) if expected else 0.0

        for i, h in enumerate(hits):
            if h.chunk.id in expected:
                sum_rr += 1.0 / (i + 1)
                break

    return EvalMetrics(precision=sum_p / n, recall=sum_r / n, mrr=sum_rr / n)
