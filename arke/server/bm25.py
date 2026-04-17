"""In-memory BM25 index. Built once at ingest, queried on every ask.

Standard Okapi BM25 with k1=1.5, b=0.75.
Keys are arbitrary strings — we use "<doc_id>:<chunk_index>".
"""
import math
import re
from dataclasses import dataclass, field

K1 = 1.5
B = 0.75


def _tokenize(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


@dataclass
class BM25Index:
    _docs: dict[str, list[str]] = field(default_factory=dict)       # key → tokens
    _df: dict[str, int] = field(default_factory=dict)               # term → doc count
    _avgdl: float = 0.0

    def add(self, key: str, text: str) -> None:
        tokens = _tokenize(text)
        self._docs[key] = tokens
        for term in set(tokens):
            self._df[term] = self._df.get(term, 0) + 1

    def build(self) -> None:
        """Call after all add() calls to finalize avgdl."""
        if self._docs:
            self._avgdl = sum(len(t) for t in self._docs.values()) / len(self._docs)

    def scores(self, query: str) -> dict[str, float]:
        """Return BM25 score for every indexed key. Zero scores omitted."""
        terms = _tokenize(query)
        if not terms or not self._docs:
            return {}

        n = len(self._docs)
        result: dict[str, float] = {}

        for term in terms:
            df = self._df.get(term, 0)
            if df == 0:
                continue
            idf = math.log((n - df + 0.5) / (df + 0.5) + 1)

            for key, tokens in self._docs.items():
                tf = tokens.count(term)
                if tf == 0:
                    continue
                dl = len(tokens)
                norm = tf * (K1 + 1) / (tf + K1 * (1 - B + B * dl / self._avgdl))
                result[key] = result.get(key, 0.0) + idf * norm

        return result

    def clear(self) -> None:
        self._docs.clear()
        self._df.clear()
        self._avgdl = 0.0
