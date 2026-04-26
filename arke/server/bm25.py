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
    """Inverted-index BM25. Postings list per term lets `scores()` touch
    only docs that match query terms, instead of scanning all N docs.
    For 121k chunks and a 10-token query: ~10 × |postings|   instead of
    ~10 × 121k × per-doc-tf-scan."""
    _doc_lengths: dict[str, int] = field(default_factory=dict)              # key → token count
    _postings: dict[str, list[tuple[str, int]]] = field(default_factory=dict)  # term → [(key, tf)]
    _df: dict[str, int] = field(default_factory=dict)                       # term → doc count
    _avgdl: float = 0.0
    _n_docs: int = 0

    def add(self, key: str, text: str) -> None:
        tokens = _tokenize(text)
        self._doc_lengths[key] = len(tokens)
        tf_local: dict[str, int] = {}
        for tok in tokens:
            tf_local[tok] = tf_local.get(tok, 0) + 1
        for term, tf in tf_local.items():
            self._postings.setdefault(term, []).append((key, tf))
            self._df[term] = self._df.get(term, 0) + 1

    def build(self) -> None:
        """Call after all add() calls to finalize avgdl + n_docs cache."""
        self._n_docs = len(self._doc_lengths)
        if self._n_docs:
            self._avgdl = sum(self._doc_lengths.values()) / self._n_docs

    def scores(self, query: str) -> dict[str, float]:
        """Return BM25 score for every key that matches at least one query
        term. Keys with zero score are absent from the dict."""
        terms = _tokenize(query)
        if not terms or not self._n_docs:
            return {}
        result: dict[str, float] = {}
        for term in terms:
            df = self._df.get(term, 0)
            if df == 0:
                continue
            idf = math.log((self._n_docs - df + 0.5) / (df + 0.5) + 1)
            for key, tf in self._postings.get(term, ()):
                dl = self._doc_lengths[key]
                norm = tf * (K1 + 1) / (tf + K1 * (1 - B + B * dl / self._avgdl))
                result[key] = result.get(key, 0.0) + idf * norm
        return result

    def clear(self) -> None:
        self._doc_lengths.clear()
        self._postings.clear()
        self._df.clear()
        self._avgdl = 0.0
        self._n_docs = 0
