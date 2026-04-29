"""
Precision check: are we extracting REAL citations or noise?

Strategy:
  1. Pattern-level diagnostics: count matches per pattern, find outliers
  2. Random sample 100 extracted cites, classify by whether they look real
  3. Check around-match context: does it look like a citation?
"""
from __future__ import annotations
import json
import random
import re
from collections import Counter
from pathlib import Path
from arke.corpora._paths import DATA, ENV_FILE



CACHE = DATA
CORPUS = CACHE / "comp_corpus"
RAW = CORPUS / "citations_raw.jsonl"


def looks_real(key: str, kind: str) -> str:
    """Heuristic classification: real | suspicious | clearly_noise."""
    k = key.strip()
    # Year-bracket reporters: validate year is plausible
    m = re.match(r"\[\s*(\d{4})\s*\]", k)
    if m:
        yr = int(m.group(1))
        if yr < 1900 or yr > 2030:
            return "clearly_noise"
    # ECLI: validate structure
    if k.startswith("ECLI:") or kind == "ECLI":
        if not re.match(r"^ECLI:[A-Z]{2}:[A-Z]+:\d{4}:\d+$", k):
            return "suspicious"
    # CAT case ref
    if kind == "CAT_REF":
        m = re.match(r"^(\d+)(?:-\d+)?/(\d+)/(\d+)/(\d+)$", k)
        if not m:
            return "suspicious"
    # Multi-slash: should look like \d+/\d+ in legitimate format
    return "real"


def main():
    by_pattern = Counter()
    by_doc_pattern = Counter()
    samples_by_pattern: dict[str, list[str]] = {}
    classify = Counter()

    with RAW.open() as f:
        for line in f:
            r = json.loads(line)
            kind = r["cite_target_kind"]
            key = r["cite_target_key"]
            by_pattern[kind] += 1
            samples_by_pattern.setdefault(kind, []).append(key)
            cls = looks_real(key, kind)
            classify[cls] += 1

    print("=== Pattern frequency (top 30) ===")
    for kind, n in by_pattern.most_common(30):
        sample = samples_by_pattern[kind][0] if samples_by_pattern[kind] else ""
        print(f"  {n:>7}  {kind:<20s}  e.g. {sample[:60]}")

    print()
    print("=== Heuristic classification of all extracted ===")
    total = sum(classify.values())
    for cls, n in classify.most_common():
        print(f"  {cls:<15}: {n} ({100*n/total:.1f}%)")

    print()
    print("=== Suspicious samples (random 30) ===")
    suspicious = []
    for kind, keys in samples_by_pattern.items():
        for k in keys:
            if looks_real(k, kind) != "real":
                suspicious.append((kind, k))
    random.seed(42)
    random.shuffle(suspicious)
    for kind, key in suspicious[:30]:
        print(f"  {kind:<15} {key}")


if __name__ == "__main__":
    main()
