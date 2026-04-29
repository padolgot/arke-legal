"""
Split unified manifest into two versions:
  manifest.jsonl       — full corpus (3,377 docs)
  manifest_cited.jsonl — cited subset (cite_in_count >= 1) for fast EVAL/sweep
"""

from __future__ import annotations
import json
import os
from pathlib import Path
from arke.corpora._paths import DATA, ENV_FILE



CACHE = DATA
CORPUS = CACHE / "comp_corpus"
FULL = CORPUS / "manifest.jsonl"
CITED = CORPUS / "manifest_cited.jsonl"


def main():
    docs = []
    with FULL.open() as f:
        for line in f:
            docs.append(json.loads(line))

    cited = [d for d in docs if d.get("cite_in_count", 0) >= 1]

    tmp = CITED.with_suffix(".jsonl.tmp")
    with tmp.open("w") as f:
        for d in cited:
            f.write(json.dumps(d, ensure_ascii=False) + "\n")
        f.flush(); os.fsync(f.fileno())
    os.replace(tmp, CITED)

    # Stats per source
    from collections import Counter
    full_by_src = Counter(d["source"] for d in docs)
    cited_by_src = Counter(d["source"] for d in cited)

    print(f"=== Two corpus versions ===")
    print(f"  Full:   {len(docs)} docs")
    for src, n in full_by_src.items(): print(f"    {src}: {n}")
    print(f"  Cited:  {len(cited)} docs (cite_in >= 1)")
    for src, n in cited_by_src.items(): print(f"    {src}: {n}")
    print()
    print(f"Cited corpus = {100*len(cited)/len(docs):.1f}% of full")


if __name__ == "__main__":
    main()
