"""
Audit current regex coverage by extracting CANDIDATE citation patterns
(broad capture) from 2 representative docs, comparing to what our
production extraction caught, surfacing the systematic gaps.
"""
from __future__ import annotations
import json
import re
from pathlib import Path
from arke.corpora._paths import DATA, ENV_FILE



CACHE = DATA
CORPUS = CACHE / "comp_corpus"

# Broad CANDIDATE patterns — likely-citations, not strict
CANDIDATE_PATTERNS = [
    # Anything in square brackets followed by uppercase abbreviation + number
    ("BRACKET_REPORTER", r"\[\s*\d{4}\s*\]\s+[A-Z][A-Za-z& -]{1,40}?\s+\d+(?:[A-Z]?-?\d+)?"),
    # Anything looking like Case <stuff>/<digits>
    ("CASE_BROAD",       r"Case[s]?\s+\S{1,30}/\d+"),
    # ECLI any form
    ("ECLI_BROAD",       r"ECLI:[^\s,;\)]+"),
    # Numeric refs N/N/N/N (CAT-like, GDPR-like, etc)
    ("MULTI_SLASH",      r"(?<![\d/])\d{1,5}(?:[-/]\d+){2,4}"),
    # Regulation-shaped
    ("REG_BROAD",        r"Regulation\s+(?:\([A-Z]+\)\s*)?\d+/\d{2,4}"),
    # AG opinion broad
    ("AG_BROAD",         r"(?:Advocate\s+General|AG)\s+\w+(?:\s+(?:in|of))?"),
    # Footnote-style "Case <name>" without slash
    ("CASE_NAMED",       r"Case\s+(?:of\s+)?[A-Z][\w\s.&-]{2,40}"),
]

DOCS_TO_AUDIT = [
    ("uk/cat/judgment-or-ruling/2022/2022-CAT-10.txt", "Mark McLaren v MOL"),
    ("uk/cat/judgment-or-ruling/2024/2024-CAT-17.txt", "Allergan v CMA"),
]


def main():
    citations_raw = CORPUS / "citations_raw.jsonl"
    by_doc: dict[str, set[str]] = {}
    with citations_raw.open() as f:
        for line in f:
            r = json.loads(line)
            sd = r["source_doc_id"]
            by_doc.setdefault(sd, set()).add(r["cite_target_key"])

    # Load manifest to map path to doc_id
    docs = {}
    for line in (CORPUS / "manifest.jsonl").open():
        d = json.loads(line)
        docs[d["corpus_path"]] = d

    # Find all matching docs (might need fuzzy path)
    for hint, label in DOCS_TO_AUDIT:
        # find matching corpus_path
        matches = [p for p in docs if hint in p]
        if not matches:
            print(f"NOT FOUND: {hint}")
            continue
        path = matches[0]
        doc = docs[path]
        text = (CORPUS / path).read_text(encoding="utf-8", errors="replace")

        print(f"\n{'='*70}\n{label} — {path}\n{'='*70}")
        print(f"Doc length: {len(text)} chars")
        production_cites = by_doc.get(doc["doc_id"], set())
        print(f"Production extraction caught: {len(production_cites)} unique cites\n")

        # Run broad candidate patterns
        candidates: dict[str, set[str]] = {}
        for pat_name, pat in CANDIDATE_PATTERNS:
            found = set()
            for m in re.finditer(pat, text):
                hit = re.sub(r"\s+", " ", m.group(0)).strip()
                if 5 < len(hit) < 80:
                    found.add(hit)
            candidates[pat_name] = found

        # Compare — what's in candidates that's NOT in production?
        all_cand = set().union(*candidates.values())
        prod_norm = {re.sub(r"\s+", "", c.lower()) for c in production_cites}
        new_finds = []
        for c in all_cand:
            cn = re.sub(r"\s+", "", c.lower())
            # consider "new" if no production cite contains this normalized form
            if not any(cn in p or p in cn for p in prod_norm):
                new_finds.append(c)

        new_finds.sort()
        # categorize new finds
        from collections import Counter
        bucket = Counter()
        for f in new_finds:
            for pat_name, pat in CANDIDATE_PATTERNS:
                if re.search(pat, f):
                    bucket[pat_name] += 1
                    break

        print(f"Candidate patterns surfaced: {len(all_cand)} total")
        print(f"Apparent NEW finds (not in production): {len(new_finds)}")
        print(f"By pattern:")
        for pat_name, cnt in bucket.most_common():
            print(f"  {pat_name:<20}: {cnt}")
        print()
        print(f"Sample new finds (first 25):")
        for f in new_finds[:25]:
            print(f"  - {f}")


if __name__ == "__main__":
    main()
