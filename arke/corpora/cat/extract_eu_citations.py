#!/usr/bin/env python3
"""Reverse-extract EU/CJEU citations from cat_skeleton/text/.

For every text file in cat_skeleton, scans for known EU citation patterns,
captures ~80 chars of surrounding context (where the case name usually
sits), aggregates by canonical citation key, and emits a ranked CSV/JSON
list of every distinct EU authority referenced — even singletons.

Patterns covered:
    ECLI:EU:C/T:YYYY:NNN          (modern, definitive identifier)
    Case C-NNN/YY  /  Case T-NNN/YY (post-1989)
    Case NNN/YY                    (pre-1989)
    [YYYY] ECR I-NNN / II-NNN / NNN (European Court Reports, used 1954-2011)
    [YYYY] N CMLR N                (Common Market Law Reports)

Output: cat_skeleton/eu_citations.jsonl (one line per unique citation key)
         + cat_skeleton/eu_citations_top.txt (human readable top of list)
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path
from arke.corpora._paths import DATA, ENV_FILE



ROOT = DATA / "cat_skeleton"
TEXT_DIR = ROOT / "text"
OUT_JSONL = ROOT / "eu_citations.jsonl"
OUT_TOP = ROOT / "eu_citations_top.txt"

# Patterns ranked by specificity. ECLI is canonical, others are alternatives.
PATTERNS: list[tuple[str, str]] = [
    # Standard 5 — case citations in formal forms.
    ("ECLI",        r"ECLI:EU:[CT]:\d{4}:\d+"),
    # Trailing CJEU markers limited to known suffixes (P=appeal, RENV=re-envoi, R=interim).
    ("CASE_C",      r"Case[s]?\s+[CT]-\d+/\d{2,4}(?:\s+(?:P|PR|RENV|R|REC))?"),
    ("CASE_OLD",    r"Case[s]?\s+\d+/\d{2,4}(?!\d)"),
    ("ECR",         r"\[\s*\d{4}\s*\]\s*ECR\s+(?:I-|II-)?\d+"),
    ("CMLR",        r"\[\s*\d{4}\s*\]\s*\d+\s*CMLR\s+\d+"),
    # Round-1 winners — shorthand variants of the same case identifiers.
    ("JOINED",      r"Joined\s+Cases?\s+[CT]?-?\d+/\d{2,4}"),
    ("BARE_C",      r"(?<![\w-])(?<!Case )(?<!Cases )C-\d+/\d{2,4}(?!\d)"),
    ("BARE_T",      r"(?<![\w-])(?<!Case )(?<!Cases )T-\d+/\d{2,4}(?!\d)"),
    ("ALT_ECLI",    r"(?<!ECLI:)EU:[CT]:\d{4}:\d+(?!\d)"),
    # FREE_V_COMM is noisy — use post-filter to drop fragment matches.
    ("FREE_V_COMM", r"([A-Z][A-Za-z][\w\.\-' ]{2,40})\s+v\.?\s+(?:European\s+)?Commission(?!\s*[\[\(])"),
    # Round-2 winners — Commission decisions / antitrust cases / AG opinions / regulations.
    ("COMM_AT",     r"(?:Case\s+)?AT\.\d{4,5}"),
    ("COMM_COMP",   r"COMP/[A-Z]?\.?\d+(?:\.\d+)?"),
    ("COMM_DEC",    r"(?:Commission\s+)?Decision\s+\d+/\d+/(?:EC|EEC|EU)"),
    ("AG_OPINION",  r"Opinion\s+of\s+(?:Advocate\s+General|AG)\s+\w+"),
    ("REGULATION",  r"Regulation\s+(?:\(?(?:EC|EU|EEC)\)?\s+)?No\.?\s*\d+/\d{4}"),
]

# FREE_V_COMM produces fragment matches like "Co AG v Commission" or "Others v Commission"
# from "Hoffmann-La Roche & Co AG v Commission" / "Aalborg Portland and Others v Commission".
# Drop these post-match so they don't pollute the main list.
FREE_V_COMM_BLOCKLIST = re.compile(
    r"^(?:Co\.?\s+(?:AG|Ltd)|Others|Inc\.?|Corp\.?|Limited|plc)\s+v",
    re.IGNORECASE,
)

COMPILED = [(name, re.compile(pat)) for name, pat in PATTERNS]

# heuristic: case name often appears immediately before "Case X/Y" or right after,
# pattern like "X v Commission" / "X v Council" / "X v Y" capitalised
NAME_NEAR = re.compile(
    r"([A-Z][A-Za-z][\w\.\&\-\(\)' ]{2,80}?\s+v\s+[A-Z][A-Za-z][\w\.\&\-\(\)' ,]{2,80})"
)


def normalize_citation(kind: str, raw: str) -> str:
    s = re.sub(r"\s+", " ", raw).strip()
    if kind == "ECLI":
        return s
    if kind == "CASE_C":
        s = re.sub(r"^Cases?\s+", "Case ", s)
        return s
    if kind == "CASE_OLD":
        s = re.sub(r"^Cases?\s+", "Case ", s)
        return s
    if kind == "ECR":
        s = re.sub(r"\[\s*", "[", s)
        s = re.sub(r"\s*\]", "]", s)
        s = re.sub(r"\s+", " ", s)
        return s
    if kind == "CMLR":
        s = re.sub(r"\s+", " ", s)
        return s
    return s


def extract_from_text(text: str) -> list[tuple[str, str, str, str]]:
    """Return list of (kind, normalized_citation, tight_ctx, wide_ctx)."""
    results: list[tuple[str, str, str, str]] = []
    for kind, regex in COMPILED:
        for m in regex.finditer(text):
            raw = m.group(0)
            # Filter FREE_V_COMM noise (fragment matches).
            if kind == "FREE_V_COMM" and FREE_V_COMM_BLOCKLIST.match(raw):
                continue
            tight = re.sub(r"\s+", " ", text[max(0, m.start() - 50): m.end() + 50]).strip()
            wide = re.sub(r"\s+", " ", text[max(0, m.start() - 150): m.end() + 150]).strip()
            cit = normalize_citation(kind, raw)
            # Cap FREE_V_COMM at 80 chars to avoid pulling sentences.
            if kind == "FREE_V_COMM" and len(cit) > 80:
                cit = cit[:80].rstrip(",.;:")
            results.append((kind, cit, tight, wide))
    return results


def main() -> None:
    aggregations: dict[str, dict] = defaultdict(lambda: {
        "kind": None,
        "count": 0,
        "files": set(),
        "names_seen": set(),
        "context_samples": [],
    })

    text_files = sorted(TEXT_DIR.glob("*.txt"))
    print(f"scanning {len(text_files)} text files...")

    for tf in text_files:
        try:
            text = tf.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            print(f"skip {tf.name}: {e}")
            continue
        for kind, cit, tight, wide in extract_from_text(text):
            agg = aggregations[cit]
            agg["kind"] = kind
            agg["count"] += 1
            agg["files"].add(tf.stem)
            # Names adjacent to the citation only — tight window prevents
            # capturing case names from neighbouring unrelated citations.
            for nm in NAME_NEAR.findall(tight):
                cleaned = re.sub(r"\s+", " ", nm).strip(" ,.;")
                if 5 < len(cleaned) < 120 and ("Commission" in cleaned or " v " in cleaned):
                    agg["names_seen"].add(cleaned[:120])
            if len(agg["context_samples"]) < 3:
                agg["context_samples"].append(wide[:280])

    # Emit JSONL sorted by count desc.
    rows = []
    for cit, agg in aggregations.items():
        rows.append({
            "citation": cit,
            "kind": agg["kind"],
            "mentions": agg["count"],
            "n_files": len(agg["files"]),
            "names_seen": sorted(agg["names_seen"])[:5],
            "context_samples": agg["context_samples"],
        })
    rows.sort(key=lambda r: (-r["mentions"], r["citation"]))

    if OUT_JSONL.exists():
        OUT_JSONL.unlink()
    with OUT_JSONL.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    # Pretty top output.
    with OUT_TOP.open("w", encoding="utf-8") as f:
        f.write(f"Total unique citation strings: {len(rows)}\n")
        f.write(f"Total mentions across corpus: {sum(r['mentions'] for r in rows)}\n\n")
        f.write(f"{'Mentions':>8s}  {'Files':>5s}  {'Kind':<8s}  Citation  / Names\n")
        f.write("-" * 90 + "\n")
        for r in rows[:200]:
            names_short = " / ".join(r["names_seen"][:2])
            f.write(f"{r['mentions']:>8d}  {r['n_files']:>5d}  {r['kind']:<8s}  {r['citation']:<35s}  {names_short[:60]}\n")

    print(f"\nunique citation strings: {len(rows)}")
    print(f"total mentions: {sum(r['mentions'] for r in rows)}")
    print(f"top 30:\n")
    for r in rows[:30]:
        names_short = (" / ".join(r["names_seen"][:1]))[:50]
        print(f"  {r['mentions']:>5d}× ({r['n_files']:>3d} files) [{r['kind']}] {r['citation']:<35s} {names_short}")
    print(f"\nfull list: {OUT_JSONL}")
    print(f"top 200 readable: {OUT_TOP}")


if __name__ == "__main__":
    main()
