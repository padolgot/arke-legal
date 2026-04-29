#!/usr/bin/env python3
"""Aggressive experiment: try alien regex patterns to find citations our
5 standard patterns might be missing.

Tested:
    A. CELEX numbers (EU canonical 11-char IDs like '61976CJ0085')
    B. PDF line-break splits ('Case C-413/\\n14', '[1979]\\nECR 461')
    C. Joined Cases prefix ('Joined Cases C-89/85')
    D. Free-form "X v Commission" names (no number nearby)
    E. Variant ECLI without 'EU:' prefix
    F. Stand-alone "C-413/14" no 'Case' prefix
"""
from __future__ import annotations

import re
from collections import Counter
from pathlib import Path
from arke.corpora._paths import DATA, ENV_FILE



TEXT_DIR = DATA / "cat_skeleton/text"

ALIEN_PATTERNS: dict[str, str] = {
    "A_CELEX":           r"6\d{4}[A-Z]{2}\d{4}",
    "B_LINEBREAK_CASE":  r"Case[s]?\s+[CT]-\d+\s*[\n\r]+\s*/\s*\d{2,4}|Case[s]?\s+[CT]-\d+/\s*[\n\r]+\s*\d{2,4}",
    "C_JOINED_CASES":    r"Joined\s+Cases?\s+[CT]?-?\d+/\d{2,4}",
    "D_FREE_V_COMMISSION": r"([A-Z][A-Za-z][\w\.\-' ]{2,40})\s+v\.?\s+(?:European\s+)?Commission(?!\s*[\[\(])",
    "E_ALT_ECLI":        r"EU:[CT]:\d{4}:\d+(?!\d)",
    "F_BARE_CASENUM":    r"(?<![\w-])C-\d+/\d{2,4}(?!\d)",
    "G_OJ_REF":          r"\[\s*\d{4}\s*\]\s*OJ\s+[A-Z]\s*\d+/\d+",
    "H_PARA_CASENAME":   r"paragraph\s+\d+\s+of\s+([A-Z][\w-]+(?:\s+[A-Z][\w-]+)?)",
}

COMPILED = {name: re.compile(pat, re.MULTILINE) for name, pat in ALIEN_PATTERNS.items()}


def main():
    text_files = sorted(TEXT_DIR.glob("*.txt"))
    print(f"scanning {len(text_files)} text files...\n")

    counts: dict[str, Counter] = {name: Counter() for name in ALIEN_PATTERNS}
    samples: dict[str, list[tuple[str, str]]] = {name: [] for name in ALIEN_PATTERNS}
    files_with_hits: dict[str, set[str]] = {name: set() for name in ALIEN_PATTERNS}

    for tf in text_files:
        text = tf.read_text(encoding="utf-8", errors="replace")
        for name, regex in COMPILED.items():
            for m in regex.finditer(text):
                hit = m.group(0).strip()
                # Normalize whitespace for counting
                norm = re.sub(r"\s+", " ", hit)[:80]
                counts[name][norm] += 1
                files_with_hits[name].add(tf.stem)
                if len(samples[name]) < 8:
                    ctx = re.sub(r"\s+", " ", text[max(0, m.start() - 60): m.end() + 60]).strip()
                    samples[name].append((norm, ctx[:200]))

    print(f"{'pattern':<25s} {'hits':>6s}  {'unique':>7s}  {'files':>6s}")
    print("-" * 50)
    for name in ALIEN_PATTERNS:
        total_hits = sum(counts[name].values())
        unique = len(counts[name])
        nf = len(files_with_hits[name])
        print(f"{name:<25s} {total_hits:>6d}  {unique:>7d}  {nf:>6d}")

    print("\n\n=== samples per pattern ===\n")
    for name in ALIEN_PATTERNS:
        if not samples[name]:
            print(f"--- {name}: NO MATCHES ---\n")
            continue
        print(f"--- {name} ({sum(counts[name].values())} total, {len(counts[name])} unique) ---")
        for hit, ctx in samples[name][:5]:
            print(f"  hit: {hit!r}")
            print(f"  ctx: {ctx!r}")
        # top unique by frequency
        if len(counts[name]) > 1:
            print(f"  top unique by frequency:")
            for h, c in counts[name].most_common(8):
                print(f"    {c:>3d}× {h!r}")
        print()


if __name__ == "__main__":
    main()
