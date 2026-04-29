#!/usr/bin/env python3
"""Round 3: creative regex experiments — what other ways do lawyers cite cases?

Tested:
    S.  EU Directives ('Directive 2005/29/EC', 'Directive (EU) 2019/770')
    T.  Old Commission cartel codes ('IV/30.787', 'IV/E-1/35.860')
    U.  CAT internal precedent ('the Tribunal in Genzyme [2004]', 'this CAT in BetterCare')
    V.  Multi-case sequences ('Cases X, Y and Z [YYYY]', 'Cases 100/80, 101/80...')
    W.  Footnote-style judgment refs ('judgment of 22 January 1976 in Case ...')
    X.  CMLR-only citations without prefix ('5 CMLR 23')
    Y.  Italic-style book refs ('Bellamy & Child' / 'Whish & Bailey')
    Z.  CJEU vs CFI implicit refs ('the CFI held' / 'the General Court held')
"""
from __future__ import annotations

import re
from collections import Counter
from pathlib import Path
from arke.corpora._paths import DATA, ENV_FILE



TEXT_DIR = DATA / "cat_skeleton/text"

ALIEN_PATTERNS: dict[str, str] = {
    "S_DIRECTIVE":     r"Directive\s+(?:\(EU\)\s+)?\d+/\d+(?:/(?:EC|EEC|EU))?",
    "T_OLD_COMM_IV":   r"IV/[A-Z]?-?\d+/\d{2,5}\.\d{2,5}|IV/\d{2}\.\d{3,5}",
    "U_CAT_PRECEDENT": r"(?:the|this)\s+(?:Tribunal|CAT)\s+in\s+([A-Z][A-Za-z][\w\-]{1,30}(?:\s+[A-Z][\w-]+){0,3})\s+(?:\[?\(?(\d{4})|v\.?\s+)",
    "V_MULTI_CASE":    r"Cases?\s+\d+/\d{2,4}(?:\s*[,&]\s*(?:and\s+)?\d+/\d{2,4})+",
    "W_DATED_JUDGMENT": r"judgment\s+of\s+\d{1,2}\s+\w+\s+\d{4}\s+in\s+Case\s+\S+",
    "X_CMLR_BARE":     r"(?<!\])\s\d+\s+CMLR\s+\d+",
    "Y_TREATISE":      r"Bellamy\s+(?:and|\&)\s+Child|Whish\s+(?:and|\&)\s+Bailey|Faull\s+(?:and|\&)\s+Nikpay|Jones\s+(?:and|\&)\s+Sufrin",
    "Z_GC_PHRASE":     r"the\s+(?:General\s+Court|CFI|Court\s+of\s+First\s+Instance|Court\s+of\s+Justice)\s+(?:held|stated|found|noted|emphasised|observed)\s+(?:in|that)",
}

COMPILED = {name: re.compile(pat, re.MULTILINE) for name, pat in ALIEN_PATTERNS.items()}


def main() -> None:
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
                norm = re.sub(r"\s+", " ", hit)[:100]
                counts[name][norm] += 1
                files_with_hits[name].add(tf.stem)
                if len(samples[name]) < 8:
                    ctx = re.sub(r"\s+", " ", text[max(0, m.start() - 60): m.end() + 60]).strip()
                    samples[name].append((norm, ctx[:200]))

    print(f"{'pattern':<22s} {'hits':>6s}  {'unique':>7s}  {'files':>6s}")
    print("-" * 50)
    for name in ALIEN_PATTERNS:
        total_hits = sum(counts[name].values())
        unique = len(counts[name])
        nf = len(files_with_hits[name])
        print(f"{name:<22s} {total_hits:>6d}  {unique:>7d}  {nf:>6d}")

    print("\n=== samples per pattern ===\n")
    for name in ALIEN_PATTERNS:
        if not samples[name]:
            print(f"--- {name}: NO MATCHES ---\n")
            continue
        total = sum(counts[name].values())
        unique = len(counts[name])
        print(f"--- {name} ({total} total, {unique} unique) ---")
        for hit, ctx in samples[name][:4]:
            print(f"  hit: {hit!r}")
            print(f"  ctx: {ctx!r}")
        if len(counts[name]) > 1:
            print(f"  top unique by frequency:")
            for h, c in counts[name].most_common(8):
                print(f"    {c:>3d}× {h!r}")
        print()


if __name__ == "__main__":
    main()
