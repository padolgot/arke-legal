#!/usr/bin/env python3
"""Round 2: even more aggressive alien regex experiments.

Tested:
    I.  Commission antitrust case codes  ('Case AT.40099', 'COMP/M.4731')
    J.  Old Commission Decision refs     ('Decision 88/518/EEC', 'Commission Decision 2003/675/EC')
    K.  EFTA Court cases                  ('Case E-15/10')
    L.  AG Opinion references             ('Opinion of Advocate General Kokott in Case ...')
    M.  CFI/General Court 'T-NN/YY' bare  (already partly covered by F_BARE in round 1)
    N.  Italics-style placeholders the PDF kept as quotes ("Tomra", "Microsoft" as case shorthand)
    O.  Square-bracket ECLI-in-brackets   ('[ECLI:EU:C:YYYY:NNN]')
    P.  Year-name pattern                 ('Hoffmann (1979)', 'Tetra Laval (2005)')
"""
from __future__ import annotations

import re
from collections import Counter
from pathlib import Path
from arke.corpora._paths import DATA, ENV_FILE



TEXT_DIR = DATA / "cat_skeleton/text"

ALIEN_PATTERNS: dict[str, str] = {
    "I_COMM_AT":        r"(?:Case\s+)?AT\.\d{4,5}",
    "I_COMM_COMP":      r"COMP/[A-Z]?\.?\d+",
    "J_DECISION":       r"(?:Commission\s+)?Decision\s+\d+/\d+/(?:EC|EEC|EU)",
    "K_EFTA":           r"Case[s]?\s+E-\d+/\d{2,4}",
    "L_AG_OPINION":     r"Opinion\s+of\s+(?:Advocate\s+General|AG)\s+\w+",
    "N_QUOTED_NAME":    r'\(\s*[“"]([A-Z][\w-]{2,30}(?:\s+[A-Z][\w-]{2,30})?)[”"]\s*\)',
    "O_BRACKET_ECLI":   r"\[\s*ECLI:EU:[CT]:\d{4}:\d+\s*\]",
    "P_YEAR_NAME":      r"\b([A-Z][\w-]{3,30}(?:[-\s]+[A-Z][\w-]{2,30})?)\s+\((\d{4})\)",
    "Q_REGULATION":     r"Regulation\s+(?:\(?(?:EC|EU|EEC)\)?\s+)?No\.?\s*\d+/\d{4}",
    "R_PARALLEL_CITE":  r"\[\d{4}\]\s+\d+\s+All\s+ER\s+\(?EC\)?\s+\d+",
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
                norm = re.sub(r"\s+", " ", hit)[:80]
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
