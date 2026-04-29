#!/usr/bin/env python3
"""Round 4: final fishing pass — every remaining citation form lawyers use.

Tested:
    AA. Academic journals (ECLR / JCLE / EuLR / etc.)
    AB. Commission press releases (IP/14/1148, MEMO/14/...)
    AC. UK reasoning attribution (per Lord X, per Lord Justice Y)
    AD. TFEU/EC Article references (Article 102 TFEU, Article 81 EC)
    AE. Joined parenthetical case sequences ('(C-89/85, C-104/85)')
    AF. Year-Court abbreviations ('CJEU 2017', 'ECJ 1979')
    AG. Treaty article cross-refs ('Article 23 of Regulation 1/2003', 'Article 7(2)')
    AH. CAT case numbers explicitly ('[2024] CAT 17') — internal CAT, possibly missed
    AI. EUR-Lex CELEX-encoded refs (61976CJ0085 — already tried but try variants)
    AJ. EWCA / EWHC / UKSC native UK case citations
"""
from __future__ import annotations

import re
from collections import Counter
from pathlib import Path
from arke.corpora._paths import DATA, ENV_FILE



TEXT_DIR = DATA / "cat_skeleton/text"

ALIEN_PATTERNS: dict[str, str] = {
    "AA_JOURNAL":      r"\(20\d{2}\)\s*\d+\s*(?:ECLR|JCLE|ELR|ICLQ|EuLR|MLR|LQR)\s*\d+|\[20\d{2}\]\s*\d+\s*(?:ECLR|JCLE|ELR|ICLQ|EuLR|MLR|LQR)\s*\d+",
    "AB_PRESS":        r"(?:IP|MEMO|SPEECH)/\d{2}/\d{2,5}",
    "AC_PER_JUDGE":    r"per\s+(?:Lord|Lady|Mr\s+Justice|Lord\s+Justice|Lady\s+Justice)\s+[A-Z][\w-]+",
    "AD_TFEU_ART":     r"Article\s+\d+\s*(?:\(\d+\))?\s+(?:TFEU|EC|EEC|of\s+the\s+Treaty|of\s+the\s+Charter)",
    "AE_PAREN_CASES":  r"\(\s*(?:Cases?\s+)?[CT]?-?\d+/\d{2,4}\s*(?:[,&]|\sand\s)\s*[CT]?-?\d+/\d{2,4}[^)]{0,80}\)",
    "AF_COURT_YEAR":   r"\b(?:CJEU|ECJ|CFI|GC)\s+\d{4}",
    "AG_REG_ART":      r"Article\s+\d+(?:\(\d+\))?\s+of\s+(?:Council\s+)?Regulation\s+(?:\(?(?:EC|EU|EEC)\)?\s+)?(?:No\.?\s*)?\d+/\d{4}",
    "AH_CAT_INTERNAL": r"\[\d{4}\]\s*CAT\s+\d+",
    "AJ_UK_CIT":       r"\[\d{4}\]\s*(?:UKSC|UKHL|EWCA\s+Civ|EWHC|EWHC\s+\(\w+\))\s+\d+",
    "AK_CHARTER_ART":  r"Article\s+\d+(?:\(\d+\))?\s+of\s+the\s+Charter",
    "AL_PARA_REF":     r"paragraph[s]?\s+\d+(?:\s*[-–]\s*\d+)?\s+of\s+(?:the\s+)?judgment",
    "AM_DOTNET_CASES": r"\b\d+/\d+/(?:EC|EEC|EU)\b",  # bare decision codes
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
