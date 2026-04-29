"""
Phase 2: stress-test contrast — compare what UK CAT actually cited
(reverse-extraction) vs what we scraped (top-down EU subject-matter pull).

Inputs:
  reverse-extraction: cat_skeleton/eu_clusters_phaseB.jsonl  (1,111 clusters)
  scraped pool:       eu_pool/manifest_text.jsonl            (1,882 docs)

For each reverse-extracted cluster, check whether its CELEX or ECLI
appears in the scraped pool.

Outputs:
  - Coverage stats (% reverse-extracted that we have in pool)
  - delta_in_pool_only: scraped docs that were NOT cited in CAT corpus (alpha — citations they should make)
  - delta_in_extraction_only: cited cases NOT in our pool (gap — should we expand?)
  - top-mentioned not-in-pool: most cited but missing
"""

from __future__ import annotations
import json
import re
from collections import Counter
from pathlib import Path
from arke.corpora._paths import DATA, ENV_FILE



CACHE = DATA
RX_FILE = CACHE / "cat_skeleton/eu_clusters_phaseB.jsonl"
POOL_FILE = CACHE / "eu_pool/manifest_text.jsonl"
OUT_FILE = CACHE / "corpus/contrast_report.txt"


def case_num_to_celex_candidates(key: str) -> list[str]:
    """Map e.g. '85/76' or 'C-62/86' to candidate CELEX strings."""
    out = []
    m = re.match(r"^(?:([CT])-)?(\d+)/(\d{2,4})$", key)
    if not m:
        return out
    prefix, n, yr = m.groups()
    yr_full = ("19" + yr if int(yr) >= 50 else "20" + yr) if len(yr) == 2 else yr
    n_pad = n.zfill(4)
    if prefix == "T":
        out += [f"6{yr_full}TJ{n_pad}", f"6{yr_full}TO{n_pad}"]
    else:
        out += [f"6{yr_full}CJ{n_pad}", f"6{yr_full}CO{n_pad}", f"6{yr_full}CC{n_pad}"]
    return out


def main():
    # Load reverse-extraction
    rx_clusters = []
    with RX_FILE.open() as f:
        for line in f:
            rx_clusters.append(json.loads(line))

    # Load scraped pool — index by celex and ecli
    pool_celex: set[str] = set()
    pool_ecli: set[str] = set()
    pool_records: list[dict] = []
    with POOL_FILE.open() as f:
        for line in f:
            r = json.loads(line)
            if r.get("celex"):
                pool_celex.add(r["celex"])
            if r.get("ecli"):
                pool_ecli.add(r["ecli"])
            pool_records.append(r)

    print(f"Reverse-extraction clusters: {len(rx_clusters)}")
    print(f"Scraped pool docs: {len(pool_records)} ({len(pool_celex)} CELEX, {len(pool_ecli)} ECLI)")
    print()

    # For each rx cluster, look up
    rx_in_pool = []   # found
    rx_not_in_pool = []
    rx_unkeyed_unresolvable = []  # ECLI/CASE_NUM only — try lookup
    rx_truly_outside = []  # AT/COMP/AG_/REGULATION/DECISION — out of EU JUDG scope

    for c in rx_clusters:
        kind = c.get("key_kind") or "UNKEYED"
        key = c.get("canonical_key", "")
        mentions = c.get("total_mentions", 0)

        found = False
        if kind == "ECLI":
            if key in pool_ecli:
                found = True
        elif kind == "CASE_NUM":
            for cand in case_num_to_celex_candidates(key):
                if cand in pool_celex:
                    found = True
                    break
        elif kind in ("AT", "COMP", "DECISION", "REGULATION", "AG_OPINION"):
            rx_truly_outside.append((kind, key, mentions))
            continue
        elif kind == "UNKEYED":
            # Skip — no machine key
            rx_unkeyed_unresolvable.append((key, mentions))
            continue

        if found:
            rx_in_pool.append((kind, key, mentions))
        else:
            rx_not_in_pool.append((kind, key, mentions))

    # Pool docs NOT cited in CAT corpus (alpha)
    rx_pool_celex_set = set()
    rx_pool_ecli_set = set()
    for c in rx_clusters:
        kind = c.get("key_kind") or "UNKEYED"
        key = c.get("canonical_key", "")
        if kind == "ECLI":
            rx_pool_ecli_set.add(key)
        elif kind == "CASE_NUM":
            for cand in case_num_to_celex_candidates(key):
                rx_pool_celex_set.add(cand)

    pool_not_cited = []
    for r in pool_records:
        cited = (r.get("celex") in rx_pool_celex_set) or (r.get("ecli") in rx_pool_ecli_set)
        if not cited:
            pool_not_cited.append(r)

    # === Reporting ===
    lines = []
    def w(s=""):
        print(s)
        lines.append(s)

    w("=" * 70)
    w("PHASE 2 CONTRAST: reverse-extraction vs scraped pool")
    w("=" * 70)

    total_rx = len(rx_clusters)
    total_rx_mentions = sum(c.get("total_mentions", 0) for c in rx_clusters)
    in_count = len(rx_in_pool)
    out_count = len(rx_not_in_pool)
    outside_count = len(rx_truly_outside)
    unkeyed_count = len(rx_unkeyed_unresolvable)

    in_mentions = sum(m for _, _, m in rx_in_pool)
    out_mentions = sum(m for _, _, m in rx_not_in_pool)
    outside_mentions = sum(m for _, _, m in rx_truly_outside)
    unkeyed_mentions = sum(m for _, m in rx_unkeyed_unresolvable)

    w()
    w(f"Reverse-extraction clusters:       {total_rx} ({total_rx_mentions} mentions)")
    w(f"  ★ IN pool (validated):           {in_count} ({in_mentions} mentions, {100*in_mentions/total_rx_mentions:.1f}%)")
    w(f"    NOT in pool (gap, JUDG kind):  {out_count} ({out_mentions} mentions, {100*out_mentions/total_rx_mentions:.1f}%)")
    w(f"    OUTSIDE scope (AT/COMP/etc.):  {outside_count} ({outside_mentions} mentions, {100*outside_mentions/total_rx_mentions:.1f}%)")
    w(f"    UNKEYED (no machine key):      {unkeyed_count} ({unkeyed_mentions} mentions, {100*unkeyed_mentions/total_rx_mentions:.1f}%)")
    w()
    w("Coverage of CAT-cited cases (judgments only):")
    judg_total = in_count + out_count
    if judg_total:
        w(f"  {in_count}/{judg_total} = {100*in_count/judg_total:.1f}% of judgment-keyed clusters in our pool")
    judg_mentions = in_mentions + out_mentions
    if judg_mentions:
        w(f"  {in_mentions}/{judg_mentions} = {100*in_mentions/judg_mentions:.1f}% of judgment mentions covered")

    w()
    w("=" * 70)
    w(f"Pool docs NOT cited in CAT corpus (alpha — uncited but doctrinally close):")
    w(f"  {len(pool_not_cited)} / {len(pool_records)} = {100*len(pool_not_cited)/len(pool_records):.1f}%")
    w("  Sample (first 10):")
    for r in pool_not_cited[:10]:
        w(f"    {r.get('celex','?'):>15s}  {r.get('resource_type','?'):8s}  {(r.get('title') or '')[:80]}")

    w()
    w("=" * 70)
    w("Top-15 MISSING from pool (cited in CAT but we don't have them):")
    w("=" * 70)
    rx_not_in_pool.sort(key=lambda x: -x[2])
    for kind, key, m in rx_not_in_pool[:15]:
        w(f"  {m:>4}× {kind:<10s} {key}")

    w()
    w("Top-10 OUTSIDE-scope (CAT cites these but they're AT/COMP/AG_OPINION):")
    rx_truly_outside.sort(key=lambda x: -x[2])
    for kind, key, m in rx_truly_outside[:10]:
        w(f"  {m:>4}× {kind:<10s} {key}")

    OUT_FILE.write_text("\n".join(lines) + "\n")
    print(f"\nReport written: {OUT_FILE}")


if __name__ == "__main__":
    main()
