#!/usr/bin/env python3
"""Phase A: pure mechanical deduplication of EU citation strings.

Group citations by canonical case identifier (case number / ECLI / Commission code).
No LLM. No hallucination risk. Strings sharing the same identifier collapse to
one cluster. Strings without an identifier (ECR-only, CMLR-only, free names)
remain UNKEYED and pass through to Phase B.

Input:  cat_skeleton/eu_citations.jsonl   (2,002 entries, frozen main regex)
Output: cat_skeleton/eu_clusters_phaseA.jsonl
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path
from arke.corpora._paths import DATA, ENV_FILE



ROOT = DATA / "cat_skeleton"
INPUT = ROOT / "eu_citations.jsonl"
OUTPUT = ROOT / "eu_clusters_phaseA.jsonl"


def extract_canonical_key(citation: str, kind: str) -> tuple[str, str]:
    """Return (canonical_key, key_kind) for clustering. UNKEYED = no identifier."""
    if kind == "ECLI":
        return (citation, "ECLI")
    if kind == "ALT_ECLI":
        return ("ECLI:" + citation, "ECLI")
    if kind in ("CASE_C", "BARE_C", "BARE_T"):
        m = re.search(r"[CT]-\d+/\d{2,4}", citation)
        if m:
            return (m.group(0), "CASE_NUM")
    if kind == "CASE_OLD":
        m = re.search(r"\d+/\d{2,4}", citation)
        if m:
            return (m.group(0), "CASE_NUM")
    if kind == "JOINED":
        m = re.search(r"[CT]?-?\d+/\d{2,4}", citation)
        if m:
            return (m.group(0).lstrip("-"), "CASE_NUM")
    if kind == "COMM_AT":
        m = re.search(r"AT\.\d+", citation)
        if m:
            return (m.group(0), "AT")
    if kind == "COMM_COMP":
        return (citation.strip(), "COMP")
    if kind == "COMM_DEC":
        m = re.search(r"\d+/\d+/(?:EC|EEC|EU)", citation)
        if m:
            return (m.group(0), "DECISION")
    if kind == "AG_OPINION":
        m = re.search(r"(?:Advocate\s+General|AG)\s+(\w+)", citation)
        if m:
            return ("AG_" + m.group(1), "AG_OPINION")
    if kind == "REGULATION":
        m = re.search(r"\d+/\d{4}", citation)
        if m:
            return (m.group(0), "REGULATION")
    return (citation, "UNKEYED")


def main() -> None:
    rows = [json.loads(l) for l in INPUT.open()]
    print(f"loaded {len(rows)} citation strings\n")

    clusters: dict[tuple[str, str], dict] = {}
    for r in rows:
        key, key_kind = extract_canonical_key(r["citation"], r["kind"])
        cid = (key_kind, key)
        if cid not in clusters:
            clusters[cid] = {
                "canonical_key": key,
                "key_kind": key_kind,
                "members": [],
                "member_kinds": [],
                "total_mentions": 0,
                "files_set": set(),
                "names_seen": set(),
                "context_samples": [],
            }
        c = clusters[cid]
        c["members"].append(r["citation"])
        c["member_kinds"].append(r["kind"])
        c["total_mentions"] += r["mentions"]
        # n_files only available as count, not as set — skip set merge
        c["files_set"].add(r.get("citation"))  # placeholder; use mention count to approximate
        for n in r.get("names_seen", []):
            c["names_seen"].add(n)
        for ctx in r.get("context_samples", []):
            if len(c["context_samples"]) < 4:
                c["context_samples"].append(ctx)

    rows_out = []
    for (key_kind, key), c in clusters.items():
        rows_out.append({
            "canonical_key": c["canonical_key"],
            "key_kind": c["key_kind"],
            "n_variants": len(c["members"]),
            "members": c["members"],
            "member_kinds": sorted(set(c["member_kinds"])),
            "total_mentions": c["total_mentions"],
            "names_seen": sorted(c["names_seen"])[:6],
            "context_samples": c["context_samples"][:3],
        })
    rows_out.sort(key=lambda r: (-r["total_mentions"], r["canonical_key"]))

    if OUTPUT.exists():
        OUTPUT.unlink()
    with OUTPUT.open("w", encoding="utf-8") as f:
        for r in rows_out:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    # Stats by key_kind
    kk_stats: dict[str, list[int]] = defaultdict(list)
    for r in rows_out:
        kk_stats[r["key_kind"]].append(r["n_variants"])

    print(f"input citations:  {len(rows)}")
    print(f"output clusters:  {len(rows_out)}")
    print(f"reduction:        {len(rows) - len(rows_out)} variants merged")
    print()
    print(f"{'key_kind':<14s} {'clusters':>9s}  {'multi-variant':>14s}  {'avg variants':>13s}")
    for kk in sorted(kk_stats):
        n_clust = len(kk_stats[kk])
        n_multi = sum(1 for v in kk_stats[kk] if v > 1)
        avg = sum(kk_stats[kk]) / max(n_clust, 1)
        print(f"  {kk:<12s} {n_clust:>9d}  {n_multi:>14d}  {avg:>13.2f}")

    print(f"\ntop 25 multi-variant clusters (mechanical merges):")
    multi = [r for r in rows_out if r["n_variants"] > 1]
    for r in multi[:25]:
        ms = ", ".join(r["members"][:4])
        if len(r["members"]) > 4:
            ms += f", +{len(r['members']) - 4}"
        print(f"  {r['total_mentions']:>4d}× ({r['n_variants']}v) [{r['key_kind']:<8s}] {r['canonical_key']:<26s}  {ms[:80]}")

    print(f"\nfull output: {OUTPUT}")


if __name__ == "__main__":
    main()
