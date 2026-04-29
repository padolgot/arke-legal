"""
Stress-test the proposed scope (ENTR + POSI + private-enforcement-procedure).

Take eu_clusters_phaseB.jsonl. For each cluster, classify its TOPICAL FIT
based on context_samples (text around the citation in CAT skeletons).
Buckets:
  - DOMINANCE  (POSI / Art 102) — abuse, rebate, dominant, exclusionary, etc.
  - CARTELS    (ENTR / Art 101) — agreement, concerted, cartel, horizontal, etc.
  - DAMAGES    (private enforcement procedure) — damages, follow-on, passing-on,
                limitation, Crehan, Manfredi, Otis
  - MERGERS    (CONC concentrations — out of scope)
  - STATE_AID  (AIDE — out of scope)
  - PROCURE    (MARC — out of scope)
  - PROCEDURAL (general, e.g. right to be heard, proportionality)
  - OTHER      (no signal)

Each cluster goes to top-1 bucket by keyword hit count. Report:
  - distribution of all 1,111 clusters
  - distribution weighted by total_mentions (how often actually cited)
  - top-20 in OTHER bucket (sanity check — what are we missing?)
"""

from __future__ import annotations
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from arke.corpora._paths import DATA, ENV_FILE



INPUT = DATA / "cat_skeleton/eu_clusters_phaseB.jsonl"

# Keyword signatures per bucket. Lower-cased substring match in context_samples.
# Order matters: DOMINANCE checked before CARTELS for cases that touch both
# (rare; we'll let the higher count win).
SIGNATURES = {
    "DOMINANCE": [
        "abuse of dominant", "dominant position", "dominant undertaking",
        "abuse of a dominant", "abusive", "abusively",
        "exclusivity rebate", "loyalty rebate", "fidelity rebate", "target rebate",
        "predatory pric", "excessive pric", "unfair pric", "margin squeeze",
        "refusal to supply", "tying", "bundling", "self-preferenc",
        "exclusionary", "foreclosure", "single dominance",
        "essential facilit", "akzo", "hoffmann-la roche", "hoffman-la roche",
        "michelin", "united brands", "tetra pak", "intel", "google android",
        "deutsche telekom", "post danmark", "tomra", "british airways",
        " article 102", "art 102", "article 82", "art 82",  # Art 82 is pre-Lisbon Art 102
        "chapter ii", "section 18 ", " s.18", "abuse of market",
    ],
    "CARTELS": [
        "concerted practice", "horizontal agreement", "vertical agreement",
        "price-fixing", "price fixing", "bid-rigging", "bid rigging",
        "market sharing", "cartel", "selective distribution",
        "exclusive distribution", "block exemption", "anti-competitive agreement",
        "by object restriction", "by effect restriction",
        "single and continuous infringement",
        " article 101", "art 101", "article 81", "art 81",  # Art 81 is pre-Lisbon Art 101
        "chapter i ", "chapter 1 ", "section 2 ", " s.2 ",
        "trucks cartel", "consten", "grundig",
    ],
    "DAMAGES": [
        "follow-on damage", "stand-alone damage", "private enforcement",
        "passing-on", "passing on",
        "damages directive", "directive 2014/104",
        "limitation period", "five years", "antitrust damage",
        " crehan", "manfredi", " otis ", "kone v ", "skanska",
        "cogeco", "donau chemie",
        "collective redress", "collective proceeding", "class action",
        "opt-out", "opt out", "cpo ", "cat rules",
    ],
    "MERGERS": [
        "concentration", "merger control", "phase ii investigation",
        "merger regulation", "regulation 139/2004", "regulation 4064/89",
        "significant impediment to effective competition", "siec",
        "ge/honeywell", "tetra laval", "general electric/honeywell",
    ],
    "STATE_AID": [
        "state aid", "article 107", "article 87", "article 88",
        "compatible with the internal market", "incompatible with the internal market",
        "altmark", "stardust marine", "deutsche post",
    ],
    "PROCURE": [
        "public procurement", "tender procedure", "call for tender",
        "directive 2014/24", "concession contract",
    ],
    "PROCEDURAL": [
        "right to be heard", "rights of the defence", "rights of defence",
        "duty to state reasons", "obligation to state reasons",
        "proportionality", "legitimate expectation",
        "burden of proof", "standard of proof", "judicial review",
        "access to file", "leniency", "fine calculation",
        "settlement procedure", "commitment decision",
    ],
}


def classify(cluster: dict) -> tuple[str, dict[str, int]]:
    """Return (bucket, hit_counts_per_bucket) for a single cluster."""
    # Aggregate searchable text: contexts + names + members
    parts = []
    parts.extend(cluster.get("context_samples", []))
    parts.extend(cluster.get("names_seen", []))
    parts.extend(cluster.get("members", []))
    blob = " || ".join(parts).lower()

    counts = {bucket: 0 for bucket in SIGNATURES}
    for bucket, kws in SIGNATURES.items():
        for kw in kws:
            counts[bucket] += blob.count(kw)

    # Take bucket with most hits; tie → priority order
    priority = ["DOMINANCE", "CARTELS", "DAMAGES", "MERGERS", "STATE_AID", "PROCURE", "PROCEDURAL"]
    best = max(priority, key=lambda b: (counts[b], -priority.index(b)))
    if counts[best] == 0:
        return "OTHER", counts
    return best, counts


def main():
    bucket_clusters = Counter()
    bucket_mentions = Counter()
    by_bucket: dict[str, list[tuple[str, int]]] = defaultdict(list)

    total_clusters = 0
    total_mentions = 0
    with INPUT.open() as f:
        for line in f:
            c = json.loads(line)
            total_clusters += 1
            mentions = c.get("total_mentions", 0)
            total_mentions += mentions
            bucket, _ = classify(c)
            bucket_clusters[bucket] += 1
            bucket_mentions[bucket] += mentions
            by_bucket[bucket].append((c.get("canonical_key", ""), mentions))

    print("=" * 70)
    print(f"Total clusters: {total_clusters}    Total mentions: {total_mentions}")
    print("=" * 70)
    print()
    print(f"{'Bucket':<14} {'Clusters':>10} {'%clu':>6}  {'Mentions':>10} {'%men':>6}")
    print("-" * 70)
    for bucket in ["DOMINANCE", "CARTELS", "DAMAGES", "MERGERS", "STATE_AID",
                   "PROCURE", "PROCEDURAL", "OTHER"]:
        nc = bucket_clusters[bucket]
        nm = bucket_mentions[bucket]
        pct_c = 100 * nc / total_clusters if total_clusters else 0
        pct_m = 100 * nm / total_mentions if total_mentions else 0
        print(f"{bucket:<14} {nc:>10} {pct_c:>5.1f}%  {nm:>10} {pct_m:>5.1f}%")

    print()
    in_scope_buckets = {"DOMINANCE", "CARTELS", "DAMAGES"}
    in_scope_c = sum(bucket_clusters[b] for b in in_scope_buckets)
    in_scope_m = sum(bucket_mentions[b] for b in in_scope_buckets)
    print(f"IN-SCOPE (DOMINANCE + CARTELS + DAMAGES):")
    print(f"   {in_scope_c}/{total_clusters} clusters = {100*in_scope_c/total_clusters:.1f}%")
    print(f"   {in_scope_m}/{total_mentions} mentions = {100*in_scope_m/total_mentions:.1f}%")

    print()
    print("=" * 70)
    print("Top-15 in OTHER (sanity-check what we miss):")
    print("=" * 70)
    other_sorted = sorted(by_bucket["OTHER"], key=lambda x: -x[1])
    for k, m in other_sorted[:15]:
        print(f"  {m:>4}× {k[:80]}")

    print()
    print("=" * 70)
    print("Top-15 in PROCEDURAL (could be picked up via secondary fetch?):")
    print("=" * 70)
    proc_sorted = sorted(by_bucket["PROCEDURAL"], key=lambda x: -x[1])
    for k, m in proc_sorted[:15]:
        print(f"  {m:>4}× {k[:80]}")

    print()
    print("=" * 70)
    print("Top-15 in DAMAGES (validate private-enforcement bucket):")
    print("=" * 70)
    dmg_sorted = sorted(by_bucket["DAMAGES"], key=lambda x: -x[1])
    for k, m in dmg_sorted[:15]:
        print(f"  {m:>4}× {k[:80]}")


if __name__ == "__main__":
    main()
