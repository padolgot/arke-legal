"""
Rigorous stress-test: classify each reverse-extracted cluster by querying
CELLAR for the case's official subject-matter classification.

For each cluster:
  1. Derive candidate CELEX values from canonical_key (multiple patterns).
  2. Bulk SPARQL query: which works exist with these CELEX, what are their
     subject-matter labels.
  3. Attribute cluster to all matched subject-matters.
  4. Aggregate: how many clusters / mentions fall under ENTR + POSI vs other?

This uses EU's own taxonomy as ground truth — no keyword heuristics.
"""

from __future__ import annotations
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

import requests
from arke.corpora._paths import DATA, ENV_FILE



ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"
UA = "arke-research/0.1 (stresstest; ivankrylov684@gmail.com)"
INPUT = DATA / "cat_skeleton/eu_clusters_phaseB.jsonl"


def celex_candidates_from_key(key: str, kind: str) -> list[str]:
    """Generate candidate CELEX values for a cluster's canonical_key."""
    out = []
    if kind == "ECLI":
        # Resolve via SPARQL on ECLI directly — handle separately
        return []
    if kind == "CASE_NUM":
        m = re.match(r"^(?:([CT])-)?(\d+)/(\d{2,4})$", key)
        if not m:
            return []
        prefix, n, yr = m.groups()
        yr_full = ("19" + yr if int(yr) >= 50 else "20" + yr) if len(yr) == 2 else yr
        n_pad = n.zfill(4)
        if prefix == "T":
            out += [f"6{yr_full}TJ{n_pad}", f"6{yr_full}TO{n_pad}", f"6{yr_full}A{n_pad}", f"6{yr_full}B{n_pad}"]
        else:
            out += [f"6{yr_full}CJ{n_pad}", f"6{yr_full}J{n_pad}",
                    f"6{yr_full}CO{n_pad}", f"6{yr_full}O{n_pad}",
                    f"6{yr_full}CC{n_pad}",  # AG opinion (sometimes)
                    f"6{yr_full}A{n_pad}",   # CFI judgment (rare for low-numbered post-1989)
                   ]
    elif kind == "AT":
        m = re.match(r"AT\.(\d+)", key)
        if m:
            # AT cases — Commission decisions; CELEX 32YYY... — not certain
            pass
    elif kind == "REGULATION":
        m = re.match(r"^(\d+)/(\d{4})$", key)
        if m:
            n, yr = m.groups()
            out.append(f"3{yr}R{n.zfill(4)}")
    elif kind == "DECISION":
        m = re.match(r"^(\d{4})/(\d+)/(EC|EEC|EU)$", key)
        if m:
            yr, n, _ = m.groups()
            out.append(f"3{yr}D{n.zfill(4)}")
    return out


def run_sparql(query: str, timeout: int = 90):
    # POST avoids URL-length limits when VALUES list is large.
    r = requests.post(
        ENDPOINT,
        data={"query": query, "format": "application/sparql-results+json"},
        headers={
            "User-Agent": UA,
            "Accept": "application/sparql-results+json",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json()


def lookup_subjects_by_celex(celex_list: list[str]) -> dict[str, list[tuple[str, str]]]:
    """For a batch of CELEX strings, return {celex: [(subj_uri, label_en)...]}.
    CELEX is stored as owl:sameAs to URI http://publications.europa.eu/resource/celex/{CELEX}."""
    if not celex_list:
        return {}
    values = " ".join(f"<http://publications.europa.eu/resource/celex/{c}>" for c in celex_list)
    query = f"""
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT ?celex_uri ?subj ?label WHERE {{
  VALUES ?celex_uri {{ {values} }}
  ?work owl:sameAs ?celex_uri .
  ?work cdm:resource_legal_is_about_subject-matter ?subj .
  ?subj skos:prefLabel ?label .
  FILTER(LANG(?label) = "en")
}}
"""
    result = run_sparql(query)
    out: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for row in result.get("results", {}).get("bindings", []):
        celex_uri = row["celex_uri"]["value"]
        celex = celex_uri.rsplit("/", 1)[-1]
        subj = row["subj"]["value"]
        label = row["label"]["value"]
        out[celex].append((subj, label))
    return dict(out)


def lookup_subjects_by_ecli(ecli_list: list[str]) -> dict[str, list[tuple[str, str]]]:
    """ECLI stored as owl:sameAs to http://publications.europa.eu/resource/ecli/{URL-encoded-ECLI}."""
    if not ecli_list:
        return {}
    from urllib.parse import quote
    values = " ".join(f"<http://publications.europa.eu/resource/ecli/{quote(e, safe='')}>" for e in ecli_list)
    query = f"""
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT ?ecli_uri ?subj ?label WHERE {{
  VALUES ?ecli_uri {{ {values} }}
  ?work owl:sameAs ?ecli_uri .
  ?work cdm:resource_legal_is_about_subject-matter ?subj .
  ?subj skos:prefLabel ?label .
  FILTER(LANG(?label) = "en")
}}
"""
    result = run_sparql(query)
    out: dict[str, list[tuple[str, str]]] = defaultdict(list)
    from urllib.parse import unquote
    for row in result.get("results", {}).get("bindings", []):
        ecli_uri = row["ecli_uri"]["value"]
        ecli = unquote(ecli_uri.rsplit("/", 1)[-1])
        subj = row["subj"]["value"]
        label = row["label"]["value"]
        out[ecli].append((subj, label))
    return dict(out)


def main():
    clusters = []
    with INPUT.open() as f:
        for line in f:
            clusters.append(json.loads(line))

    print(f"Loaded {len(clusters)} clusters")

    # Derive CELEX candidates per cluster
    celex_per_cluster: dict[int, list[str]] = {}
    ecli_per_cluster: dict[int, str] = {}
    all_celex = set()
    all_ecli = set()

    for i, c in enumerate(clusters):
        kind = c.get("key_kind") or "UNKEYED"
        key = c.get("canonical_key", "")
        if kind == "ECLI":
            ecli_per_cluster[i] = key
            all_ecli.add(key)
        else:
            cands = celex_candidates_from_key(key, kind)
            celex_per_cluster[i] = cands
            all_celex.update(cands)

    print(f"  candidates: {len(all_celex)} CELEX guesses + {len(all_ecli)} ECLI direct")

    # Bulk lookup CELEX in batches
    celex_subjects: dict[str, list[tuple[str, str]]] = {}
    celex_list = sorted(all_celex)
    BATCH = 100
    for i in range(0, len(celex_list), BATCH):
        batch = celex_list[i:i+BATCH]
        print(f"  CELEX batch {i//BATCH+1}/{(len(celex_list)+BATCH-1)//BATCH} ({len(batch)})", flush=True)
        try:
            sub = lookup_subjects_by_celex(batch)
            celex_subjects.update(sub)
        except Exception as e:
            print(f"    EXC {type(e).__name__}: {e}")

    # Bulk lookup ECLI in batches
    ecli_subjects: dict[str, list[tuple[str, str]]] = {}
    ecli_list = sorted(all_ecli)
    for i in range(0, len(ecli_list), BATCH):
        batch = ecli_list[i:i+BATCH]
        print(f"  ECLI batch {i//BATCH+1}/{(len(ecli_list)+BATCH-1)//BATCH} ({len(batch)})", flush=True)
        try:
            sub = lookup_subjects_by_ecli(batch)
            ecli_subjects.update(sub)
        except Exception as e:
            print(f"    EXC {type(e).__name__}: {e}")

    print(f"  matched: {len(celex_subjects)} CELEX + {len(ecli_subjects)} ECLI")

    # Aggregate per cluster
    cluster_subjects: dict[int, set[str]] = defaultdict(set)  # idx → {label, label, ...}
    matched_clusters = 0
    for i, c in enumerate(clusters):
        labels = set()
        if i in ecli_per_cluster:
            for _, label in ecli_subjects.get(ecli_per_cluster[i], []):
                labels.add(label)
        if i in celex_per_cluster:
            for cand in celex_per_cluster[i]:
                for _, label in celex_subjects.get(cand, []):
                    labels.add(label)
        if labels:
            cluster_subjects[i] = labels
            matched_clusters += 1

    print(f"  resolved: {matched_clusters}/{len(clusters)} clusters got at least one subject-matter from CELLAR")

    # Distribution per subject-matter
    subj_clusters = Counter()
    subj_mentions = Counter()
    in_scope_buckets = {
        "Agreements, decisions and concerted practices",
        "Concerted practices",
        "Dominant position",
    }
    out_scope_buckets = {
        "State aids",
        "Public procurement in the European Union",
    }

    in_scope_clusters = 0
    in_scope_mentions = 0
    out_scope_only_clusters = 0
    no_match_clusters = 0
    no_match_mentions = 0
    no_match_top: list[tuple[str, int]] = []

    for i, c in enumerate(clusters):
        mentions = c.get("total_mentions", 0)
        labels = cluster_subjects.get(i, set())
        for lbl in labels:
            subj_clusters[lbl] += 1
            subj_mentions[lbl] += mentions
        if not labels:
            no_match_clusters += 1
            no_match_mentions += mentions
            no_match_top.append((c.get("canonical_key", ""), mentions))
            continue
        if labels & in_scope_buckets:
            in_scope_clusters += 1
            in_scope_mentions += mentions
        elif labels & out_scope_buckets and not (labels - out_scope_buckets - {"Competition"}):
            # only state aid / procurement, no overlap with agreements/dominance
            out_scope_only_clusters += 1

    print()
    print("=" * 80)
    print("Subject-matter distribution (CELLAR ground truth):")
    print("=" * 80)
    for lbl, n in subj_clusters.most_common(25):
        m = subj_mentions[lbl]
        marker = "★ IN-SCOPE" if lbl in in_scope_buckets else (" out" if lbl in out_scope_buckets else "")
        print(f"  {n:>4} clusters  {m:>5} mentions   {lbl[:60]:<60}  {marker}")

    print()
    print("=" * 80)
    print("Coverage of proposed scope (ENTR + POSI):")
    print("=" * 80)
    total = len(clusters)
    total_men = sum(c.get("total_mentions", 0) for c in clusters)
    print(f"  Resolved clusters (any subject-matter): {matched_clusters}/{total} = {100*matched_clusters/total:.1f}%")
    print(f"  Resolved tagged in-scope (ENTR/POSI/PRAT): {in_scope_clusters} ({100*in_scope_clusters/total:.1f}% of total, {100*in_scope_clusters/max(matched_clusters,1):.1f}% of resolved)")
    print(f"  In-scope mentions: {in_scope_mentions}/{total_men} = {100*in_scope_mentions/total_men:.1f}%")
    print(f"  Unresolved (CELEX/ECLI not in CELLAR): {no_match_clusters} clusters, {no_match_mentions} mentions ({100*no_match_mentions/total_men:.1f}%)")

    print()
    print("Top-15 unresolved clusters (cannot map to CELLAR work):")
    no_match_top.sort(key=lambda x: -x[1])
    for k, m in no_match_top[:15]:
        print(f"  {m:>4}× {k}")


if __name__ == "__main__":
    main()
