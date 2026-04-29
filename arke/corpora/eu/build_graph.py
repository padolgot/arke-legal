"""
Build citation graph from raw citations.

Steps:
  1. Build ID index — for each doc, all canonical identifiers (CELEX, ECLI,
     case-num, neutral_citation, case_ref).
  2. Resolve each raw citation to a target doc_id (best-effort).
  3. Aggregate: for each (source_doc_id, target_doc_id) pair, count edges.
  4. Compute per-doc cite_in_count, cite_out_count.
  5. Update manifest with weights.

Outputs:
  comp_corpus/citation_graph.jsonl — edges with counts
  comp_corpus/manifest.jsonl       — augmented with cite_in_count, cite_out_count
"""

from __future__ import annotations
import json
import os
import re
from collections import Counter, defaultdict
from pathlib import Path
from arke.corpora._paths import DATA, ENV_FILE



CACHE = DATA
CORPUS_DIR = CACHE / "comp_corpus"
MANIFEST = CORPUS_DIR / "manifest.jsonl"
RAW_CITES = CORPUS_DIR / "citations_raw.jsonl"
GRAPH_FILE = CORPUS_DIR / "citation_graph.jsonl"


def case_num_to_celex(key: str) -> list[str]:
    """Try to map a 'Case 85/76'-style key to candidate CELEX strings."""
    m = re.search(r"(?:([CT])-)?(\d+)/(\d{2,4})", key)
    if not m:
        return []
    p, n, y = m.groups()
    yr = ("19" + y if int(y) >= 50 else "20" + y) if len(y) == 2 else y
    n_pad = n.zfill(4)
    if p == "T":
        return [f"6{yr}TJ{n_pad}", f"6{yr}TO{n_pad}"]
    return [f"6{yr}CJ{n_pad}", f"6{yr}CO{n_pad}", f"6{yr}CC{n_pad}", f"6{yr}CV{n_pad}"]


def normalize_uk_neutral(key: str) -> str | None:
    """Normalize UK neutral citation to compact key like '2026-CAT-36'."""
    # e.g. '[2026] CAT 36' → '2026-CAT-36'
    m = re.search(r"\[\s*(\d{4})\s*\]\s+(CAT|UKSC|EWCA Civ|EWCA Crim|EWHC|UKHL|UKPC)\s+(\d+)(?:\s*\(([A-Z][a-z]+)\))?", key)
    if m:
        year, court, num, div = m.group(1), m.group(2), m.group(3), m.group(4) or ""
        court_compact = court.replace(" ", "-")
        if div:
            return f"{year}-{court_compact}-{num}-{div}"
        return f"{year}-{court_compact}-{num}"
    return None


def normalize_cat_ref(key: str) -> str | None:
    """CAT case ref e.g. '1234/5/12/26' → as-is."""
    m = re.match(r"^(\d{4})/(\d)/(\d{1,3})/(\d{2})$", key)
    if m:
        return key
    return None


def main():
    # Load manifest, build ID index
    docs: list[dict] = []
    with MANIFEST.open() as f:
        for line in f:
            docs.append(json.loads(line))

    # Maps from canonical IDs → doc_id
    celex_to_doc: dict[str, str] = {}
    ecli_to_doc: dict[str, str] = {}
    neutral_to_doc: dict[str, str] = {}
    case_ref_to_doc: dict[str, str] = {}

    for d in docs:
        if d.get("celex"):
            celex_to_doc[d["celex"]] = d["doc_id"]
        if d.get("ecli"):
            ecli_to_doc[d["ecli"]] = d["doc_id"]
        if d.get("neutral_citation"):
            n = normalize_uk_neutral(d["neutral_citation"])
            if n:
                neutral_to_doc[n] = d["doc_id"]
        if d.get("case_ref"):
            case_ref_to_doc[d["case_ref"]] = d["doc_id"]

    # Joined-case fix: enrich celex_to_doc by querying CELLAR for ALL CELEX/case-num
    # variants of each work. CELLAR may register a joined case under a single lead
    # CELEX while citations refer to any participant. We pre-fetch all sameAs CELEX
    # links per work and add them as aliases.
    print("Resolving joined-case CELEX aliases via CELLAR...")
    import requests
    ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"
    work_uris = []
    for d in docs:
        if d["source"] == "eu_cellar" and d.get("cellar_uuid"):
            work_uris.append(f"http://publications.europa.eu/resource/cellar/{d['cellar_uuid']}")
    BATCH = 100
    extra_celex_added = 0
    for i in range(0, len(work_uris), BATCH):
        batch = work_uris[i:i+BATCH]
        values = " ".join(f"<{u}>" for u in batch)
        q = f"""
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT ?work ?same WHERE {{
  VALUES ?work {{ {values} }}
  ?work owl:sameAs ?same .
  FILTER(STRSTARTS(STR(?same), "http://publications.europa.eu/resource/celex/"))
}}
"""
        try:
            r = requests.post(ENDPOINT,
                data={"query": q, "format": "application/sparql-results+json"},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=120)
            r.raise_for_status()
            for row in r.json().get("results", {}).get("bindings", []):
                work = row["work"]["value"]
                celex = row["same"]["value"].rsplit("/", 1)[-1]
                uuid = work.rsplit("/", 1)[-1]
                doc_id = next((d["doc_id"] for d in docs if d.get("cellar_uuid") == uuid), None)
                if doc_id and celex not in celex_to_doc:
                    celex_to_doc[celex] = doc_id
                    extra_celex_added += 1
        except Exception as e:
            print(f"  joined-case batch {i//BATCH+1} EXC: {e}")
    print(f"  +{extra_celex_added} joined-case CELEX aliases added")

    print(f"ID index: celex={len(celex_to_doc)} ecli={len(ecli_to_doc)} neutral={len(neutral_to_doc)} caseref={len(case_ref_to_doc)}")

    def resolve(side: str, kind: str, key: str) -> str | None:
        """Resolve a raw citation to target doc_id or None."""
        if side == "EU":
            if kind == "ECLI":
                return ecli_to_doc.get(key)
            if kind == "ALT_ECLI":
                # e.g. EU:C:1979:36 → ECLI:EU:C:1979:36
                full = "ECLI:" + key
                return ecli_to_doc.get(full)
            if kind in ("CASE_C", "CASE_OLD", "BARE_C", "BARE_T", "JOINED"):
                for cand in case_num_to_celex(key):
                    if cand in celex_to_doc:
                        return celex_to_doc[cand]
            if kind == "REGULATION":
                m = re.search(r"(\d+)/(\d{4})", key)
                if m:
                    n, y = m.groups()
                    cand = f"3{y}R{n.zfill(4)}"
                    return celex_to_doc.get(cand)
            return None  # ECR/CMLR/COMM_AT/COMM_COMP/COMM_DEC/AG_OPINION/FREE_V_COMM — too noisy or out-of-scope

        if side == "UK":
            n = normalize_uk_neutral(key)
            if n:
                return neutral_to_doc.get(n)
            n2 = normalize_cat_ref(key)
            if n2:
                return case_ref_to_doc.get(n2)
            return None
        return None

    # Aggregate edges
    edge_counts: Counter[tuple[str, str]] = Counter()
    n_total = 0
    n_resolved = 0
    n_self = 0

    with RAW_CITES.open() as f:
        for line in f:
            r = json.loads(line)
            n_total += 1
            tgt = resolve(r["side"], r["cite_target_kind"], r["cite_target_key"])
            if not tgt:
                continue
            src = r["source_doc_id"]
            if tgt == src:
                n_self += 1
                continue
            edge_counts[(src, tgt)] += 1
            n_resolved += 1

    print(f"Raw citations: {n_total}, resolved to graph edges: {n_resolved} ({100*n_resolved/n_total:.1f}%), self-cite skipped: {n_self}")

    # Write graph
    tmp = GRAPH_FILE.with_suffix(".jsonl.tmp")
    with tmp.open("w") as out:
        for (src, tgt), c in edge_counts.items():
            out.write(json.dumps({"source_doc_id": src, "target_doc_id": tgt, "count": c}) + "\n")
        out.flush()
        os.fsync(out.fileno())
    os.replace(tmp, GRAPH_FILE)

    # Compute per-doc weights
    cite_in: Counter[str] = Counter()
    cite_out: Counter[str] = Counter()
    for (src, tgt), c in edge_counts.items():
        cite_in[tgt] += c
        cite_out[src] += c

    # Update manifest
    augmented = []
    for d in docs:
        d["cite_in_count"] = cite_in.get(d["doc_id"], 0)
        d["cite_out_count"] = cite_out.get(d["doc_id"], 0)
        augmented.append(d)

    tmp = MANIFEST.with_suffix(".jsonl.tmp")
    with tmp.open("w") as f:
        for d in augmented:
            f.write(json.dumps(d, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, MANIFEST)

    print(f"\nWeights added to manifest. {GRAPH_FILE}")
    print()
    # Top-cited
    print("Top-15 most cited docs in corpus:")
    top_cited = cite_in.most_common(15)
    by_id = {d["doc_id"]: d for d in docs}
    for doc_id, cnt in top_cited:
        d = by_id.get(doc_id, {})
        title = (d.get("title") or "")[:80]
        ident = d.get("celex") or d.get("neutral_citation") or doc_id
        print(f"  {cnt:>5}× {ident:<25s} {title}")

    print()
    print("Distribution of cite_in_count:")
    bins = [0, 1, 2, 5, 10, 20, 50, 100, 1000]
    counts_in_bin = [0] * len(bins)
    for d in docs:
        c = cite_in.get(d["doc_id"], 0)
        for i in range(len(bins)-1, -1, -1):
            if c >= bins[i]:
                counts_in_bin[i] += 1
                break
    for i, b in enumerate(bins):
        next_b = bins[i+1] if i+1 < len(bins) else None
        label = f">={b}" if next_b is None else f"[{b},{next_b})"
        print(f"  {label:<12}: {counts_in_bin[i]} docs")


if __name__ == "__main__":
    main()
