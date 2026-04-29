"""
Phase 1.5: for each work in works.jsonl, find best English manifestation URI.

Priority order: txt > html > xhtml > pdf > pdfa1a > fmx4
(plain text simplest; PDF needs pypdfium2; fmx4 = Formex, structured but heavy parse)

Outputs works_with_manif.jsonl — adds {english_manif_uri, format} per work.

Bulk SPARQL with VALUES — 100 works per call, ~20 calls total, ~5 minutes.
"""

from __future__ import annotations
import json
import os
from pathlib import Path

import requests
from arke.corpora._paths import DATA, ENV_FILE



ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"
UA = "arke-research/0.1 (list-manifestations; ivankrylov684@gmail.com)"
OUT_DIR = DATA / "eu_pool"
WORKS_IN = OUT_DIR / "works.jsonl"
WORKS_OUT = OUT_DIR / "works_with_manif.jsonl"

# Format preference. CELLAR txt manifestations are phantom (listed but no datastream).
# html / xhtml return real content via /DOC_1; pdf needs pypdfium2 to extract; fmx4 = Formex XML.
FORMAT_PRIORITY = {"html": 0, "xhtml": 1, "pdf": 2, "pdfa1a": 3, "fmx4": 4, "txt": 5, "print": 6}


def sparql(q: str, timeout: int = 180):
    r = requests.post(
        ENDPOINT,
        data={"query": q, "format": "application/sparql-results+json"},
        headers={"User-Agent": UA, "Accept": "application/sparql-results+json",
                 "Content-Type": "application/x-www-form-urlencoded"},
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json()


def query_for_works(work_uris: list[str]) -> str:
    values = " ".join(f"<{u}>" for u in work_uris)
    return f"""
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
SELECT ?work ?manif ?fmt WHERE {{
  VALUES ?work {{ {values} }}
  ?expr cdm:expression_belongs_to_work ?work .
  ?expr cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/ENG> .
  ?manif cdm:manifestation_manifests_expression ?expr .
  ?manif cdm:manifestation_type ?fmt .
}}
"""


def atomic_write(path: Path, lines: list[str]):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w") as f:
        for line in lines:
            f.write(line + "\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def main():
    works = []
    with WORKS_IN.open() as f:
        for line in f:
            works.append(json.loads(line))
    print(f"Loaded {len(works)} works", flush=True)

    work_uris = [w["work_uri"] for w in works]

    # Bulk fetch manifestations
    BATCH = 100
    manif_per_work: dict[str, list[tuple[str, str]]] = {u: [] for u in work_uris}
    for i in range(0, len(work_uris), BATCH):
        batch = work_uris[i:i+BATCH]
        print(f"  batch {i//BATCH+1}/{(len(work_uris)+BATCH-1)//BATCH} ({len(batch)})", flush=True)
        try:
            result = sparql(query_for_works(batch))
            for row in result.get("results", {}).get("bindings", []):
                w = row["work"]["value"]
                m = row["manif"]["value"]
                f = row["fmt"]["value"].rsplit("/", 1)[-1].lower()
                manif_per_work.setdefault(w, []).append((m, f))
        except Exception as e:
            print(f"    EXC {type(e).__name__}: {e}", flush=True)

    # Pick best manifestation per work
    out_lines = []
    n_with = 0
    fmt_counts: dict[str, int] = {}
    no_manif: list[str] = []
    for w in works:
        cands = manif_per_work.get(w["work_uri"], [])
        # Sort by priority; unknown formats sorted last
        cands.sort(key=lambda x: FORMAT_PRIORITY.get(x[1], 99))
        if cands:
            manif_uri, fmt = cands[0]
            w["english_manif_uri"] = manif_uri
            w["english_manif_format"] = fmt
            n_with += 1
            fmt_counts[fmt] = fmt_counts.get(fmt, 0) + 1
        else:
            w["english_manif_uri"] = None
            w["english_manif_format"] = None
            no_manif.append(w.get("celex") or w["work_uri"])
        out_lines.append(json.dumps(w, ensure_ascii=False))

    atomic_write(WORKS_OUT, out_lines)
    print(f"\nResults:")
    print(f"  works with English manifestation: {n_with}/{len(works)}")
    print(f"  format distribution: {fmt_counts}")
    print(f"  works WITHOUT English manifestation: {len(no_manif)}")
    if no_manif[:10]:
        for x in no_manif[:10]:
            print(f"    - {x}")


if __name__ == "__main__":
    main()
