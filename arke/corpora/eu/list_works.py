"""
Phase 1: pull complete list of works in our scope from CELLAR.

Scope: JUDG + OPIN_AG, tagged with any of {ENTR, POSI, PRAT, EXCL}.

Output: <umbrella>/corpora/eu_pool/works.jsonl
  one line per work: {cellar_uuid, celex, ecli, title, date, resource_type, subject_matters}
"""

from __future__ import annotations
import json
import os
import sys
from pathlib import Path

import requests
from arke.corpora._paths import DATA, ENV_FILE



ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"
UA = "arke-research/0.1 (list-works; ivankrylov684@gmail.com)"

OUT_DIR = DATA / "eu_pool"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT_DIR / "works.jsonl"

SCOPE_SUBJECT_BROAD = "http://publications.europa.eu/resource/authority/subject-matter/CONC"
EXCLUDE_SUBJECTS = [
    "http://publications.europa.eu/resource/authority/subject-matter/AIDE",   # State aids
    "http://publications.europa.eu/resource/authority/subject-matter/MARC",   # Public procurement
    "http://publications.europa.eu/resource/authority/subject-matter/MERG",   # Concentrations / mergers
]
RESOURCE_TYPES = [
    "http://publications.europa.eu/resource/authority/resource-type/JUDG",
    "http://publications.europa.eu/resource/authority/resource-type/OPIN_AG",
]


def sparql_post(query: str, timeout: int = 180):
    r = requests.post(
        ENDPOINT,
        data={"query": query, "format": "application/sparql-results+json"},
        headers={"User-Agent": UA, "Accept": "application/sparql-results+json",
                 "Content-Type": "application/x-www-form-urlencoded"},
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json()


def build_query() -> str:
    rt_vals = " ".join(f"<{u}>" for u in RESOURCE_TYPES)
    excl_vals = " ".join(f"<{u}>" for u in EXCLUDE_SUBJECTS)
    return f"""
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT ?work ?celex ?ecli ?type ?date
       (GROUP_CONCAT(DISTINCT ?subj_label; SEPARATOR="|") AS ?subjects)
       (SAMPLE(?title) AS ?one_title)
WHERE {{
  VALUES ?type {{ {rt_vals} }}
  ?work cdm:work_has_resource-type ?type .
  ?work cdm:resource_legal_is_about_subject-matter <{SCOPE_SUBJECT_BROAD}> .
  FILTER NOT EXISTS {{
    ?work cdm:resource_legal_is_about_subject-matter ?bad .
    VALUES ?bad {{ {excl_vals} }}
  }}
  OPTIONAL {{
    ?work cdm:resource_legal_is_about_subject-matter ?subj .
    ?subj skos:prefLabel ?subj_label .
    FILTER(LANG(?subj_label) = "en")
  }}

  OPTIONAL {{
    ?work owl:sameAs ?celex_uri .
    FILTER(STRSTARTS(STR(?celex_uri), "http://publications.europa.eu/resource/celex/"))
    BIND(REPLACE(STR(?celex_uri), "^.+/celex/", "") AS ?celex)
  }}
  OPTIONAL {{
    ?work owl:sameAs ?ecli_uri .
    FILTER(STRSTARTS(STR(?ecli_uri), "http://publications.europa.eu/resource/ecli/"))
    BIND(REPLACE(STR(?ecli_uri), "^.+/ecli/", "") AS ?ecli_raw)
    BIND(REPLACE(?ecli_raw, "%3A", ":") AS ?ecli)
  }}
  OPTIONAL {{
    ?work cdm:work_date_document ?date .
  }}
  OPTIONAL {{
    ?expr cdm:expression_belongs_to_work ?work .
    ?expr cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/ENG> .
    ?expr cdm:expression_title ?title .
  }}
}}
GROUP BY ?work ?celex ?ecli ?type ?date
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
    print(f"Querying CELLAR — this may take 60-120s for ~1,900 works...")
    query = build_query()
    result = sparql_post(query, timeout=300)
    bindings = result.get("results", {}).get("bindings", [])
    print(f"  got {len(bindings)} rows")

    rows = []
    for b in bindings:
        work_uri = b["work"]["value"]
        cellar_uuid = work_uri.rsplit("/", 1)[-1] if "/cellar/" in work_uri else None
        rt_uri = b.get("type", {}).get("value", "")
        rt_short = rt_uri.rsplit("/", 1)[-1] if rt_uri else None
        subjects = b.get("subjects", {}).get("value", "").split("|") if b.get("subjects") else []
        row = {
            "work_uri": work_uri,
            "cellar_uuid": cellar_uuid,
            "celex": b.get("celex", {}).get("value"),
            "ecli": b.get("ecli", {}).get("value"),
            "resource_type": rt_short,
            "date": b.get("date", {}).get("value"),
            "title": b.get("one_title", {}).get("value"),
            "subject_matters": [s for s in subjects if s],
        }
        rows.append(row)

    # Dedup by work_uri (should already be deduped via GROUP BY but double-check)
    seen = set()
    uniq = []
    for r in rows:
        if r["work_uri"] not in seen:
            seen.add(r["work_uri"])
            uniq.append(r)

    print(f"  unique works: {len(uniq)}")
    # Stats
    rt_counts: dict[str, int] = {}
    has_celex = 0
    has_ecli = 0
    for r in uniq:
        rt_counts[r["resource_type"]] = rt_counts.get(r["resource_type"], 0) + 1
        if r["celex"]: has_celex += 1
        if r["ecli"]:  has_ecli += 1
    print(f"  by resource_type: {rt_counts}")
    print(f"  with CELEX: {has_celex}, with ECLI: {has_ecli}")

    lines = [json.dumps(r, ensure_ascii=False) for r in uniq]
    atomic_write(OUT_FILE, lines)
    print(f"  wrote {OUT_FILE}")


if __name__ == "__main__":
    main()
