"""
Exact count of artery: JUDG + OPIN_AG tagged with one of 4 subject-matters.
"""
from __future__ import annotations
import requests

ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"
UA = "arke-research/0.1 (count; ivankrylov684@gmail.com)"


def run(query, timeout=120):
    r = requests.post(
        ENDPOINT,
        data={"query": query, "format": "application/sparql-results+json"},
        headers={"User-Agent": UA, "Accept": "application/sparql-results+json",
                 "Content-Type": "application/x-www-form-urlencoded"},
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json()


# First find URI for "Exclusive agreements" subject-matter.
Q_FIND_EXCL = """
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT ?subj ?label WHERE {
  ?subj skos:inScheme <http://publications.europa.eu/resource/authority/subject-matter> .
  ?subj skos:prefLabel ?label .
  FILTER(LANG(?label) = "en")
  FILTER(REGEX(STR(?label), "exclusive|agreement|concerted|dominant|exemption", "i"))
}
ORDER BY ?label
"""

# Subject-matter URIs we care about (will fill in after finding Exclusive)
SCOPE_URIS = [
    "http://publications.europa.eu/resource/authority/subject-matter/ENTR",  # Agreements/concerted
    "http://publications.europa.eu/resource/authority/subject-matter/POSI",  # Dominant position
    "http://publications.europa.eu/resource/authority/subject-matter/PRAT",  # Concerted practices
    # Exclusive agreements — to be added below
]

RESOURCE_TYPES = [
    "http://publications.europa.eu/resource/authority/resource-type/JUDG",
    "http://publications.europa.eu/resource/authority/resource-type/OPIN_AG",
]


def show(label, q):
    print(f"\n=== {label} ===")
    r = run(q)
    rows = r.get("results", {}).get("bindings", [])
    head = r.get("head", {}).get("vars", [])
    print(f"  {len(rows)} rows")
    for row in rows[:30]:
        d = {v: row.get(v, {}).get("value", "—")[:120] for v in head}
        print(f"  {d}")
    return rows


def count_query(subj_uris, rtype_uris):
    subj_vals = " ".join(f"<{u}>" for u in subj_uris)
    rt_vals = " ".join(f"<{u}>" for u in rtype_uris)
    return f"""
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
SELECT (COUNT(DISTINCT ?work) AS ?n) WHERE {{
  VALUES ?type {{ {rt_vals} }}
  VALUES ?subj {{ {subj_vals} }}
  ?work cdm:work_has_resource-type ?type .
  ?work cdm:resource_legal_is_about_subject-matter ?subj .
}}
"""


def count_per_subject(subj_uris, rtype_uris):
    subj_vals = " ".join(f"<{u}>" for u in subj_uris)
    rt_vals = " ".join(f"<{u}>" for u in rtype_uris)
    return f"""
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT ?subj ?label (COUNT(DISTINCT ?work) AS ?n) WHERE {{
  VALUES ?type {{ {rt_vals} }}
  VALUES ?subj {{ {subj_vals} }}
  ?work cdm:work_has_resource-type ?type .
  ?work cdm:resource_legal_is_about_subject-matter ?subj .
  ?subj skos:prefLabel ?label .
  FILTER(LANG(?label) = "en")
}}
GROUP BY ?subj ?label
ORDER BY DESC(?n)
"""


def main():
    rows = show("Find 'Exclusive agreements' subject-matter URI", Q_FIND_EXCL)
    excl_uri = None
    for row in rows:
        lbl = row.get("label", {}).get("value", "")
        if lbl.lower().strip() == "exclusive agreements":
            excl_uri = row.get("subj", {}).get("value")
            break

    full_scope = list(SCOPE_URIS)
    if excl_uri:
        full_scope.append(excl_uri)
        print(f"\nExclusive agreements URI: {excl_uri}")
    else:
        print("\nNo exact 'Exclusive agreements' match — proceeding with 3 subject-matters.")

    print("\nSubject-matter URIs in scope:")
    for u in full_scope:
        print(f"  {u}")

    # JUDG only
    show("JUDG count per subject-matter", count_per_subject(full_scope, RESOURCE_TYPES[:1]))
    show("Total UNIQUE JUDG across all 4 subject-matters", count_query(full_scope, RESOURCE_TYPES[:1]))

    # JUDG + OPIN_AG
    show("Total UNIQUE (JUDG + OPIN_AG) across all 4 subject-matters", count_query(full_scope, RESOURCE_TYPES))

    # Just OPIN_AG
    show("OPIN_AG only count", count_query(full_scope, RESOURCE_TYPES[1:]))


if __name__ == "__main__":
    main()
