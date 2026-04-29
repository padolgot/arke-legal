"""
CELLAR SPARQL pilot.

Goals:
  1. Confirm public endpoint reachable from our infra (no WAF).
  2. Inspect subject-matter classifications used for case-law.
  3. Count judgments by relevant competition subdomains.
  4. Reality-check artery size before committing to a fetch plan.
"""

from __future__ import annotations
import json
import urllib.parse

import requests

ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"
UA = "arke-research/0.1 (sparql-probe; ivankrylov684@gmail.com)"


def run(query: str, timeout: int = 60):
    params = {"query": query, "format": "application/sparql-results+json"}
    r = requests.get(
        ENDPOINT,
        params=params,
        headers={"User-Agent": UA, "Accept": "application/sparql-results+json"},
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json()


# Step 1: simplest possible SPARQL — sanity check.
Q_SANITY = """
SELECT (COUNT(*) AS ?n) WHERE {
  ?s ?p ?o .
} LIMIT 1
"""

# Step 2: list resource types used by case-law-ish docs (sample, not full).
# This shows us the CDM types we can filter on.
Q_RESOURCE_TYPES = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
SELECT DISTINCT ?type WHERE {
  ?work cdm:work_has_resource-type ?type .
  FILTER(REGEX(STR(?type), "JUDG|ORDER|OPIN|RULING", "i"))
}
"""

# Step 3: count Court of Justice + General Court judgments total.
Q_JUDG_COUNT = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
SELECT (COUNT(DISTINCT ?work) AS ?n) WHERE {
  ?work cdm:work_has_resource-type <http://publications.europa.eu/resource/authority/resource-type/JUDG> .
}
"""

# Step 4: list directory codes (subject-matter taxonomy used by case-law).
# These are pre-defined classifications EU uses for legal areas.
# Looking for "competition" related ones.
Q_DIRCODES = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT DISTINCT ?code ?label WHERE {
  ?code skos:inScheme <http://publications.europa.eu/resource/authority/fd_555> .
  ?code skos:prefLabel ?label .
  FILTER(LANG(?label) = "en")
  FILTER(REGEX(STR(?label), "competition|cartel|dominant|antitrust|merger|state aid", "i"))
}
"""

# Step 5: count judgments tagged with directory-code 08* (Competition policy)
# Using the CDM property cdm:resource_legal_is_about_subject-matter
Q_COMP_TAG = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT ?subj ?label (COUNT(DISTINCT ?work) AS ?n) WHERE {
  ?work cdm:work_has_resource-type <http://publications.europa.eu/resource/authority/resource-type/JUDG> .
  ?work cdm:resource_legal_is_about_subject-matter ?subj .
  ?subj skos:prefLabel ?label .
  FILTER(LANG(?label) = "en")
  FILTER(REGEX(STR(?label), "competition|cartel|dominant|antitrust", "i"))
}
GROUP BY ?subj ?label
ORDER BY DESC(?n)
"""


def show(label: str, query: str):
    print(f"\n=== {label} ===")
    try:
        result = run(query)
        bindings = result.get("results", {}).get("bindings", [])
        if not bindings:
            print("  (empty result set)")
            return
        head_vars = result.get("head", {}).get("vars", [])
        print(f"  vars: {head_vars}  rows: {len(bindings)}")
        for row in bindings[:20]:
            display = {v: row.get(v, {}).get("value", "—")[:120] for v in head_vars}
            print(f"  {display}")
        if len(bindings) > 20:
            print(f"  ... +{len(bindings)-20} more rows")
    except requests.HTTPError as e:
        print(f"  HTTP {e.response.status_code}: {e.response.text[:300]}")
    except Exception as e:
        print(f"  EXC {type(e).__name__}: {e}")


def main():
    show("STEP 1: sanity check (any triple count)", Q_SANITY)
    show("STEP 2: resource types matching JUDG/ORDER/OPIN/RULING", Q_RESOURCE_TYPES)
    show("STEP 3: total JUDG count", Q_JUDG_COUNT)
    show("STEP 4: directory codes (fd_555) matching competition keywords", Q_DIRCODES)
    show("STEP 5: judgments tagged with competition-related subject-matters", Q_COMP_TAG)


if __name__ == "__main__":
    main()
