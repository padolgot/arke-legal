"""
Pilot 2: count by fd_555 directory codes + enumerate subject-matter taxonomy.
Also: verify pre-1989 cases reachable via CELLAR REST (no WAF).
"""

from __future__ import annotations
import requests

ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"
UA = "arke-research/0.1 (sparql-probe2; ivankrylov684@gmail.com)"


def run(query: str, timeout: int = 90):
    r = requests.get(
        ENDPOINT,
        params={"query": query, "format": "application/sparql-results+json"},
        headers={"User-Agent": UA, "Accept": "application/sparql-results+json"},
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json()


# Step 6: count JUDG by competition-related fd_555 directory codes.
Q_FD555_COUNTS = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT ?code ?label (COUNT(DISTINCT ?work) AS ?n) WHERE {
  ?work cdm:work_has_resource-type <http://publications.europa.eu/resource/authority/resource-type/JUDG> .
  ?work cdm:resource_legal_is_about_concept_directory-code ?code .
  ?code skos:prefLabel ?label .
  FILTER(LANG(?label) = "en")
  FILTER(STRSTARTS(STR(?code), "http://publications.europa.eu/resource/authority/fd_555/08")
      || STRSTARTS(STR(?code), "http://publications.europa.eu/resource/authority/fd_555/072010")
      || STRSTARTS(STR(?code), "http://publications.europa.eu/resource/authority/fd_555/073010")
      || STRSTARTS(STR(?code), "http://publications.europa.eu/resource/authority/fd_555/074010")
      || STRSTARTS(STR(?code), "http://publications.europa.eu/resource/authority/fd_555/04104"))
}
GROUP BY ?code ?label
ORDER BY DESC(?n)
"""

# Step 7: enumerate ALL subject-matter values used by JUDG (top 50 by frequency).
# Will reveal whether there's a separate "cartels" / "agreements" code.
Q_ALL_SUBJ_MATTERS = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT ?subj ?label (COUNT(DISTINCT ?work) AS ?n) WHERE {
  ?work cdm:work_has_resource-type <http://publications.europa.eu/resource/authority/resource-type/JUDG> .
  ?work cdm:resource_legal_is_about_subject-matter ?subj .
  ?subj skos:prefLabel ?label .
  FILTER(LANG(?label) = "en")
}
GROUP BY ?subj ?label
ORDER BY DESC(?n)
LIMIT 80
"""

# Step 8: verify pre-1989 case reachable: get CELEX + work URI for Hoffmann-La Roche 85/76.
# Use ECLI lookup since we know ECLI:EU:C:1979:36 from our extracted citations.
Q_HOFFMANN_BY_ECLI = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
SELECT ?work ?celex ?ecli ?title WHERE {
  ?work cdm:resource_legal_id_celex ?celex .
  OPTIONAL { ?work cdm:case-law_ecli ?ecli . }
  OPTIONAL { ?work cdm:work_title ?title . FILTER(LANG(?title) = "en") }
  FILTER(?celex IN ("61976CJ0085", "61976J0085", "61981CJ0322", "61981J0322", "61976CJ0027", "61976J0027"))
}
"""

# Step 9: directory codes (full set of fd_555/08* — competition principles tree).
Q_FD555_08_TREE = """
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT ?code ?label WHERE {
  ?code skos:inScheme <http://publications.europa.eu/resource/authority/fd_555> .
  ?code skos:prefLabel ?label .
  FILTER(LANG(?label) = "en")
  FILTER(STRSTARTS(STR(?code), "http://publications.europa.eu/resource/authority/fd_555/08"))
}
ORDER BY ?code
"""


def show(label: str, query: str):
    print(f"\n=== {label} ===")
    try:
        result = run(query)
        bindings = result.get("results", {}).get("bindings", [])
        if not bindings:
            print("  (empty)")
            return
        head_vars = result.get("head", {}).get("vars", [])
        print(f"  {len(bindings)} rows")
        for row in bindings[:60]:
            display = {v: row.get(v, {}).get("value", "—")[:140] for v in head_vars}
            print(f"  {display}")
    except requests.HTTPError as e:
        print(f"  HTTP {e.response.status_code}: {e.response.text[:300]}")
    except Exception as e:
        print(f"  EXC {type(e).__name__}: {e}")


def main():
    show("STEP 6: JUDG counts by competition fd_555 codes", Q_FD555_COUNTS)
    show("STEP 7: top 80 subject-matters used by JUDG", Q_ALL_SUBJ_MATTERS)
    show("STEP 8: verify pre-1989 cases (Hoffmann/Michelin/United Brands)", Q_HOFFMANN_BY_ECLI)
    show("STEP 9: full fd_555/08* tree (competition principles)", Q_FD555_08_TREE)


if __name__ == "__main__":
    main()
