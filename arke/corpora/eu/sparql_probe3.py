"""
Pilot 3: introspect CELLAR — find correct properties for CELEX, ECLI lookup.
"""
from __future__ import annotations
import requests

ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"
UA = "arke-research/0.1 (sparql-probe3; ivankrylov684@gmail.com)"


def run(query, timeout=60):
    r = requests.get(
        ENDPOINT,
        params={"query": query, "format": "application/sparql-results+json"},
        headers={"User-Agent": UA, "Accept": "application/sparql-results+json"},
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json()


# Find any JUDG and dump properties.
Q_DUMP = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
SELECT ?p ?o WHERE {
  ?work cdm:work_has_resource-type <http://publications.europa.eu/resource/authority/resource-type/JUDG> .
  ?work ?p ?o .
}
LIMIT 100
"""

# Find any work via the CELEX-as-URI route:
# URI pattern: http://publications.europa.eu/resource/celex/<CELEX>
Q_CELEX_URI = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
SELECT ?work ?p ?o WHERE {
  BIND(<http://publications.europa.eu/resource/celex/61986CJ0062> AS ?work)
  ?work ?p ?o .
}
LIMIT 50
"""

# Try owl:sameAs link
Q_SAMEAS = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT ?work ?other WHERE {
  ?work owl:sameAs <http://publications.europa.eu/resource/celex/61986CJ0062> .
  ?work owl:sameAs ?other .
} LIMIT 20
"""

# Search any prop containing CELEX 61986CJ0062 in object
Q_OBJ_CONTAINS = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
SELECT ?work ?p ?o WHERE {
  ?work ?p ?o .
  FILTER(CONTAINS(STR(?o), "61986CJ0062"))
}
LIMIT 30
"""

# Search ECLI:
Q_ECLI = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
SELECT ?work ?p ?o WHERE {
  ?work ?p ?o .
  FILTER(CONTAINS(STR(?o), "ECLI:EU:C:1979:36"))
}
LIMIT 30
"""


def show(label, q):
    print(f"\n=== {label} ===")
    try:
        r = run(q)
        rows = r.get("results", {}).get("bindings", [])
        if not rows:
            print("  (empty)")
            return
        head = r.get("head", {}).get("vars", [])
        print(f"  {len(rows)} rows")
        for row in rows[:50]:
            d = {v: row.get(v, {}).get("value", "—")[:130] for v in head}
            print(f"  {d}")
    except Exception as e:
        print(f"  EXC: {type(e).__name__}: {e}")


def main():
    show("DUMP one JUDG props", Q_DUMP)
    show("Resolve CELEX-as-URI: 61986CJ0062 (AKZO)", Q_CELEX_URI)
    show("Try owl:sameAs", Q_SAMEAS)
    show("FULL-text search CELEX 61986CJ0062", Q_OBJ_CONTAINS)
    show("FULL-text search ECLI:EU:C:1979:36", Q_ECLI)


if __name__ == "__main__":
    main()
