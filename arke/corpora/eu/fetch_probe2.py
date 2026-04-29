"""
Probe 2: pre-1989 cases via cellar UUID + via ECLI URI.

For Hoffmann-La Roche, we know ECLI:EU:C:1979:36 was matched in stress-test
via owl:sameAs to http://publications.europa.eu/resource/ecli/ECLI%3AEU%3AC%3A1979%3A36.
So that ECLI form WORKS in CELLAR. Question: does fetching that URI
return content?

Also: get cellar UUID for Hoffmann-La Roche via SPARQL, then fetch by UUID.
"""

from __future__ import annotations
from urllib.parse import quote
import requests

UA = "arke-research/0.1 (fetch-probe2; ivankrylov684@gmail.com)"
ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"


def sparql(query):
    r = requests.post(
        ENDPOINT,
        data={"query": query, "format": "application/sparql-results+json"},
        headers={"User-Agent": UA, "Accept": "application/sparql-results+json",
                 "Content-Type": "application/x-www-form-urlencoded"},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


# Step 1: get cellar UUID + canonical CELEX for Hoffmann-La Roche via its ECLI.
ecli = "ECLI:EU:C:1979:36"
ecli_uri = f"http://publications.europa.eu/resource/ecli/{quote(ecli, safe='')}"
Q = f"""
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT ?work ?other WHERE {{
  ?work owl:sameAs <{ecli_uri}> .
  ?work owl:sameAs ?other .
}}
"""

print("=== SPARQL: find Hoffmann-La Roche by ECLI ===")
result = sparql(Q)
for row in result.get("results", {}).get("bindings", []):
    print(f"  work: {row['work']['value']}")
    print(f"  sameAs: {row['other']['value']}")

# Step 2: try fetching via different URIs
TARGETS = [
    ("ECLI URI direct", ecli_uri),
    # Cellar UUID URI (extracted manually from above output if exists)
    # Re-extract programmatically:
]

# Get the cellar UUID
cellar_uuid = None
celex_canonical = None
for row in result.get("results", {}).get("bindings", []):
    work = row['work']['value']
    if "/cellar/" in work and not cellar_uuid:
        cellar_uuid = work.split("/cellar/")[-1]
    other = row['other']['value']
    if "/celex/" in other and not celex_canonical:
        celex_canonical = other.split("/celex/")[-1]

print(f"\n  cellar_uuid: {cellar_uuid}")
print(f"  celex (from owl:sameAs): {celex_canonical}")

if cellar_uuid:
    TARGETS.append(("CELLAR UUID direct", f"http://publications.europa.eu/resource/cellar/{cellar_uuid}"))
if celex_canonical:
    TARGETS.append(("CELEX URI (from sameAs)", f"http://publications.europa.eu/resource/celex/{celex_canonical}"))

# Step 3: probe each URI with relevant Accepts
print("\n=== Fetch attempts ===")
for label, uri in TARGETS:
    print(f"\n--- {label}: {uri} ---")
    for accept in [
        "application/xml;notice=tree",
        "application/xml;notice=branch",
        "text/html",
        "*/*",
    ]:
        try:
            r = requests.get(uri, headers={"User-Agent": UA, "Accept": accept}, timeout=60, allow_redirects=True)
            ct = r.headers.get("content-type", "?").split(";")[0]
            size = len(r.content)
            print(f"  Accept={accept:35s} → {r.status_code} {ct:30s} {size:>9}b")
            if final := (r.url if r.url != uri else None):
                print(f"    final: {final[:140]}")
        except Exception as e:
            print(f"  Accept={accept:35s} ERR {type(e).__name__}: {e}")
