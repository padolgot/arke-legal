"""
Find available manifestations (file format renderings) per work via SPARQL.
For each work, SPARQL query returns expression URIs + their language + their manifestation file types.
"""
import requests

ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"
UA = "arke-research/0.1 (manifestation-probe; ivankrylov684@gmail.com)"


def sparql(q):
    r = requests.post(
        ENDPOINT,
        data={"query": q, "format": "application/sparql-results+json"},
        headers={"User-Agent": UA, "Accept": "application/sparql-results+json",
                 "Content-Type": "application/x-www-form-urlencoded"},
        timeout=120,
    )
    r.raise_for_status()
    return r.json()


# Query: list ALL English manifestations of a work
# Use a known case: Intel 2017 (62014CJ0413) which is failing on HTML
def query_for(celex):
    return f"""
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT ?work ?expr ?manif ?lang_id ?fmt_label WHERE {{
  ?work owl:sameAs <http://publications.europa.eu/resource/celex/{celex}> .
  ?expr cdm:expression_belongs_to_work ?work .
  OPTIONAL {{
    ?expr cdm:expression_uses_language ?lang .
    BIND(REPLACE(STR(?lang), "^.+/", "") AS ?lang_id)
  }}
  ?manif cdm:manifestation_manifests_expression ?expr .
  OPTIONAL {{
    ?manif cdm:manifestation_type ?fmt .
    BIND(STR(?fmt) AS ?fmt_label)
  }}
}}
"""


CASES = [
    ("Intel 2017", "62014CJ0413"),
    ("Hoffmann 1979", "61976CJ0085"),
    ("MasterCard 2014", "62012CJ0382"),
    ("Generics UK 2020", "62018CJ0307"),
]

for label, celex in CASES:
    print(f"\n=== {label}  CELEX={celex} ===")
    try:
        result = sparql(query_for(celex))
        bindings = result.get("results", {}).get("bindings", [])
        print(f"  {len(bindings)} manifestations")
        for b in bindings[:30]:
            d = {k: v["value"] for k, v in b.items()}
            print(f"    expr={d.get('expr','-').rsplit('/',1)[-1]:50s} "
                  f"lang={d.get('lang_id','?'):4s} fmt={d.get('fmt_label','-')[:40]}")
            print(f"      manif={d.get('manif','-')}")
    except Exception as e:
        print(f"  EXC: {e}")
