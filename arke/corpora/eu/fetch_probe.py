"""
Probe how to fetch full document text from CELLAR REST.

CELLAR uses content negotiation. The same URI can return different formats
based on Accept header. Possible endpoints:
  - http://publications.europa.eu/resource/celex/{CELEX}
  - http://publications.europa.eu/resource/cellar/{UUID}

Try multiple Accept headers, log what comes back.
"""

from __future__ import annotations
import requests

UA = "arke-research/0.1 (fetch-probe; ivankrylov684@gmail.com)"

# Test cases — both pre-1989 and post-1989, since pre-1989 was the WAF problem.
TARGETS = [
    ("AKZO post-1989 ECJ", "http://publications.europa.eu/resource/celex/61986CJ0062"),
    ("Hoffmann-La Roche pre-1989", "http://publications.europa.eu/resource/celex/61976J0085"),
    ("Tetra Pak General Court", "http://publications.europa.eu/resource/celex/61991TJ0083"),
]

ACCEPTS = [
    "text/html",
    "text/html;type=simplified",
    "application/xhtml+xml",
    "text/plain",
    "application/xml;notice=branch",
    "application/xml;notice=tree",
    "application/pdf",
    "application/zip",
    "*/*",
]


def main():
    for label, uri in TARGETS:
        print(f"\n{'='*80}\n{label}: {uri}\n{'='*80}")
        for accept in ACCEPTS:
            try:
                r = requests.get(
                    uri,
                    headers={"User-Agent": UA, "Accept": accept},
                    timeout=30,
                    allow_redirects=True,
                )
                ct = r.headers.get("content-type", "?").split(";")[0]
                size = len(r.content)
                final_url = r.url[:120]
                # Detect content nature
                is_pdf = r.content[:4] == b"%PDF"
                is_zip = r.content[:2] == b"PK"
                is_html = b"<html" in r.content[:200].lower() or b"<!doctype" in r.content[:200].lower()
                marker = "PDF" if is_pdf else "ZIP" if is_zip else "HTML" if is_html else "?"
                print(f"  Accept={accept:35s} → {r.status_code} {ct:30s} {size:>9}b [{marker}]")
                if final_url != uri:
                    print(f"    redirected to: {final_url}")
            except requests.RequestException as e:
                print(f"  Accept={accept:35s} ERR {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
