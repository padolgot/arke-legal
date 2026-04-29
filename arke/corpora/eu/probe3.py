"""
Probe 3: pre-1989 documents via PDF endpoint and CURIA.

Hypothesis: pre-1989 ECJ judgments are PDF-only on EUR-Lex; alternatively
CURIA (curia.europa.eu) has them via case-law document API.
"""

from __future__ import annotations
import time
import requests

UA = "arke-research/0.1 (probe3; ivankrylov684@gmail.com)"

TESTS = [
    (
        "85/76 PDF EUR-Lex",
        "https://eur-lex.europa.eu/legal-content/EN/TXT/PDF/?uri=CELEX:61976J0085",
    ),
    (
        "85/76 PDF AUTO",
        "https://eur-lex.europa.eu/legal-content/EN/TXT/PDF/?uri=CELEX:61976J0085&from=EN",
    ),
    (
        "27/76 PDF EUR-Lex",
        "https://eur-lex.europa.eu/legal-content/EN/TXT/PDF/?uri=CELEX:61976J0027",
    ),
    (
        "322/81 PDF EUR-Lex",
        "https://eur-lex.europa.eu/legal-content/EN/TXT/PDF/?uri=CELEX:61981J0322",
    ),
    # CURIA case-law search by case number
    (
        "85/76 CURIA juris-search",
        "https://curia.europa.eu/juris/liste.jsf?num=85%2F76&language=en",
    ),
    (
        "27/76 CURIA juris-search",
        "https://curia.europa.eu/juris/liste.jsf?num=27%2F76&language=en",
    ),
    (
        "322/81 CURIA juris-search",
        "https://curia.europa.eu/juris/liste.jsf?num=322%2F81&language=en",
    ),
    # Also try without /TXT/ — just /HTML/PDF
    (
        "85/76 HTML/PDF/",
        "https://eur-lex.europa.eu/legal-content/EN/HTML/PDF/?uri=CELEX:61976J0085",
    ),
    # CELLAR direct (resolves CELEX → cellar URI then fetch)
    (
        "85/76 cellar resolve",
        "http://publications.europa.eu/resource/celex/61976J0085",
    ),
]


def fetch(url, timeout=20):
    sess = requests.Session()
    try:
        r = sess.get(url, headers={"User-Agent": UA}, timeout=timeout, allow_redirects=True)
        return r
    except Exception as e:
        return None


def main():
    for label, url in TESTS:
        print(f"\n=== {label} ===")
        print(f"URL: {url}")
        r = fetch(url)
        if r is None:
            print("  ERR (exception)")
            continue
        ct = r.headers.get("content-type", "?").split(";")[0]
        size = len(r.content)
        print(f"  status={r.status_code} ct={ct} size={size}b final={r.url[:140]}")
        # Check first 8 bytes for PDF magic
        if r.content[:4] == b"%PDF":
            print("  ★ PDF MAGIC")
        elif b"<html" in r.content[:200].lower() or b"<!doctype" in r.content[:200].lower():
            # Check title
            import re
            t = re.search(rb"<title>(.*?)</title>", r.content, re.I | re.S)
            if t:
                print(f"  HTML title: {t.group(1).decode('utf8','replace').strip()[:160]}")
            # CURIA-specific: look for case rows
            if b"liste.jsf" in url.encode() or b"curia.europa.eu" in url.encode():
                # Find docid links in body
                docids = re.findall(rb'docid=(\d+)', r.content)
                if docids:
                    uniq = sorted(set(docids))
                    print(f"  CURIA docids found: {len(uniq)} unique → {[d.decode() for d in uniq[:8]]}")
                else:
                    print("  CURIA: no docids in body")
        time.sleep(1.0)


if __name__ == "__main__":
    main()
