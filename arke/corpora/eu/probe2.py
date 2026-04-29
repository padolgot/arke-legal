"""
Follow-up probe: investigate why pre-1989 CASE_NUM keys return 202/0-bytes.

Hypotheses:
  1. EUR-Lex async-renders some old documents — retry with delay.
  2. Pre-1989 CELEX needs different sector/format.
  3. Need Accept-Language header or different content-negotiation path.
  4. Document only exists in ECLI form, retrievable via redirect.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import requests

UA = "arke-research/0.1 (probe2; ivankrylov684@gmail.com)"

# Verified case: Hoffmann-La Roche 85/76 has known ECLI:EU:C:1979:36
# United Brands 27/76 → ECLI:EU:C:1978:22
# Michelin 322/81 → ECLI:EU:C:1983:313

TESTS = [
    # (label, url, headers)
    (
        "85/76 plain CELEX",
        "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:61976J0085",
        {},
    ),
    (
        "85/76 CELEX with AUTO lang",
        "https://eur-lex.europa.eu/legal-content/EN/AUTO/?uri=CELEX:61976J0085",
        {},
    ),
    (
        "85/76 CELEX with from=EN",
        "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:61976J0085&from=EN",
        {},
    ),
    (
        "85/76 with Accept-Language",
        "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:61976J0085",
        {"Accept-Language": "en-GB,en;q=0.9"},
    ),
    (
        "85/76 via ECLI form",
        "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=ecli:EU:C:1979:36",
        {},
    ),
    (
        "85/76 sector 1 (pre-Lisbon, treaty?)",
        "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:11976J0085",
        {},
    ),
    (
        "27/76 via ECLI",
        "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=ecli:EU:C:1978:22",
        {},
    ),
    (
        "322/81 via ECLI",
        "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=ecli:EU:C:1983:313",
        {},
    ),
    # Pure HTML page  (no /TXT/ — the document homepage)
    (
        "85/76 via /HTML/",
        "https://eur-lex.europa.eu/legal-content/EN/HTML/?uri=CELEX:61976J0085",
        {},
    ),
    # AUTO redirect
    (
        "85/76 AUTO with case-law SUM",
        "https://eur-lex.europa.eu/legal-content/EN/SUM/?uri=CELEX:61976J0085",
        {},
    ),
]


def fetch_with_retry(url: str, headers: dict, retries: int = 3, retry_pause: float = 3.0):
    sess = requests.Session()
    log = []
    for attempt in range(retries):
        h = {"User-Agent": UA, **headers}
        try:
            r = sess.get(url, headers=h, timeout=30, allow_redirects=True)
        except requests.RequestException as e:
            log.append(f"  attempt {attempt+1}: ERR {type(e).__name__}: {e}")
            return log, None
        size = len(r.content)
        log.append(
            f"  attempt {attempt+1}: status={r.status_code} size={size}b ct={r.headers.get('content-type','?').split(';')[0]} final={r.url[:120]}"
        )
        if r.status_code == 200 and size > 1000:
            return log, r
        if r.status_code == 202:
            time.sleep(retry_pause)
            continue
        return log, r
    return log, r


def main():
    for label, url, headers in TESTS:
        print(f"\n=== {label} ===")
        print(f"URL: {url}")
        if headers:
            print(f"Headers: {headers}")
        log, r = fetch_with_retry(url, headers)
        for line in log:
            print(line)
        if r is not None and r.status_code == 200 and len(r.content) > 1000:
            # Snippet of body — look for document title
            body = r.text
            # Find title or meta
            import re
            title = re.search(r"<title>(.*?)</title>", body, re.I | re.S)
            if title:
                print(f"  title: {title.group(1).strip()[:150]}")
            # Look for "no documents found"
            if "No documents matching" in body or "No documents found" in body:
                print("  *** PAGE SAYS NO DOCUMENTS ***")
            # Check for key phrase suggesting actual judgment
            if any(k in body for k in ["Hoffmann-La Roche", "United Brands", "Michelin"]):
                print("  >>> DOC TEXT PRESENT")
        time.sleep(1.0)


if __name__ == "__main__":
    main()
