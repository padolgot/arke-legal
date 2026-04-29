"""
Probe 4: BAILII as alternate channel for pre-1989 ECJ judgments.

BAILII hosts UK + EU case law in stable HTML. URL patterns vary; try several.
Also try EUR-Lex with very long retry interval (assume async cellar render).
"""

from __future__ import annotations
import time
import requests

UA_BROWSER = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

TESTS = [
    # BAILII patterns (year = year of judgment, not registration)
    # Hoffmann-La Roche 85/76 → judgment 1979
    ("BAILII 85/76 (1979)", "https://www.bailii.org/eu/cases/EUECJ/1979/C8576.html"),
    ("BAILII 85/76 underscore", "https://www.bailii.org/eu/cases/EUECJ/1979/C-85-76.html"),
    ("BAILII 85/76 raw num", "https://www.bailii.org/eu/cases/EUECJ/1979/85_76.html"),
    # United Brands 27/76 → 1978
    ("BAILII 27/76 (1978) C2776", "https://www.bailii.org/eu/cases/EUECJ/1978/C2776.html"),
    # Michelin 322/81 → 1983
    ("BAILII 322/81 (1983) C32281", "https://www.bailii.org/eu/cases/EUECJ/1983/C32281.html"),
    # Browse year index
    ("BAILII 1979 index", "https://www.bailii.org/eu/cases/EUECJ/1979/"),
    # AKZO C-62/86 (we know EUR-Lex works for this; sanity-check BAILII has it too)
    ("BAILII AKZO C-62/86 (1991)", "https://www.bailii.org/eu/cases/EUECJ/1991/C6286.html"),
]


def fetch(url, ua=UA_BROWSER, timeout=20):
    try:
        return requests.get(url, headers={"User-Agent": ua}, timeout=timeout, allow_redirects=True)
    except Exception as e:
        print(f"  EXC: {e}")
        return None


def main():
    for label, url in TESTS:
        print(f"\n=== {label} ===")
        print(f"URL: {url}")
        r = fetch(url)
        if r is None:
            continue
        ct = r.headers.get("content-type", "?").split(";")[0]
        size = len(r.content)
        print(f"  status={r.status_code} ct={ct} size={size}b")
        if r.status_code == 200 and size > 1000:
            import re
            t = re.search(rb"<title>(.*?)</title>", r.content, re.I | re.S)
            if t:
                print(f"  title: {t.group(1).decode('utf8','replace').strip()[:160]}")
            # snippet
            body_clean = re.sub(rb"<[^>]+>", b" ", r.content[:5000])
            body_clean = re.sub(rb"\s+", b" ", body_clean).strip()
            print(f"  snippet: {body_clean[:300].decode('utf8','replace')}")
        time.sleep(0.7)

    # Test EUR-Lex retry with LONG wait — assume async render
    print("\n\n=== EUR-Lex long-wait retry: 85/76 ===")
    sess = requests.Session()
    sess.headers.update({"User-Agent": UA_BROWSER})
    url = "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:61976J0085"
    for wait in [5, 15, 30]:
        print(f"  Sleeping {wait}s before request...")
        time.sleep(wait)
        r = sess.get(url, timeout=30, allow_redirects=True)
        print(f"  status={r.status_code} size={len(r.content)}b ct={r.headers.get('content-type','?').split(';')[0]}")
        if r.status_code == 200 and len(r.content) > 100000:
            print("  SUCCESS — got real document")
            break


if __name__ == "__main__":
    main()
