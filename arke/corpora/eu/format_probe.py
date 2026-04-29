"""
Test which formats are available across years for our 4 subject-matters.
For each test case, try: HTML, PDF (default), plain text via .ENG.txt expression URI.
"""
from __future__ import annotations
from urllib.parse import quote
import requests

UA = "arke-research/0.1 (format-probe; ivankrylov684@gmail.com)"

# Test cases spanning era + court + type
CASES = [
    # (label, celex, ecli)
    ("Hoffmann-La Roche 1979 ECJ", "61976CJ0085", "ECLI:EU:C:1979:36"),
    ("United Brands 1978 ECJ",     "61976CJ0027", "ECLI:EU:C:1978:22"),
    ("AKZO 1991 ECJ",              "61986CJ0062", "ECLI:EU:C:1991:286"),
    ("Tetra Pak 1994 GC",          "61991TJ0083", "ECLI:EU:T:1994:246"),
    ("MasterCard 2014 ECJ",        "62012CJ0382", "ECLI:EU:C:2014:2201"),
    ("Intel 2017 ECJ",             "62014CJ0413", "ECLI:EU:C:2017:632"),
    ("Generics UK 2020 ECJ",       "62018CJ0307", "ECLI:EU:C:2020:52"),
    ("AG Opinion Hoffmann",        None,          "ECLI:EU:C:1978:202"),  # AG opinion
]

ENDPOINTS_TO_TRY = [
    ("CELEX HTML",        "Accept: text/html",                     "http://publications.europa.eu/resource/celex/{CELEX}"),
    ("CELEX PDF",         "Accept: application/pdf",               "http://publications.europa.eu/resource/celex/{CELEX}"),
    ("ECLI HTML",         "Accept: text/html",                     "http://publications.europa.eu/resource/ecli/{ECLI_Q}"),
    ("ECLI PDF",          "Accept: application/pdf",               "http://publications.europa.eu/resource/ecli/{ECLI_Q}"),
    ("ECLI .ENG.txt",     "Accept: text/plain",                    "http://publications.europa.eu/resource/ecli/{ECLI_Q}.ENG.txt"),
    ("CELEX .ENG.txt",    "Accept: text/plain",                    "http://publications.europa.eu/resource/celex/{CELEX}.ENG.txt"),
]


def fetch(url, accept):
    try:
        r = requests.get(url, headers={"User-Agent": UA, "Accept": accept},
                         timeout=60, allow_redirects=True)
        return r.status_code, len(r.content), r.headers.get("content-type", "?").split(";")[0], r.content[:200]
    except Exception as e:
        return -1, 0, f"err:{type(e).__name__}", b""


def main():
    for label, celex, ecli in CASES:
        ecli_q = quote(ecli, safe="") if ecli else None
        print(f"\n{'='*80}\n{label}  (CELEX={celex}, ECLI={ecli})\n{'='*80}")
        for ename, accept, url_tpl in ENDPOINTS_TO_TRY:
            if "{CELEX}" in url_tpl and not celex:
                continue
            if "{ECLI_Q}" in url_tpl and not ecli_q:
                continue
            url = url_tpl.replace("{CELEX}", celex or "").replace("{ECLI_Q}", ecli_q or "")
            accept_val = accept.split(": ", 1)[1]
            status, size, ct, snippet = fetch(url, accept_val)
            ok = "★" if status == 200 and size > 5000 else " "
            print(f"  {ok} {ename:20s}  → {status:>3} {ct:30s} {size:>9}b")


if __name__ == "__main__":
    main()
