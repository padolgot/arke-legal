"""
EUR-Lex channel probe.

Goal: determine which of these URL channels work in 2026, what response codes
they return, what content-type, and which one resolves to a fetchable
document for each key_kind in eu_clusters_phaseB.jsonl.

Channels tested per sample:
  1. ECLI direct          https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=ecli:{ecli}
  2. CELEX guess          https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{guess}
  3. Search by case-num   https://eur-lex.europa.eu/search.html?CASE_LAW_NUMBER={num}
  4. ECLI in CELEX-form   via /legal-content/EN/AUTO/?uri=ecli:{ecli}
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from urllib.parse import quote

import requests

UA = "arke-research/0.1 (probe; ivankrylov684@gmail.com)"

SAMPLES = [
    # (canonical_key, key_kind, expected_form)
    ("85/76", "CASE_NUM", "Hoffmann-La Roche (1976 ECJ)"),
    ("322/81", "CASE_NUM", "Michelin (1981 ECJ)"),
    ("27/76", "CASE_NUM", "United Brands"),
    ("C-62/86", "CASE_NUM", "AKZO (post-1989 prefix)"),
    ("T-83/91", "CASE_NUM", "Tetra Pak (General Court)"),
    ("ECLI:EU:C:2014:2201", "ECLI", "Huawei v ZTE"),
    ("ECLI:EU:C:1979:36", "ECLI", "Hoffmann-La Roche ECLI"),
    ("AT.39824", "AT", "Trucks cartel"),
    ("COMP/34.579", "COMP", "MasterCard COMP"),
    ("1/2003", "REGULATION", "Modernisation regulation"),
    ("2007/98/EC", "DECISION", "Commission decision"),
]


def celex_from_case_num(num: str) -> list[str]:
    """Generate CELEX guesses for a case number like '85/76' or 'C-62/86'."""
    m = re.match(r"^(?:([CT])-)?(\d+)/(\d{2,4})$", num)
    if not m:
        return []
    prefix, n, yr = m.groups()
    if len(yr) == 2:
        yr_full = "19" + yr if int(yr) >= 50 else "20" + yr
    else:
        yr_full = yr
    n_pad = n.zfill(4)
    if prefix == "T":
        # General Court: CELEX letter "A" (judgment) or "B" (order)
        return [f"6{yr_full}TJ{n_pad}", f"6{yr_full}A{n_pad}", f"6{yr_full}B{n_pad}"]
    else:
        # Court of Justice: J = judgment, O = order, A = AG opinion
        return [f"6{yr_full}CJ{n_pad}", f"6{yr_full}J{n_pad}", f"6{yr_full}O{n_pad}"]


def fetch(url: str, timeout: int = 15) -> tuple[int, str, int]:
    """Returns (status, content-type, content-length-bytes-prefix)."""
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout, allow_redirects=True)
        ct = r.headers.get("content-type", "")
        # Probe content: does the page contain actual document text or "no documents"?
        body = r.text[:50000]
        has_no_results = (
            "No documents matching the search criteria"
            in body
            or "Search results: 0" in body
            or "No documents found" in body
        )
        size = len(r.content)
        marker = "EMPTY" if has_no_results else "OK"
        return r.status_code, ct.split(";")[0], size, marker, r.url
    except requests.RequestException as e:
        return 0, f"err:{type(e).__name__}", 0, "ERR", url


def probe_one(key: str, kind: str, label: str) -> dict:
    out = {"key": key, "kind": kind, "label": label, "channels": []}

    if kind == "CASE_NUM":
        for celex_guess in celex_from_case_num(key):
            url = f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{celex_guess}"
            r = fetch(url)
            out["channels"].append({"channel": f"CELEX:{celex_guess}", "url": url, "result": r})
            time.sleep(0.5)
        # search fallback
        url = f"https://eur-lex.europa.eu/search.html?CASE_LAW_NUMBER={quote(key)}"
        r = fetch(url)
        out["channels"].append({"channel": "search-by-num", "url": url, "result": r})

    elif kind == "ECLI":
        ecli = key
        url = f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=ecli:{quote(ecli)}"
        r = fetch(url)
        out["channels"].append({"channel": "ECLI-direct", "url": url, "result": r})

    elif kind == "AT":
        num = key.replace("AT.", "")
        url = f"https://eur-lex.europa.eu/search.html?text={quote('AT.' + num)}"
        r = fetch(url)
        out["channels"].append({"channel": "AT-search", "url": url, "result": r})
        # Also try direct case page
        url2 = f"https://ec.europa.eu/competition/elojade/isef/case_details.cfm?proc_code=1_{num}"
        r2 = fetch(url2)
        out["channels"].append({"channel": "DG-COMP-page", "url": url2, "result": r2})

    elif kind == "COMP":
        num = key.replace("COMP/", "")
        url = f"https://eur-lex.europa.eu/search.html?text={quote('COMP/' + num)}"
        r = fetch(url)
        out["channels"].append({"channel": "COMP-search", "url": url, "result": r})

    elif kind == "REGULATION":
        m = re.match(r"^(\d+)/(\d{4})$", key)
        if m:
            n, yr = m.groups()
            n_pad = n.zfill(4)
            celex = f"3{yr}R{n_pad}"
            url = f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{celex}"
            r = fetch(url)
            out["channels"].append({"channel": f"CELEX:{celex}", "url": url, "result": r})

    elif kind == "DECISION":
        m = re.match(r"^(\d{4})/(\d+)/(EC|EEC|EU)$", key)
        if m:
            yr, n, suf = m.groups()
            n_pad = n.zfill(4)
            celex = f"3{yr}D{n_pad}"
            url = f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{celex}"
            r = fetch(url)
            out["channels"].append({"channel": f"CELEX:{celex}", "url": url, "result": r})

    return out


def main():
    out_path = Path(__file__).parent / "probe_results.jsonl"
    with out_path.open("w") as f:
        for key, kind, label in SAMPLES:
            print(f"\n=== {key} ({kind}) — {label} ===", flush=True)
            res = probe_one(key, kind, label)
            f.write(json.dumps(res) + "\n")
            f.flush()
            for ch in res["channels"]:
                status, ct, size, marker, final = ch["result"]
                print(f"  {ch['channel']:32s}  → {status} {ct:30s} {size:>7}b {marker}", flush=True)
                if final != ch["url"]:
                    print(f"    redirected to: {final[:120]}", flush=True)
            time.sleep(1.5)


if __name__ == "__main__":
    main()
