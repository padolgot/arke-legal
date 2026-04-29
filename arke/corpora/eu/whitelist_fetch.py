"""
Fetch whitelist of CAT-cited but missing-from-scope works.

For each cluster (CELEX / ECLI / REGULATION key), bypass subject-matter scope
and fetch directly by CELEX URI. Use the same manifestation lookup +
download pipeline. Save to eu_pool/ with via=whitelist marker.
"""

from __future__ import annotations
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import quote

import requests
from arke.corpora._paths import DATA, ENV_FILE



ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"
UA = "arke-research/0.1 (whitelist; ivankrylov684@gmail.com)"
OUT_DIR = DATA / "eu_pool"
RAW_DIR = OUT_DIR / "raw"
WHITELIST = OUT_DIR / "whitelist.jsonl"
WORKS_FILE = OUT_DIR / "works.jsonl"
MANIFEST_FILE = OUT_DIR / "manifest.jsonl"
PROGRESS_FILE = OUT_DIR / "progress.json"
WL_LOG = OUT_DIR / "whitelist_log.jsonl"

FORMAT_PRIORITY = {"html": 0, "xhtml": 1, "pdf": 2, "pdfa1a": 3, "fmx4": 4, "txt": 5, "print": 6}

WORKERS = 8
TIMEOUT = 90
_lock = threading.Lock()


def case_num_celex_candidates(key: str) -> list[str]:
    m = re.match(r"^(?:([CT])-)?(\d+)/(\d{2,4})$", key)
    if not m:
        return []
    p, n, y = m.groups()
    yr = ("19" + y if int(y) >= 50 else "20" + y) if len(y) == 2 else y
    n_pad = n.zfill(4)
    if p == "T":
        return [f"6{yr}TJ{n_pad}", f"6{yr}TO{n_pad}"]
    return [f"6{yr}CJ{n_pad}", f"6{yr}CO{n_pad}", f"6{yr}CC{n_pad}", f"6{yr}CV{n_pad}"]


def regulation_celex(key: str) -> str | None:
    m = re.match(r"^(\d+)/(\d{4})$", key)
    if not m:
        return None
    n, y = m.groups()
    return f"3{y}R{n.zfill(4)}"


def sparql(q: str, timeout: int = 120):
    r = requests.post(
        ENDPOINT,
        data={"query": q, "format": "application/sparql-results+json"},
        headers={"User-Agent": UA, "Accept": "application/sparql-results+json",
                 "Content-Type": "application/x-www-form-urlencoded"},
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json()


def append_jsonl(path: Path, record: dict):
    line = json.dumps(record, ensure_ascii=False) + "\n"
    with _lock:
        fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
        try:
            os.write(fd, line.encode("utf-8"))
            os.fsync(fd)
        finally:
            os.close(fd)


def write_atomic_bytes(path: Path, content: bytes):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def resolve_works(uris: list[str], by: str) -> dict[str, dict]:
    """
    Bulk SPARQL: for given CELEX or ECLI URIs, return work metadata.
    by ∈ {"celex", "ecli"}.
    Returns {key_uri: {work_uri, cellar_uuid, celex, ecli, type, subjects, title, date}}.
    """
    if not uris:
        return {}
    out: dict[str, dict] = {}
    BATCH = 60
    pred = "owl:sameAs"
    for i in range(0, len(uris), BATCH):
        batch = uris[i:i+BATCH]
        values = " ".join(f"<{u}>" for u in batch)
        q = f"""
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT ?key_uri ?work ?type ?date
       (GROUP_CONCAT(DISTINCT ?slbl; SEPARATOR="|") AS ?subjects)
       (SAMPLE(?title) AS ?one_title)
       (SAMPLE(?other) AS ?one_other)
WHERE {{
  VALUES ?key_uri {{ {values} }}
  ?work {pred} ?key_uri .
  OPTIONAL {{ ?work cdm:work_has_resource-type ?type . }}
  OPTIONAL {{ ?work cdm:resource_legal_is_about_subject-matter ?subj .
             ?subj skos:prefLabel ?slbl . FILTER(LANG(?slbl)="en") }}
  OPTIONAL {{ ?work cdm:work_date_document ?date . }}
  OPTIONAL {{ ?expr cdm:expression_belongs_to_work ?work .
             ?expr cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/ENG> .
             ?expr cdm:expression_title ?title . }}
  OPTIONAL {{ ?work owl:sameAs ?other .
             FILTER(?other != ?key_uri) }}
}}
GROUP BY ?key_uri ?work ?type ?date
"""
        try:
            res = sparql(q)
            for row in res.get("results", {}).get("bindings", []):
                ku = row["key_uri"]["value"]
                work = row["work"]["value"]
                if "/cellar/" not in work:
                    continue
                uuid = work.rsplit("/", 1)[-1]
                rt = row.get("type", {}).get("value", "")
                rt_short = rt.rsplit("/", 1)[-1] if rt else None
                subjects = (row.get("subjects", {}).get("value") or "").split("|")
                subjects = [s for s in subjects if s]
                # Extract celex/ecli from key_uri
                celex = ku.rsplit("/", 1)[-1] if "/celex/" in ku else None
                ecli = None
                if "/ecli/" in ku:
                    from urllib.parse import unquote
                    ecli = unquote(ku.rsplit("/", 1)[-1])
                # also from "other" sameAs
                other = row.get("one_other", {}).get("value", "")
                if not celex and "/celex/" in other:
                    celex = other.rsplit("/", 1)[-1]
                if not ecli and "/ecli/" in other:
                    from urllib.parse import unquote
                    ecli = unquote(other.rsplit("/", 1)[-1])
                out[ku] = {
                    "work_uri": work,
                    "cellar_uuid": uuid,
                    "celex": celex,
                    "ecli": ecli,
                    "resource_type": rt_short,
                    "subject_matters": subjects,
                    "date": row.get("date", {}).get("value"),
                    "title": row.get("one_title", {}).get("value"),
                }
        except Exception as e:
            print(f"  SPARQL EXC batch {i//BATCH+1} ({by}): {type(e).__name__}: {e}", flush=True)
    return out


def best_manifs(works: list[dict]) -> dict[str, list[tuple[str, str]]]:
    if not works:
        return {}
    out: dict[str, list[tuple[str, str]]] = {}
    BATCH = 80
    work_uris = [w["work_uri"] for w in works]
    for i in range(0, len(work_uris), BATCH):
        batch = work_uris[i:i+BATCH]
        values = " ".join(f"<{u}>" for u in batch)
        q = f"""
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
SELECT ?work ?manif ?fmt WHERE {{
  VALUES ?work {{ {values} }}
  ?expr cdm:expression_belongs_to_work ?work .
  ?expr cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/ENG> .
  ?manif cdm:manifestation_manifests_expression ?expr .
  ?manif cdm:manifestation_type ?fmt .
}}
"""
        try:
            res = sparql(q)
            for row in res.get("results", {}).get("bindings", []):
                w = row["work"]["value"]
                m = row["manif"]["value"]
                f = row["fmt"]["value"].rsplit("/", 1)[-1].lower()
                out.setdefault(w, []).append((m, f))
        except Exception as e:
            print(f"  manif batch SPARQL EXC: {e}", flush=True)
    for w in out:
        out[w].sort(key=lambda x: FORMAT_PRIORITY.get(x[1], 99))
    return out


def fetch_one(uri: str) -> tuple[bool, dict, bytes]:
    fetch_uri = uri.rstrip("/") + "/DOC_1"
    try:
        r = requests.get(fetch_uri, headers={"User-Agent": UA}, timeout=TIMEOUT, allow_redirects=True)
    except requests.RequestException as e:
        return False, {"error": f"req_exc:{type(e).__name__}"}, b""
    if r.status_code != 200 or len(r.content) < 500:
        return False, {"error": f"http_{r.status_code}", "size": len(r.content)}, b""
    return True, {"size": len(r.content), "ct": r.headers.get("content-type", "?").split(";")[0]}, r.content


def download_work(work: dict, manifs: list[tuple[str, str]]) -> dict:
    uuid = work["cellar_uuid"]
    if not manifs:
        append_jsonl(WL_LOG, {"uuid": uuid, "celex": work.get("celex"),
                              "result": "no_english_manifestation"})
        return {"uuid": uuid, "ok": False}
    for manif_uri, fmt in manifs:
        if fmt == "print":
            continue
        ok, info, content = fetch_one(manif_uri)
        if ok:
            out_path = RAW_DIR / f"{uuid}.{fmt}"
            write_atomic_bytes(out_path, content)
            manifest_record = {
                "cellar_uuid": uuid,
                "celex": work.get("celex"),
                "ecli": work.get("ecli"),
                "resource_type": work.get("resource_type"),
                "date": work.get("date"),
                "title": (work.get("title") or "")[:300],
                "subject_matters": work.get("subject_matters"),
                "english_manif_format": fmt,
                "english_manif_uri": manif_uri,
                "raw_path": f"raw/{uuid}.{fmt}",
                "raw_size": info["size"],
                "raw_ct": info["ct"],
                "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "via": "whitelist",
            }
            append_jsonl(MANIFEST_FILE, manifest_record)
            append_jsonl(WL_LOG, {"uuid": uuid, "celex": work.get("celex"),
                                  "result": "ok", "format": fmt, "size": info["size"]})
            return {"uuid": uuid, "ok": True}
        time.sleep(0.05)
    append_jsonl(WL_LOG, {"uuid": uuid, "celex": work.get("celex"),
                          "result": "all_formats_failed",
                          "tried": [f for _, f in manifs]})
    return {"uuid": uuid, "ok": False}


def main():
    # Load whitelist
    whitelist = []
    with WHITELIST.open() as f:
        for line in f:
            whitelist.append(json.loads(line))
    print(f"Whitelist: {len(whitelist)} clusters")

    # Load existing scope works (we already have them)
    scope_uuids: set[str] = set()
    with WORKS_FILE.open() as f:
        for line in f:
            w = json.loads(line)
            if w.get("cellar_uuid"):
                scope_uuids.add(w["cellar_uuid"])

    # Build CELEX URI list and ECLI URI list
    celex_uris: list[str] = []
    ecli_uris: list[str] = []
    cluster_keys: list[tuple[str, str]] = []  # (uri, original_cluster_key) for log
    for c in whitelist:
        kind = c["kind"]
        key = c["key"]
        if kind == "CASE_NUM":
            for celex in case_num_celex_candidates(key):
                uri = f"http://publications.europa.eu/resource/celex/{celex}"
                celex_uris.append(uri)
                cluster_keys.append((uri, key))
        elif kind == "REGULATION":
            celex = regulation_celex(key)
            if celex:
                uri = f"http://publications.europa.eu/resource/celex/{celex}"
                celex_uris.append(uri)
                cluster_keys.append((uri, key))
        elif kind == "ECLI":
            uri = f"http://publications.europa.eu/resource/ecli/{quote(key, safe='')}"
            ecli_uris.append(uri)
            cluster_keys.append((uri, key))
    print(f"  CELEX URI candidates: {len(celex_uris)}")
    print(f"  ECLI URI candidates:  {len(ecli_uris)}")

    # Resolve in CELLAR
    print("Resolving CELEX URIs...", flush=True)
    celex_resolved = resolve_works(celex_uris, "celex")
    print(f"  resolved: {len(celex_resolved)}/{len(celex_uris)}", flush=True)
    print("Resolving ECLI URIs...", flush=True)
    ecli_resolved = resolve_works(ecli_uris, "ecli")
    print(f"  resolved: {len(ecli_resolved)}/{len(ecli_uris)}", flush=True)

    # Combine, dedup by cellar_uuid, exclude already-in-scope
    all_works: dict[str, dict] = {}
    for d in (celex_resolved, ecli_resolved):
        for uri, w in d.items():
            uuid = w["cellar_uuid"]
            if uuid in scope_uuids:
                continue
            if uuid in all_works:
                continue
            all_works[uuid] = w

    print(f"  unique new works: {len(all_works)}")

    # Fetch best English manifestation per work
    print("Fetching manifestations metadata...", flush=True)
    manifs_per_work = best_manifs(list(all_works.values()))

    # Download in parallel
    print(f"Downloading {len(all_works)} works...", flush=True)
    n_ok = 0
    n_fail = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futs = {pool.submit(download_work, w, manifs_per_work.get(w["work_uri"], [])): w
                for w in all_works.values()}
        done = set()
        if PROGRESS_FILE.exists():
            done = set(json.loads(PROGRESS_FILE.read_text()).get("done", []))
        for i, fut in enumerate(as_completed(futs), 1):
            res = fut.result()
            if res["ok"]:
                n_ok += 1
                done.add(res["uuid"])
            else:
                n_fail += 1
            if i % 25 == 0:
                tmp = PROGRESS_FILE.with_suffix(".json.tmp")
                with tmp.open("w") as f:
                    json.dump({"done": sorted(done)}, f)
                    f.flush(); os.fsync(f.fileno())
                os.replace(tmp, PROGRESS_FILE)
                print(f"  [{i}/{len(futs)}] ok={n_ok} fail={n_fail}", flush=True)
        # final save
        tmp = PROGRESS_FILE.with_suffix(".json.tmp")
        with tmp.open("w") as f:
            json.dump({"done": sorted(done)}, f)
            f.flush(); os.fsync(f.fileno())
        os.replace(tmp, PROGRESS_FILE)
    print(f"\nFinal: ok={n_ok} fail={n_fail} elapsed={(time.time()-t0)/60:.1f}min")


if __name__ == "__main__":
    main()
