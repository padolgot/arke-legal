"""
Retry the 297 failed works by trying ALL their English manifestations.

Many phantom failures pick the first-priority format that has no datastream.
Other manifestations of the same work (different format) often DO have content.
"""

from __future__ import annotations
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from arke.corpora._paths import DATA, ENV_FILE



ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"
UA = "arke-research/0.1 (retry; ivankrylov684@gmail.com)"
OUT_DIR = DATA / "eu_pool"
RAW_DIR = OUT_DIR / "raw"
WORKS_FILE = OUT_DIR / "works.jsonl"
ERRORS_FILE = OUT_DIR / "errors.jsonl"
MANIFEST_FILE = OUT_DIR / "manifest.jsonl"
PROGRESS_FILE = OUT_DIR / "progress.json"
RETRY_LOG = OUT_DIR / "retry_log.jsonl"

# Priority — txt last (phantom), print last (RDF stub).
FORMAT_PRIORITY = {"html": 0, "xhtml": 1, "pdf": 2, "pdfa1a": 3, "fmx4": 4, "txt": 5, "print": 6}

WORKERS = 6
TIMEOUT = 60
_lock = threading.Lock()


def sparql(q: str, timeout: int = 90):
    r = requests.post(
        ENDPOINT,
        data={"query": q, "format": "application/sparql-results+json"},
        headers={"User-Agent": UA, "Accept": "application/sparql-results+json",
                 "Content-Type": "application/x-www-form-urlencoded"},
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json()


def get_all_english_manifs(work_uris: list[str]) -> dict[str, list[tuple[str, str]]]:
    """Returns {work_uri: [(manif_uri, fmt), ...]} sorted by priority."""
    out: dict[str, list[tuple[str, str]]] = {}
    BATCH = 80
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
            print(f"  SPARQL EXC batch {i//BATCH+1}: {type(e).__name__}: {e}", flush=True)
    # Sort each by priority
    for w in out:
        out[w].sort(key=lambda x: FORMAT_PRIORITY.get(x[1], 99))
    return out


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


def try_manif(manif_uri: str, fmt: str) -> tuple[bool, dict, bytes]:
    """Try /DOC_1 of manifestation. Returns (ok, info, content)."""
    fetch_uri = manif_uri.rstrip("/") + "/DOC_1"
    try:
        r = requests.get(fetch_uri, headers={"User-Agent": UA}, timeout=TIMEOUT, allow_redirects=True)
    except requests.RequestException as e:
        return False, {"error": f"req_exc:{type(e).__name__}"}, b""
    if r.status_code != 200 or len(r.content) < 500:
        return False, {"error": f"http_{r.status_code}", "size": len(r.content)}, b""
    return True, {"size": len(r.content), "ct": r.headers.get("content-type", "?").split(";")[0]}, r.content


def retry_work(work: dict, manifs: list[tuple[str, str]]) -> dict:
    uuid = work["cellar_uuid"]
    if not manifs:
        append_jsonl(RETRY_LOG, {"uuid": uuid, "celex": work.get("celex"),
                                  "result": "still_no_english_manif"})
        return {"uuid": uuid, "ok": False}

    for manif_uri, fmt in manifs:
        if fmt in ("print",):  # known no-datastream format
            continue
        ok, info, content = try_manif(manif_uri, fmt)
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
                "via": "retry",
            }
            append_jsonl(MANIFEST_FILE, manifest_record)
            append_jsonl(RETRY_LOG, {"uuid": uuid, "celex": work.get("celex"),
                                      "result": "ok", "format": fmt, "size": info["size"]})
            return {"uuid": uuid, "ok": True}
        time.sleep(0.05)

    # All formats exhausted
    append_jsonl(RETRY_LOG, {"uuid": uuid, "celex": work.get("celex"),
                              "result": "all_formats_failed",
                              "tried": [f for _, f in manifs]})
    return {"uuid": uuid, "ok": False}


def main():
    # Read failed cellar_uuids from errors.jsonl
    failed_uuids: set[str] = set()
    with ERRORS_FILE.open() as f:
        for line in f:
            r = json.loads(line)
            if r.get("cellar_uuid"):
                failed_uuids.add(r["cellar_uuid"])
    print(f"Failed UUIDs: {len(failed_uuids)}")

    # Load works
    works_by_uuid: dict[str, dict] = {}
    with WORKS_FILE.open() as f:
        for line in f:
            w = json.loads(line)
            if w.get("cellar_uuid"):
                works_by_uuid[w["cellar_uuid"]] = w

    failed_works = [works_by_uuid[u] for u in failed_uuids if u in works_by_uuid]
    print(f"Failed works to retry: {len(failed_works)}")

    # Already-done check (don't retry if somehow succeeded)
    done = set()
    if PROGRESS_FILE.exists():
        try:
            done = set(json.loads(PROGRESS_FILE.read_text()).get("done", []))
        except Exception:
            pass
    todo = [w for w in failed_works if w["cellar_uuid"] not in done]
    print(f"After dedup with progress: {len(todo)} todo")

    # Bulk SPARQL for ALL English manifestations of failed works
    work_uris = [w["work_uri"] for w in todo]
    print(f"Fetching ALL English manifestations for {len(work_uris)} works...")
    all_manifs = get_all_english_manifs(work_uris)
    n_have_some = sum(1 for w in todo if all_manifs.get(w["work_uri"]))
    print(f"  works with at least one English manif: {n_have_some}")

    # Retry in parallel
    n_ok = 0
    n_fail = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {
            pool.submit(retry_work, w, all_manifs.get(w["work_uri"], [])): w
            for w in todo
        }
        for i, fut in enumerate(as_completed(futures), 1):
            res = fut.result()
            if res["ok"]:
                n_ok += 1
                done.add(res["uuid"])
            else:
                n_fail += 1
            if i % 30 == 0:
                tmp = PROGRESS_FILE.with_suffix(".json.tmp")
                with tmp.open("w") as f:
                    json.dump({"done": sorted(done)}, f)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, PROGRESS_FILE)
                print(f"  [{i:>3}/{len(todo)}] ok={n_ok} fail={n_fail}", flush=True)

    # Final progress save
    tmp = PROGRESS_FILE.with_suffix(".json.tmp")
    with tmp.open("w") as f:
        json.dump({"done": sorted(done)}, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, PROGRESS_FILE)

    elapsed = time.time() - t0
    print(f"\nFinal: rescued={n_ok} still_failed={n_fail}  elapsed={elapsed/60:.1f}min")


if __name__ == "__main__":
    main()
