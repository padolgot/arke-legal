"""
Phase 2: download English manifestation for each work in works_with_manif.jsonl.

Each work has a pre-resolved manifestation URI + format.
Direct GET on that URI returns the file (txt/xhtml/html/pdf/etc.).

Parallel downloader (8 workers) with atomic writes and resumability.
"""

from __future__ import annotations
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from arke.corpora._paths import DATA, ENV_FILE



UA = "arke-research/0.1 (downloader; ivankrylov684@gmail.com)"
OUT_DIR = DATA / "eu_pool"
RAW_DIR = OUT_DIR / "raw"
WORKS_FILE = OUT_DIR / "works_with_manif.jsonl"
MANIFEST_FILE = OUT_DIR / "manifest.jsonl"
PROGRESS_FILE = OUT_DIR / "progress.json"
ERRORS_FILE = OUT_DIR / "errors.jsonl"

WORKERS = 8
SLEEP_S = 0.1
TIMEOUT = 180
PROGRESS_FLUSH_EVERY = 50

RAW_DIR.mkdir(parents=True, exist_ok=True)
_progress_lock = threading.Lock()
_append_lock = threading.Lock()


def load_progress() -> set[str]:
    if not PROGRESS_FILE.exists():
        return set()
    try:
        return set(json.loads(PROGRESS_FILE.read_text()).get("done", []))
    except Exception:
        return set()


def save_progress(done: set[str]):
    tmp = PROGRESS_FILE.with_suffix(".json.tmp")
    with tmp.open("w") as f:
        json.dump({"done": sorted(done)}, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, PROGRESS_FILE)


def append_jsonl_atomic(path: Path, record: dict):
    line = json.dumps(record, ensure_ascii=False) + "\n"
    with _append_lock:
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


def fetch_one(uri: str, fmt: str) -> tuple[bool, dict, bytes]:
    """Returns (ok, info, content_bytes)."""
    try:
        r = requests.get(uri, headers={"User-Agent": UA}, timeout=TIMEOUT, allow_redirects=True)
    except requests.RequestException as e:
        return False, {"error": f"request_exception:{type(e).__name__}", "msg": str(e)[:200]}, b""
    if r.status_code != 200:
        return False, {"error": f"http_{r.status_code}", "size": len(r.content)}, b""
    if len(r.content) < 500:
        return False, {"error": "too_small", "size": len(r.content)}, b""
    return True, {"size": len(r.content), "ct": r.headers.get("content-type", "?").split(";")[0]}, r.content


def worker_task(work: dict) -> dict:
    uuid = work["cellar_uuid"]
    uri = work.get("english_manif_uri")
    fmt = work.get("english_manif_format") or "unknown"
    if not uri:
        append_jsonl_atomic(ERRORS_FILE, {"cellar_uuid": uuid, "celex": work.get("celex"),
                                          "error": "no_english_manifestation",
                                          "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})
        return {"uuid": uuid, "ok": False, "skipped": True}
    # CELLAR manifestation URIs return RDF metadata; need /DOC_1 suffix
    # to fetch the actual file content.
    fetch_uri = uri.rstrip("/") + "/DOC_1"
    ok, info, content = fetch_one(fetch_uri, fmt)
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
            "english_manif_uri": uri,
            "raw_path": f"raw/{uuid}.{fmt}",
            "raw_size": info["size"],
            "raw_ct": info["ct"],
            "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        append_jsonl_atomic(MANIFEST_FILE, manifest_record)
    else:
        err_record = {"cellar_uuid": uuid, "celex": work.get("celex"),
                      "ecli": work.get("ecli"), "manif_uri": uri, "format": fmt,
                      **info, "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        append_jsonl_atomic(ERRORS_FILE, err_record)
    time.sleep(SLEEP_S)
    return {"uuid": uuid, "ok": ok, "size": info.get("size", 0)}


def main(limit: int | None = None):
    works = []
    with WORKS_FILE.open() as f:
        for line in f:
            works.append(json.loads(line))

    done = load_progress()
    todo = [w for w in works if w.get("cellar_uuid") and w["cellar_uuid"] not in done]
    if limit:
        todo = todo[:limit]
    n_no_manif = sum(1 for w in todo if not w.get("english_manif_uri"))
    print(f"Total: {len(works)}, done: {len(done)}, todo: {len(todo)} (no_manif: {n_no_manif})", flush=True)
    print(f"Workers: {WORKERS}", flush=True)

    n_ok = 0
    n_err = 0
    t0 = time.time()
    new_done: set[str] = set()

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(worker_task, w): w for w in todo}
        for i, fut in enumerate(as_completed(futures), 1):
            try:
                res = fut.result()
            except Exception as e:
                w = futures[fut]
                append_jsonl_atomic(ERRORS_FILE, {"cellar_uuid": w.get("cellar_uuid"),
                                                  "error": f"future_exc:{type(e).__name__}",
                                                  "msg": str(e)[:200],
                                                  "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})
                n_err += 1
                continue
            if res.get("skipped"):
                n_err += 1
            elif res["ok"]:
                n_ok += 1
                with _progress_lock:
                    new_done.add(res["uuid"])
            else:
                n_err += 1

            if i % PROGRESS_FLUSH_EVERY == 0:
                with _progress_lock:
                    done.update(new_done)
                    snap = set(done)
                save_progress(snap)
                elapsed = time.time() - t0
                rate = i / elapsed
                eta = (len(todo) - i) / rate if rate > 0 else float("inf")
                print(f"  [{i:>4}/{len(todo)}] ok={n_ok} err={n_err}  rate={rate:.2f}/s  eta={eta/60:.1f}min",
                      flush=True)

    with _progress_lock:
        done.update(new_done)
    save_progress(done)
    elapsed = time.time() - t0
    print(f"\nFinal: ok={n_ok} err={n_err} elapsed={elapsed/60:.1f}min", flush=True)


if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else None
    main(limit)
