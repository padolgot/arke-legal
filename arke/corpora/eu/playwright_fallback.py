"""
Playwright fallback for works missing from CELLAR REST.

For works with no English manifestation OR all formats 404'd,
try EUR-Lex frontend at legal-content/EN/TXT/HTML/?uri=CELEX:{celex}.
This passes CloudFront WAF's JS challenge automatically via headless Chromium.

Output: appends to manifest.jsonl with via=playwright; saves raw to raw/{uuid}.frontend.html.
"""

from __future__ import annotations
import json
import os
import time
from pathlib import Path

from playwright.sync_api import sync_playwright
from arke.corpora._paths import DATA, ENV_FILE



OUT_DIR = DATA / "eu_pool"
RAW_DIR = OUT_DIR / "raw"
WORKS_FILE = OUT_DIR / "works.jsonl"
RETRY_LOG = OUT_DIR / "retry_log.jsonl"
MANIFEST_FILE = OUT_DIR / "manifest.jsonl"
PROGRESS_FILE = OUT_DIR / "progress.json"
PW_LOG = OUT_DIR / "playwright_log.jsonl"

NAV_TIMEOUT_MS = 25000
WAIT_MS = 3000  # let WAF challenge complete


def append_jsonl(path: Path, record: dict):
    line = json.dumps(record, ensure_ascii=False) + "\n"
    fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
    try:
        os.write(fd, line.encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)


def write_atomic(path: Path, content: bytes):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def load_progress() -> set[str]:
    if not PROGRESS_FILE.exists():
        return set()
    return set(json.loads(PROGRESS_FILE.read_text()).get("done", []))


def save_progress(done: set[str]):
    tmp = PROGRESS_FILE.with_suffix(".json.tmp")
    with tmp.open("w") as f:
        json.dump({"done": sorted(done)}, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, PROGRESS_FILE)


def fetch_via_frontend(page, celex: str) -> tuple[bool, str, int]:
    url = f"https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:{celex}"
    try:
        page.goto(url, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
    except Exception as e:
        return False, f"goto_exc:{type(e).__name__}", 0
    page.wait_for_timeout(WAIT_MS)
    try:
        # Detect "no documents" page
        title = page.title()[:200]
        body = page.content()
        if "No documents" in body or "?-EUR-Lex" in title or len(body) < 5000:
            return False, f"empty:title={title[:60]}", len(body)
        return True, body, len(body)
    except Exception as e:
        return False, f"content_exc:{type(e).__name__}", 0


def main():
    # Build target list — CELEX from retry_log (still failed) + (no_manif works)
    failed_uuids: set[str] = set()
    if RETRY_LOG.exists():
        with RETRY_LOG.open() as f:
            for line in f:
                r = json.loads(line)
                if r.get("result") in ("all_formats_failed", "still_no_english_manif"):
                    failed_uuids.add(r["uuid"])
    print(f"Targets from retry_log: {len(failed_uuids)}")

    works_by_uuid: dict[str, dict] = {}
    with WORKS_FILE.open() as f:
        for line in f:
            w = json.loads(line)
            if w.get("cellar_uuid"):
                works_by_uuid[w["cellar_uuid"]] = w

    done = load_progress()
    targets = []
    for uuid in failed_uuids:
        w = works_by_uuid.get(uuid)
        if not w or not w.get("celex") or uuid in done:
            continue
        targets.append(w)
    print(f"To fetch via Playwright: {len(targets)}")

    n_ok = 0
    n_fail = 0
    t0 = time.time()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"),
            locale="en-GB",
        )
        page = ctx.new_page()
        for i, w in enumerate(targets, 1):
            uuid = w["cellar_uuid"]
            celex = w["celex"]
            ok, body_or_err, size = fetch_via_frontend(page, celex)
            if ok:
                raw_path = RAW_DIR / f"{uuid}.frontend.html"
                write_atomic(raw_path, body_or_err.encode("utf-8"))
                manifest_record = {
                    "cellar_uuid": uuid,
                    "celex": celex,
                    "ecli": w.get("ecli"),
                    "resource_type": w.get("resource_type"),
                    "date": w.get("date"),
                    "title": (w.get("title") or "")[:300],
                    "subject_matters": w.get("subject_matters"),
                    "english_manif_format": "html",
                    "english_manif_uri": f"https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:{celex}",
                    "raw_path": f"raw/{uuid}.frontend.html",
                    "raw_size": size,
                    "raw_ct": "text/html",
                    "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "via": "playwright_frontend",
                }
                append_jsonl(MANIFEST_FILE, manifest_record)
                done.add(uuid)
                n_ok += 1
                append_jsonl(PW_LOG, {"uuid": uuid, "celex": celex, "result": "ok", "size": size})
            else:
                n_fail += 1
                append_jsonl(PW_LOG, {"uuid": uuid, "celex": celex, "result": "fail",
                                       "reason": body_or_err[:200]})

            if i % 20 == 0:
                save_progress(done)
                el = time.time() - t0
                rate = i / el
                eta = (len(targets) - i) / rate if rate > 0 else float("inf")
                print(f"  [{i:>3}/{len(targets)}] ok={n_ok} fail={n_fail} eta={eta/60:.1f}min", flush=True)

        browser.close()
    save_progress(done)
    print(f"\nFinal: ok={n_ok} fail={n_fail} elapsed={(time.time()-t0)/60:.1f}min")


if __name__ == "__main__":
    main()
