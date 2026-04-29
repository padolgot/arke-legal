#!/usr/bin/env python3
"""CAT Document Archive scraper.

Downloads every PDF from https://www.catribunal.org.uk/documents
plus per-document metadata (case ref, parties, doc_type, date, neutral
citation, summary URL).

Output layout (under --out):
    pdfs/{sha1}.pdf       — content-addressed PDF, deduplicated
    pdfs/{sha1}.json      — sidecar with raw bytes metadata
    manifest.jsonl        — append-only log, one line per result row
    progress.json         — set of completed pages, for resume
    errors.jsonl          — append-only log of PDF fetch failures

Resumable: re-running skips already-completed pages.
Atomic writes everywhere on disk (.tmp + fsync + os.replace).

Usage:
    python scraper.py                              # full run, all 343 pages
    python scraper.py --start-page 50 --end-page 60
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from arke.corpora._paths import DATA, ENV_FILE



BASE = "https://www.catribunal.org.uk"
DOCS_URL = f"{BASE}/documents"
UA = "arke-research-scraper/0.1 (CAT corpus build)"
SLEEP_HTML_S = 0.5
SLEEP_PDF_S = 0.2
HTTP_TIMEOUT_S = 60
HTTP_MAX_RETRIES = 3
LAST_PAGE = 342

log = logging.getLogger("cat_scraper")


@dataclass
class Row:
    page: int
    row_index: int
    case_ref: str
    case_url: str
    parties: str
    doc_type: str
    pdf_url: str
    pdf_absolute_url: str
    date: str
    neutral_citation: str | None
    summary_url: str | None


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, path)
    except Exception:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
        raise


def append_jsonl(path: Path, record: dict) -> None:
    """Append one JSON line. Single small write under PIPE_BUF is atomic on POSIX
    when the file is opened with O_APPEND. fsync after to flush to disk."""
    line = json.dumps(record, ensure_ascii=False) + "\n"
    data = line.encode("utf-8")
    if len(data) > 4000:
        raise ValueError(f"jsonl line too large ({len(data)} bytes), atomicity not guaranteed")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)


def http_get(session: requests.Session, url: str) -> bytes:
    last_exc: Exception | None = None
    for attempt in range(1, HTTP_MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=HTTP_TIMEOUT_S)
            r.raise_for_status()
            return r.content
        except requests.RequestException as e:
            last_exc = e
            if attempt < HTTP_MAX_RETRIES:
                backoff = 2 ** attempt
                log.warning("http error on %s (attempt %s): %s — retry in %ss", url, attempt, e, backoff)
                time.sleep(backoff)
    assert last_exc is not None
    raise last_exc


def parse_listing(html: bytes, page: int) -> list[Row]:
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select("li.views-row")
    rows: list[Row] = []
    for i, item in enumerate(items):
        case_a = item.select_one("div.h4 a")
        pdf_a = item.select_one("h2.h5 a.link-plain")
        time_el = item.select_one("time")
        if not (case_a and pdf_a and time_el):
            log.warning("page %s row %s: incomplete row, skipping", page, i)
            continue

        case_text = case_a.get_text(" ", strip=True)
        case_url = case_a["href"]
        # case ref pattern e.g. "1766/4/12/26 Aramark Limited v ..."
        ref_split = case_text.split(" ", 1)
        case_ref = ref_split[0]
        parties = ref_split[1] if len(ref_split) > 1 else ""

        pdf_url = pdf_a["href"]
        doc_type = pdf_a.get_text(" ", strip=True)

        date_attr = time_el.get("datetime", "")
        date = date_attr[:10] if date_attr else ""

        # neutral citation: optional <span>[YYYY]</span>...<span>CAT</span>...<span>N</span>
        cit_year: str | None = None
        cit_n: str | None = None
        for span in item.find_all("span"):
            t = span.get_text(strip=True)
            if t.startswith("[") and t.endswith("]") and len(t) <= 8:
                cit_year = t.strip("[]")
            elif t.isdigit() and len(t) <= 4:
                cit_n = t
        citation = f"[{cit_year}] CAT {cit_n}" if (cit_year and cit_n) else None

        summary_a = item.select_one("a[href^='/judgments/']")
        summary_url = summary_a["href"] if summary_a else None

        rows.append(Row(
            page=page,
            row_index=i,
            case_ref=case_ref,
            case_url=case_url,
            parties=parties,
            doc_type=doc_type,
            pdf_url=pdf_url,
            pdf_absolute_url=BASE + pdf_url,
            date=date,
            neutral_citation=citation,
            summary_url=summary_url,
        ))
    return rows


def sha1_hex(data: bytes) -> str:
    return hashlib.sha1(data).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=str(DATA / "cat_raw"))
    parser.add_argument("--start-page", type=int, default=0)
    parser.add_argument("--end-page", type=int, default=LAST_PAGE)
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse listing pages only, do not download PDFs.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    out = Path(args.out)
    pdfs_dir = out / "pdfs"
    pdfs_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out / "manifest.jsonl"
    progress_path = out / "progress.json"
    errors_path = out / "errors.jsonl"

    if progress_path.exists():
        progress = json.loads(progress_path.read_text())
        pages_done = set(progress.get("pages_done", []))
    else:
        pages_done = set()

    session = requests.Session()
    session.headers.update({"User-Agent": UA})

    n_pdfs_downloaded = 0

    for page in range(args.start_page, args.end_page + 1):
        if page in pages_done:
            log.info("page %s already done, skip", page)
            continue

        listing_url = f"{DOCS_URL}?page={page}"
        log.info("fetch listing %s", listing_url)
        html = http_get(session, listing_url)
        time.sleep(SLEEP_HTML_S)
        rows = parse_listing(html, page)
        log.info("page %s: %s rows parsed", page, len(rows))

        for row in rows:
            sha: str | None = None
            size: int | None = None
            local_pdf_rel: str | None = None

            if not args.dry_run:
                try:
                    pdf_bytes = http_get(session, row.pdf_absolute_url)
                except requests.RequestException as e:
                    append_jsonl(errors_path, {
                        "page": row.page, "row_index": row.row_index,
                        "pdf_url": row.pdf_absolute_url, "error": str(e),
                    })
                    log.error("PDF fetch failed: %s — logged, continuing", row.pdf_absolute_url)
                    pdf_bytes = None

                if pdf_bytes is not None:
                    sha = sha1_hex(pdf_bytes)
                    size = len(pdf_bytes)
                    pdf_path = pdfs_dir / f"{sha}.pdf"
                    if not pdf_path.exists():
                        atomic_write_bytes(pdf_path, pdf_bytes)
                    local_pdf_rel = f"pdfs/{sha}.pdf"
                    n_pdfs_downloaded += 1
                    time.sleep(SLEEP_PDF_S)

            record = asdict(row) | {
                "sha1": sha,
                "size_bytes": size,
                "local_path": local_pdf_rel,
            }
            append_jsonl(manifest_path, record)

        pages_done.add(page)
        atomic_write_bytes(
            progress_path,
            json.dumps({"pages_done": sorted(pages_done)}, indent=2).encode(),
        )

    log.info("done: pdfs_downloaded=%s pages_processed=%s",
             n_pdfs_downloaded, len(pages_done))
    return 0


if __name__ == "__main__":
    sys.exit(main())
