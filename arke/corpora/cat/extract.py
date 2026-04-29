#!/usr/bin/env python3
"""Extract text from CAT corpus PDFs via pypdfium2.

Reads manifest.jsonl produced by scraper.py, deduplicates by sha1,
runs each PDF through PyPDFium2, writes text/{sha1}.txt and a parallel
manifest_text.jsonl that mirrors manifest.jsonl with extra fields:
    text_chars, n_pages, text_path

Resumable: if text/{sha1}.txt already exists, the extraction is skipped
(but its stats are still recomputed cheaply for the manifest_text line).

Atomic writes everywhere (.tmp + fsync + os.replace).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
import time
from pathlib import Path

import pypdfium2 as pdfium
from arke.corpora._paths import DATA, ENV_FILE



CAT_RAW = DATA / "cat_raw"
SOURCE_MANIFEST = CAT_RAW / "manifest.jsonl"
TEXT_DIR = CAT_RAW / "text"
OUTPUT_MANIFEST = CAT_RAW / "manifest_text.jsonl"
PROGRESS_PATH = CAT_RAW / "progress_extract.json"
ERRORS_PATH = CAT_RAW / "errors_extract.jsonl"

log = logging.getLogger("cat_extract")


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
    line = json.dumps(record, ensure_ascii=False) + "\n"
    data = line.encode("utf-8")
    if len(data) > 8000:
        raise ValueError(f"jsonl line too large ({len(data)} bytes)")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)


def extract_text(pdf_path: Path) -> tuple[str, int]:
    """Return (full_text, n_pages). Pages joined by double newline."""
    doc = pdfium.PdfDocument(str(pdf_path))
    n = len(doc)
    parts: list[str] = []
    for i in range(n):
        page = doc[i]
        textpage = page.get_textpage()
        parts.append(textpage.get_text_range() or "")
    return "\n\n".join(parts), n


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None,
                        help="Process only first N unique PDFs (sanity check).")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    TEXT_DIR.mkdir(parents=True, exist_ok=True)

    # Pass 1: collect unique sha1 → first pdf path. Drop rows with no sha1
    # (download failures during scrape — shouldn't be any but just in case).
    rows: list[dict] = []
    sha_to_pdf: dict[str, Path] = {}
    with SOURCE_MANIFEST.open() as f:
        for line in f:
            r = json.loads(line)
            rows.append(r)
            sha = r.get("sha1")
            if sha and sha not in sha_to_pdf:
                sha_to_pdf[sha] = CAT_RAW / r["local_path"]

    unique_shas = list(sha_to_pdf.keys())
    log.info("manifest: %d rows, %d unique PDFs", len(rows), len(unique_shas))
    if args.limit:
        unique_shas = unique_shas[: args.limit]
        log.info("limiting to first %d", args.limit)

    sha_stats: dict[str, dict] = {}
    n_extracted = 0
    n_skipped = 0
    n_failed = 0
    t_start = time.time()

    for i, sha in enumerate(unique_shas, 1):
        text_path = TEXT_DIR / f"{sha}.txt"
        pdf_path = sha_to_pdf[sha]

        if text_path.exists():
            existing = text_path.read_text(encoding="utf-8")
            sha_stats[sha] = {
                "text_chars": len(existing),
                "n_pages": None,  # we don't recompute; fill from current run only
                "text_path": f"text/{sha}.txt",
                "skipped": True,
            }
            n_skipped += 1
            continue

        try:
            text, n_pages = extract_text(pdf_path)
        except Exception as e:
            log.error("extract failed sha=%s pdf=%s: %s", sha[:8], pdf_path.name, e)
            append_jsonl(ERRORS_PATH, {
                "sha1": sha, "pdf_path": str(pdf_path), "error": str(e),
            })
            sha_stats[sha] = {
                "text_chars": 0,
                "n_pages": 0,
                "text_path": None,
                "error": str(e),
            }
            n_failed += 1
            continue

        atomic_write_bytes(text_path, text.encode("utf-8"))
        sha_stats[sha] = {
            "text_chars": len(text),
            "n_pages": n_pages,
            "text_path": f"text/{sha}.txt",
        }
        n_extracted += 1

        if n_extracted % 200 == 0:
            elapsed = time.time() - t_start
            rate = n_extracted / max(elapsed, 1e-9)
            remaining = len(unique_shas) - i
            eta_s = remaining / max(rate, 1e-9)
            log.info("progress %d/%d unique pdfs | rate=%.1f/s | eta=%.0fs",
                     i, len(unique_shas), rate, eta_s)

    # Pass 2: write manifest_text.jsonl mirroring source manifest with extras.
    log.info("writing manifest_text.jsonl with %d rows", len(rows))
    if OUTPUT_MANIFEST.exists():
        OUTPUT_MANIFEST.unlink()
    for r in rows:
        sha = r.get("sha1")
        extras = sha_stats.get(sha) if sha else None
        if extras is None:
            extras = {"text_chars": None, "n_pages": None, "text_path": None}
        out = {**r, "text_chars": extras.get("text_chars"),
               "n_pages": extras.get("n_pages"),
               "text_path": extras.get("text_path")}
        append_jsonl(OUTPUT_MANIFEST, out)

    # Save progress snapshot.
    atomic_write_bytes(
        PROGRESS_PATH,
        json.dumps({
            "unique_shas_total": len(unique_shas),
            "extracted_this_run": n_extracted,
            "skipped_already_done": n_skipped,
            "failed": n_failed,
        }, indent=2).encode(),
    )

    elapsed = time.time() - t_start
    log.info("done: extracted=%d skipped=%d failed=%d elapsed=%.1fs",
             n_extracted, n_skipped, n_failed, elapsed)
    return 0 if n_failed == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
