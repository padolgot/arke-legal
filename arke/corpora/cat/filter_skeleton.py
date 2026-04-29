#!/usr/bin/env python3
"""Filter the full CAT corpus down to skeleton-argument-grade authorities.

Reads manifest_text.jsonl, keeps only documents whose doc_type maps to
authority-grade material useful for skeleton argument drafting:
    * UKSC + CoA judgments
    * CAT judgments + rulings
    * Reasoned orders

Drops everything else (admin orders, transcripts, summaries, schedules,
notices, consents, transfers).

Output:
    <umbrella>/corpora/cat_skeleton/
        pdfs/{sha1}.pdf       — copied
        text/{sha1}.txt       — copied
        manifest_skeleton.jsonl — filtered manifest

Originals at .../cat_raw/ untouched.
"""
from __future__ import annotations

import json
import logging
import re
import shutil
import sys
from pathlib import Path
from arke.corpora._paths import DATA, ENV_FILE



SOURCE = DATA / "cat_raw"
DEST = DATA / "cat_skeleton"

KEEP_CATEGORIES = {
    "A_apex_UKSC",
    "A_apex_CoA",
    "B_judgment_or_ruling",
}

log = logging.getLogger("filter_skeleton")


def categorise(doc_type: str) -> str:
    """Same heuristic as the survey script. Order matters."""
    if re.search(r"Supreme Court", doc_type):
        return "A_apex_UKSC"
    if re.search(r"Court of Appeal|EWCA", doc_type):
        return "A_apex_CoA"
    if re.search(r"^Judgment\b|^Ruling\b", doc_type):
        return "B_judgment_or_ruling"
    if re.search(r"^Reasoned", doc_type):
        return "B_reasoned_order"
    return "OTHER"


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    pdf_dest = DEST / "pdfs"
    text_dest = DEST / "text"
    manifest_dest = DEST / "manifest_skeleton.jsonl"
    pdf_dest.mkdir(parents=True, exist_ok=True)
    text_dest.mkdir(parents=True, exist_ok=True)
    if manifest_dest.exists():
        manifest_dest.unlink()

    src_manifest = SOURCE / "manifest_text.jsonl"
    n_kept = 0
    n_dropped = 0
    copied_shas: set[str] = set()

    with src_manifest.open() as f:
        for line in f:
            r = json.loads(line)
            cat = categorise(r["doc_type"])
            if cat not in KEEP_CATEGORIES:
                n_dropped += 1
                continue

            sha = r.get("sha1")
            if not sha or not r.get("local_path") or not r.get("text_path"):
                log.warning("skip row with missing assets: %s", r["doc_type"])
                continue

            src_pdf = SOURCE / r["local_path"]
            src_txt = SOURCE / r["text_path"]
            dst_pdf = pdf_dest / f"{sha}.pdf"
            dst_txt = text_dest / f"{sha}.txt"

            if sha not in copied_shas:
                if src_pdf.exists() and not dst_pdf.exists():
                    shutil.copy2(src_pdf, dst_pdf)
                if src_txt.exists() and not dst_txt.exists():
                    shutil.copy2(src_txt, dst_txt)
                copied_shas.add(sha)

            out_record = {**r, "category": cat}
            with manifest_dest.open("a", encoding="utf-8") as out:
                out.write(json.dumps(out_record, ensure_ascii=False) + "\n")
            n_kept += 1

    log.info("kept rows: %d", n_kept)
    log.info("dropped rows: %d", n_dropped)
    log.info("unique PDFs copied: %d", len(copied_shas))
    log.info("output at %s", DEST)
    return 0


if __name__ == "__main__":
    sys.exit(main())
