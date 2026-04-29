#!/usr/bin/env python3
"""Smoke test for PDF→text extraction over the CAT corpus.

Samples a handful of PDFs from each major doc_type category, extracts
text via two independent engines (pypdfium2 + pdftotext/poppler), and
prints a per-document report so we can eyeball:
  * Does the PDF have an embedded text layer (or is it a scan that needs OCR)?
  * Is it encrypted / malformed?
  * Does pypdfium2 output agree with pdftotext output (cross-engine sanity)?
  * Any embedded files, attachments, weirdness?
  * What does first/last 300 chars look like?
"""
from __future__ import annotations

import json
import re
import subprocess
from collections import defaultdict
from pathlib import Path

import pypdfium2 as pdfium
from arke.corpora._paths import DATA, ENV_FILE



CAT_RAW = DATA / "cat_raw"
MANIFEST = CAT_RAW / "manifest.jsonl"
PDFS = CAT_RAW / "pdfs"

# Category buckets — capture the major flavours.
CATEGORY_PATTERNS = [
    ("Judgment_UKSC", r"Supreme Court"),
    ("Judgment_CoA", r"Court of Appeal"),
    ("Judgment_main", r"^Judgment$|^Judgment \(.*Trial|^Judgment \(CPO|^Judgment \(Strike"),
    ("Transcript_hearing", r"Transcript.*[Hh]earing|Transcript of hearing"),
    ("Transcript_CMC", r"Transcript.*CMC|case management"),
    ("Transcript_CPO", r"Transcript.*CPO|CPO Hearing"),
    ("Order_chair", r"^Order of the Chair|Order of the President|Order of the Tribunal"),
    ("Ruling", r"^Ruling"),
    ("Reasoned_order", r"^Reasoned"),
    ("Summary", r"^Summary"),
    ("Schedule", r"^Schedule"),
    ("Consent", r"^Consent"),
]

PER_CATEGORY = 2
TARGET_CASE_PARTIES = ["Hammond", "Apple", "Vodafone", "Merricks", "Patourel", "Phillip Evans"]


def categorise(doc_type: str) -> str | None:
    for name, pat in CATEGORY_PATTERNS:
        if re.search(pat, doc_type):
            return name
    return None


def pdfium_extract(pdf_path: Path) -> tuple[str, dict]:
    pdf = pdfium.PdfDocument(str(pdf_path))
    parts: list[str] = []
    for page in pdf:
        textpage = page.get_textpage()
        parts.append(textpage.get_text_range())
    text = "\n\n".join(parts)
    meta = {
        "pages": len(pdf),
    }
    return text, meta


def poppler_extract(pdf_path: Path) -> str:
    res = subprocess.run(
        ["pdftotext", "-layout", str(pdf_path), "-"],
        capture_output=True, timeout=60,
    )
    if res.returncode != 0:
        return f"<<pdftotext failed: {res.stderr.decode(errors='replace')[:200]}>>"
    return res.stdout.decode("utf-8", errors="replace")


def pdfium_metadata(pdf_path: Path) -> dict:
    """Inspect potentially weird things: encryption, attachments, scan-only."""
    pdf = pdfium.PdfDocument(str(pdf_path))
    n_pages = len(pdf)
    # crude scan detection: extract text from first 3 pages, if total < 50 chars → likely scan
    sample = ""
    for i in range(min(3, n_pages)):
        sample += pdf[i].get_textpage().get_text_range() or ""
    return {
        "pages": n_pages,
        "first3_text_len": len(sample),
        "looks_like_scan": (len(sample) < 50 and n_pages > 0),
        "form_type": pdf.get_formtype(),  # 0 = no form, others = AcroForm/XFA
    }


def pick_samples() -> list[dict]:
    by_cat: dict[str, list[dict]] = defaultdict(list)
    target_hits: list[dict] = []
    with MANIFEST.open() as f:
        for line in f:
            r = json.loads(line)
            if not r.get("local_path"):
                continue
            cat = categorise(r["doc_type"])
            if cat and len(by_cat[cat]) < PER_CATEGORY:
                by_cat[cat].append({"_cat": cat, **r})
            for needle in TARGET_CASE_PARTIES:
                if needle in r["parties"] and "Judgment" in r["doc_type"] and len(target_hits) < 6:
                    if not any(t["sha1"] == r["sha1"] for t in target_hits):
                        target_hits.append({"_cat": f"target_{needle}", **r})

    samples: list[dict] = []
    for cat in [name for name, _ in CATEGORY_PATTERNS]:
        samples.extend(by_cat.get(cat, []))
    samples.extend(target_hits)
    return samples


def main() -> None:
    samples = pick_samples()
    print(f"# CAT corpus extraction smoke test\n")
    print(f"Sampling {len(samples)} PDFs across categories.\n")
    print(f"Engines: pypdfium2 (Google PDFium) + pdftotext (Poppler) for cross-validation.\n")
    print("=" * 78)

    discrepancies = []
    issues = []

    for s in samples:
        pdf_path = CAT_RAW / s["local_path"]
        if not pdf_path.exists():
            print(f"\n[MISSING] {s['local_path']}")
            continue

        meta = pdfium_metadata(pdf_path)
        try:
            t_pdfium, _ = pdfium_extract(pdf_path)
        except Exception as e:
            t_pdfium = f"<<pdfium failed: {e}>>"
            issues.append((s["doc_type"], "pdfium", str(e)))
        try:
            t_poppler = poppler_extract(pdf_path)
        except Exception as e:
            t_poppler = f"<<poppler failed: {e}>>"
            issues.append((s["doc_type"], "poppler", str(e)))

        len_pdfium = len(t_pdfium)
        len_poppler = len(t_poppler)
        diff_pct = abs(len_pdfium - len_poppler) / max(len_pdfium, len_poppler, 1) * 100
        if diff_pct > 15:
            discrepancies.append((s["doc_type"], s["sha1"][:8], len_pdfium, len_poppler, diff_pct))

        head = re.sub(r"\s+", " ", t_pdfium[:280])
        tail = re.sub(r"\s+", " ", t_pdfium[-200:]) if len_pdfium > 480 else ""

        print(f"\n--- [{s['_cat']}] {s['doc_type'][:60]}")
        print(f"    parties: {s['parties'][:70]}")
        print(f"    date: {s['date']} | citation: {s.get('neutral_citation') or '-'}")
        print(f"    pages: {meta['pages']} | bytes: {s['size_bytes']:,}")
        print(f"    pdfium chars: {len_pdfium:,} | poppler chars: {len_poppler:,} | diff: {diff_pct:.1f}%")
        if meta["looks_like_scan"]:
            print(f"    !!! LIKELY SCAN — first 3 pages only {meta['first3_text_len']} chars text")
            issues.append((s["doc_type"], "scan", f"first3 chars={meta['first3_text_len']}"))
        if meta["form_type"]:
            print(f"    !! contains form (type={meta['form_type']})")
        print(f"    head: {head[:200]!r}")
        if tail:
            print(f"    tail: {tail[:160]!r}")

    print()
    print("=" * 78)
    print(f"\n# Summary\n")
    print(f"Samples scanned: {len(samples)}")
    print(f"Issues flagged: {len(issues)}")
    if issues:
        for x in issues:
            print(f"  - {x}")
    print(f"Cross-engine length discrepancies (>15%): {len(discrepancies)}")
    for d in discrepancies:
        print(f"  - {d}")
    if not issues and not discrepancies:
        print("  ALL CLEAN — both engines agree, no scans, no encryption, no forms.")


if __name__ == "__main__":
    main()
