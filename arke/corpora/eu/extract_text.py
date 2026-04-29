"""
Phase 3: extract plain text from raw/ html / xhtml / xml files.

Output: text/{cellar_uuid}.txt
Manifest update: append text_path + text_size to each manifest record.
"""

from __future__ import annotations
import json
import os
import re
from pathlib import Path
from arke.corpora._paths import DATA, ENV_FILE



OUT_DIR = DATA / "eu_pool"
RAW_DIR = OUT_DIR / "raw"
TEXT_DIR = OUT_DIR / "text"
MANIFEST_FILE = OUT_DIR / "manifest.jsonl"
TEXT_MANIFEST = OUT_DIR / "manifest_text.jsonl"

TEXT_DIR.mkdir(parents=True, exist_ok=True)

# HTML/XHTML strip — robust regex chain. No BeautifulSoup dependency.
SCRIPT_STYLE = re.compile(r"<(?:script|style)\b[^>]*>.*?</(?:script|style)>", re.I | re.S)
TAG = re.compile(r"<[^>]+>")
WHITESPACE = re.compile(r"\s+")
ENTITIES = {
    "&nbsp;": " ", "&amp;": "&", "&lt;": "<", "&gt;": ">", "&quot;": '"',
    "&apos;": "'", "&#160;": " ", "&#8217;": "'", "&#8216;": "'", "&#8220;": '"',
    "&#8221;": '"', "&#8211;": "-", "&#8212;": "—", "&#8230;": "...",
}


def html_to_text(html: str) -> str:
    # Drop script/style blocks entirely
    s = SCRIPT_STYLE.sub(" ", html)
    # Drop all tags
    s = TAG.sub(" ", s)
    # Decode common entities
    for ent, repl in ENTITIES.items():
        s = s.replace(ent, repl)
    # Numeric entities best-effort
    s = re.sub(r"&#(\d+);", lambda m: chr(int(m.group(1))) if int(m.group(1)) < 0x110000 else " ", s)
    s = re.sub(r"&#x([0-9a-fA-F]+);", lambda m: chr(int(m.group(1), 16)) if int(m.group(1), 16) < 0x110000 else " ", s)
    # Normalize whitespace; preserve paragraph breaks via newlines
    s = WHITESPACE.sub(" ", s)
    return s.strip()


def write_atomic(path: Path, text: str):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def pdf_to_text(path: Path) -> str:
    import pypdfium2 as pdfium
    doc = pdfium.PdfDocument(str(path))
    pages = []
    for p in doc:
        pages.append(p.get_textpage().get_text_range())
    return "\n\n".join(pages).strip()


def main():
    n_ok = 0
    n_skip = 0
    n_err = 0
    with MANIFEST_FILE.open() as f, TEXT_MANIFEST.open("w") as out:
        for line in f:
            rec = json.loads(line)
            uuid = rec["cellar_uuid"]
            fmt = rec.get("english_manif_format")
            raw_path = OUT_DIR / rec["raw_path"]

            if not raw_path.exists():
                n_err += 1
                continue
            try:
                if fmt in ("html", "xhtml", "xml"):
                    raw = raw_path.read_text(encoding="utf-8", errors="replace")
                    text = html_to_text(raw)
                elif fmt in ("pdf", "pdfa1a"):
                    text = pdf_to_text(raw_path)
                else:
                    n_skip += 1
                    continue
                if len(text) < 200:
                    n_err += 1
                    continue
                text_path = TEXT_DIR / f"{uuid}.txt"
                write_atomic(text_path, text)
                rec["text_path"] = f"text/{uuid}.txt"
                rec["text_size"] = len(text)
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                n_ok += 1
            except Exception as e:
                print(f"ERR {uuid}: {type(e).__name__}: {e}", flush=True)
                n_err += 1

    print(f"Extracted: {n_ok} ok / {n_skip} skipped / {n_err} errors")


if __name__ == "__main__":
    main()
