"""
Extract all citations from any text doc — UK and EU patterns combined.

Works on every text in cat_skeleton/text/ + eu_pool/text/.
Output: citations.jsonl, one row per (source_doc_id, citation_kind, citation_key, evidence_window).
"""

from __future__ import annotations
import json
import os
import re
from pathlib import Path
from arke.corpora._paths import DATA, ENV_FILE



CACHE = DATA
CORPUS_DIR = CACHE / "comp_corpus"
MANIFEST = CORPUS_DIR / "manifest.jsonl"
OUT_FILE = CORPUS_DIR / "citations_raw.jsonl"

# === EU patterns (frozen 15-regex from cat_scraper, proven) ===
EU_PATTERNS: list[tuple[str, str]] = [
    ("ECLI",        r"ECLI:EU:[CT]:\d{4}:\d+"),
    ("CASE_C",      r"Case[s]?\s+[CT]-\d+/\d{2,4}(?:\s+(?:P|PR|RENV|R|REC))?"),
    ("CASE_OLD",    r"Case[s]?\s+\d+/\d{2,4}(?!\d)"),
    ("ECR",         r"\[\s*\d{4}\s*\]\s*ECR\s+(?:I-|II-)?\d+"),
    ("CMLR",        r"\[\s*\d{4}\s*\]\s*\d+\s*CMLR\s+\d+"),
    ("JOINED",      r"Joined\s+Cases?\s+[CT]?-?\d+/\d{2,4}"),
    ("BARE_C",      r"(?<![\w-])(?<!Case )(?<!Cases )C-\d+/\d{2,4}(?!\d)"),
    ("BARE_T",      r"(?<![\w-])(?<!Case )(?<!Cases )T-\d+/\d{2,4}(?!\d)"),
    ("ALT_ECLI",    r"(?<!ECLI:)EU:[CT]:\d{4}:\d+(?!\d)"),
    ("FREE_V_COMM", r"([A-Z][A-Za-z][\w\.\-' ]{2,40})\s+v\.?\s+(?:European\s+)?Commission(?!\s*[\[\(])"),
    ("COMM_AT",     r"(?:Case\s+)?AT\.\d{4,5}"),
    ("COMM_COMP",   r"COMP/[A-Z]?\.?\d+(?:\.\d+)?"),
    ("COMM_DEC",    r"(?:Commission\s+)?Decision\s+\d+/\d+/(?:EC|EEC|EU)"),
    ("AG_OPINION",  r"Opinion\s+of\s+(?:Advocate\s+General|AG)\s+\w+"),
    ("REGULATION",  r"Regulation\s+(?:\(?(?:EC|EU|EEC)\)?\s*)?(?:No\.?\s*)?\d+/\d{4}"),
]

FREE_V_COMM_BLOCKLIST = re.compile(
    r"^(?:Co\.?\s+(?:AG|Ltd)|Others|Inc\.?|Corp\.?|Limited|plc)\s+v",
    re.IGNORECASE,
)

# === UK patterns ===
UK_PATTERNS: list[tuple[str, str]] = [
    # Neutral citations
    ("CAT_NC",      r"\[\s*(\d{4})\s*\]\s*CAT\s+(\d+)"),
    ("UKSC_NC",     r"\[\s*(\d{4})\s*\]\s*UKSC\s+(\d+)"),
    ("EWCA_CIV",    r"\[\s*(\d{4})\s*\]\s*EWCA\s+Civ\s+(\d+)"),
    ("EWCA_CRIM",   r"\[\s*(\d{4})\s*\]\s*EWCA\s+Crim\s+(\d+)"),
    ("EWHC",        r"\[\s*(\d{4})\s*\]\s*EWHC\s+(\d+)(?:\s*\(([A-Z][a-z]+)\))?"),
    ("UKHL",        r"\[\s*(\d{4})\s*\]\s*UKHL\s+(\d+)"),
    ("UKPC",        r"\[\s*(\d{4})\s*\]\s*UKPC\s+(\d+)"),
    # Common-law reporters
    ("AC_REPORT",     r"\[\s*\d{4}\s*\]\s+(?:\d+\s+)?AC\s+\d+"),
    ("WLR_REPORT",    r"\[\s*\d{4}\s*\]\s+(?:\d+\s+)?WLR\s+\d+"),
    ("BCC_REPORT",    r"\[\s*\d{4}\s*\]\s+BCC\s+\d+"),
    ("CH_REPORT",     r"\[\s*\d{4}\s*\]\s+(?:\d+\s+)?Ch\s+\d+"),                # Chancery
    ("BUS_LR",        r"\[\s*\d{4}\s*\]\s+Bus\s*LR\s+\d+"),                      # Business Law Reports
    ("STC_REPORT",    r"\[\s*\d{4}\s*\]\s+STC\s+\d+"),                            # Simon's Tax Cases
    ("BPIR_REPORT",   r"\[\s*\d{4}\s*\]\s+BPIR\s+\d+"),                           # Bankruptcy
    ("RPC_REPORT",    r"\[\s*\d{4}\s*\]\s+(?:\d+\s+)?RPC\s+\d+"),                # Reports of Patent Cases
    ("FSR_REPORT",    r"\[\s*\d{4}\s*\]\s+FSR\s+\d+"),                            # Fleet Street Reports
    ("LRPC_REPORT",   r"\[\s*\d{4}\s*\]\s+(?:\d+\s+)?Lloyd's\s+Rep\s+\d+"),      # Lloyd's
    ("QB_REPORT",     r"\[\s*\d{4}\s*\]\s+(?:\d+\s+)?QB\s+\d+"),                  # Queen's Bench
    ("KB_REPORT",     r"\[\s*\d{4}\s*\]\s+(?:\d+\s+)?KB\s+\d+"),                  # King's Bench
    ("ECC_REPORT",    r"\[\s*\d{4}\s*\]\s+ECC\s+\d+"),                            # European Commercial Cases
    ("ALL_ER",        r"\[\s*\d{4}\s*\]\s+(?:\d+\s+)?All\s*ER\s+\d+"),           # All England Reports
    # CAT case reference: <case_num>/<section>/<court>/<2-digit-year>
    # case_num can be 1-4 digits (sometimes >4 with sub-cases: 1517/11/7/22)
    ("CAT_REF",     r"(?<![\d/])\b\d{1,5}(?:-\d+)?/\d{1,2}/\d{1,2}/\d{2}\b"),
]


def extract_eu(text: str) -> list[tuple[str, str]]:
    """Return list of (kind, key) tuples for EU citations in text."""
    out = []
    for kind, pat in EU_PATTERNS:
        for m in re.finditer(pat, text):
            key = m.group(0).strip()
            if kind == "FREE_V_COMM" and FREE_V_COMM_BLOCKLIST.match(key):
                continue
            out.append((kind, key))
    return out


def extract_uk(text: str) -> list[tuple[str, str]]:
    """UK citations — return list of (kind, normalized_key)."""
    out = []
    for kind, pat in UK_PATTERNS:
        for m in re.finditer(pat, text):
            full = m.group(0)
            # Normalize whitespace
            key = re.sub(r"\s+", " ", full).strip()
            out.append((kind, key))
    return out


def main():
    # Load manifest
    docs = []
    with MANIFEST.open() as f:
        for line in f:
            docs.append(json.loads(line))
    print(f"Total docs to scan: {len(docs)}")

    n_uk_in = 0
    n_eu_in = 0
    cite_total_uk = 0
    cite_total_eu = 0

    tmp = OUT_FILE.with_suffix(".jsonl.tmp")
    with tmp.open("w") as out:
        for i, doc in enumerate(docs, 1):
            text_path = CORPUS_DIR / doc["corpus_path"]
            if not text_path.exists():
                continue
            try:
                text = text_path.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue

            eu_cites = extract_eu(text)
            uk_cites = extract_uk(text)

            for kind, key in eu_cites:
                out.write(json.dumps({
                    "source_doc_id": doc["doc_id"],
                    "source_kind": doc["source"],
                    "cite_target_kind": kind,
                    "cite_target_key": key,
                    "side": "EU",
                }, ensure_ascii=False) + "\n")
                cite_total_eu += 1

            for kind, key in uk_cites:
                out.write(json.dumps({
                    "source_doc_id": doc["doc_id"],
                    "source_kind": doc["source"],
                    "cite_target_kind": kind,
                    "cite_target_key": key,
                    "side": "UK",
                }, ensure_ascii=False) + "\n")
                cite_total_uk += 1

            if doc["source"] == "uk_cat":
                n_uk_in += 1
            else:
                n_eu_in += 1

            if i % 500 == 0:
                print(f"  scanned {i}/{len(docs)}  cites_uk={cite_total_uk}  cites_eu={cite_total_eu}", flush=True)

        out.flush()
        os.fsync(out.fileno())
    os.replace(tmp, OUT_FILE)

    print()
    print(f"=== Citation extraction done ===")
    print(f"  UK docs scanned: {n_uk_in}")
    print(f"  EU docs scanned: {n_eu_in}")
    print(f"  Total UK citations found: {cite_total_uk}")
    print(f"  Total EU citations found: {cite_total_eu}")
    print(f"  Output: {OUT_FILE}")


if __name__ == "__main__":
    main()
