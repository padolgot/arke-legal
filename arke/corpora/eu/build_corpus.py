"""
Phase: build hierarchical layout for the unified corpus.

Layout:
  <umbrella>/corpora/
  ├── uk/cat/{category}/{year}/{slug}.txt
  └── eu/{court}/{primary_subject}/{year}/{celex}.txt

Sources:
  uk/cat: from cat_skeleton/manifest_skeleton.jsonl
  eu:     from eu_pool/manifest_text.jsonl, filtered to in-scope works

Out-of-scope EU works (in manifest but not in current works.jsonl) are dropped.

Symlinks back to original text files in cat_skeleton/text/ and eu_pool/text/.
A unified manifest at comp_corpus/manifest.jsonl with hierarchical paths.
"""

from __future__ import annotations
import json
import os
import re
from pathlib import Path
from arke.corpora._paths import DATA, ENV_FILE



CACHE = DATA
UK_DIR = CACHE / "cat_skeleton"
EU_DIR = CACHE / "eu_pool"
CORPUS_DIR = CACHE / "comp_corpus"

# Court detection from CELEX letter: 6{YEAR}{COURT_LETTERS}{NUM}
# CJ/CC/CO/CV = Court of Justice (judgments / orders / opinions / view AG)
# TJ/TO = General Court (Tribunal)
# FJ/FO = Civil Service Tribunal (legacy)
COURT_FROM_CELEX = {
    "CJ": "ecj", "CO": "ecj", "CC": "ecj", "CV": "ecj",
    "TJ": "general-court", "TO": "general-court",
    "FJ": "civil-service-tribunal", "FO": "civil-service-tribunal",
}

# Subject priority for primary tag
SUBJECT_PRIORITY = [
    "Dominant position",
    "Agreements, decisions and concerted practices",
    "Concerted practices",
    "Exclusive agreements",
]
SUBJECT_FOLDER = {
    "Dominant position": "dominance",
    "Agreements, decisions and concerted practices": "cartels-agreements",
    "Concerted practices": "concerted-practices",
    "Exclusive agreements": "exclusive-agreements",
    "Competition": "competition",  # fallback for CONC-only-tagged
}


def safe_slug(s: str | None, max_len: int = 60) -> str:
    if not s:
        return "untitled"
    s = re.sub(r"[^A-Za-z0-9._-]+", "-", s).strip("-")
    return (s or "untitled")[:max_len]


# Common abbreviations to compact UK party names. Order matters: longer phrases
# first so "Office of Communications" wins over generic substitutions.
UK_PARTY_ABBREV = [
    (r"\bCompetition and Markets Authority\b", "CMA"),
    (r"\bOffice of Communications\b", "Ofcom"),
    (r"\bOffice of Fair Trading\b", "OFT"),
    (r"\bCompetition Commission\b", "CC"),
    (r"\bGas and Electricity Markets Authority\b", "Ofgem"),
    (r"\bSecretary of State for [\w\s]+", "SoS"),
    (r"\bDirector General of Fair Trading\b", "DGFT"),
    # Strip corporate suffixes WITH any trailing dot — fixes "Inc." leaving "."
    (r"\b(?:Limited|Ltd|Plc|PLC|Inc|Corporation|Corp|Holdings?|Group)\b\.?", ""),
    (r"\b&\s*Co\b\.?", ""),
    (r"\b(?:Mr|Mrs|Ms|Dr)\b\.?\s+", ""),
]


def _truncate_at_word(s: str, max_len: int = 70) -> str:
    """Cut at last word boundary so we never end mid-word ('Apple-Re...')."""
    if len(s) <= max_len:
        return s
    cut = s[:max_len]
    if "-" in cut:
        cut = cut.rsplit("-", 1)[0]
    return cut.strip("-")


def _slugify(s: str) -> str:
    """Turn a cleaned party-string into a dash-slug. Drop ALL punctuation
    (was bug: '.' allowed through created '-.-' artifacts)."""
    s = re.sub(r"\s+v\.?\s+", " v ", s)
    s = re.sub(r"[^\w\s&]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace(" v ", "-v-").replace(" ", "-")
    s = re.sub(r"-+", "-", s).strip("-")
    return s


def party_slug_uk(parties: str | None) -> str:
    if not parties:
        return "untitled"
    s = parties.strip()
    for pat, repl in UK_PARTY_ABBREV:
        s = re.sub(pat, repl, s, flags=re.IGNORECASE)
    return _truncate_at_word(_slugify(s)) or "untitled"


def party_slug_eu(title: str | None) -> str:
    if not title:
        return "untitled"
    s = title
    # CELLAR titles: "Judgment of the Court of 13 February 1979.#Hoffmann-La Roche..."
    if ".#" in s:
        s = s.split(".#", 1)[1]
    s = re.sub(r"\s+of\s+the\s+European\s+Communit(?:y|ies)\b.*$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+of\s+the\s+European\s+Union\b.*$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"#.*$", "", s)
    s = re.sub(r"\s*&\s*Co\b\.?\s*(?:AG|KG|Ltd|GmbH)?", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+(?:GmbH|AG|SA|NV|SpA|SRL|Ltd|Inc)\b\.?", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\bothers\b", "Ors", s, flags=re.IGNORECASE)
    return _truncate_at_word(_slugify(s)) or "untitled"


def celex_to_case_num(celex: str) -> str | None:
    """Convert CELEX 61976CJ0085 → '85-76'; 62012CJ0382 → 'C-382-12'; 61991TJ0083 → 'T-83-91'."""
    m = re.match(r"^6(\d{4})([A-Z]{1,2})0*(\d+)$", celex)
    if not m:
        return None
    year, court, num = m.groups()
    yr_short = year[-2:]
    if court.startswith("T"):
        return f"T-{num}-{yr_short}"
    if int(year) >= 1989:
        return f"C-{num}-{yr_short}"
    return f"{num}-{yr_short}"


def derive_eu_path(rec: dict) -> tuple[str, str, str, str] | None:
    """Returns (court, subject_folder, year, filename_stem) or None.
    filename_stem = '{Party}_{CaseNum}' (e.g. 'Hoffmann-La-Roche-v-Commission_85-76')."""
    celex = rec.get("celex")
    if not celex:
        return None
    m = re.match(r"^6(\d{4})([A-Z]{1,2})\d+", celex)
    if not m:
        return None
    year = m.group(1)
    court_letters = m.group(2)
    court = COURT_FROM_CELEX.get(court_letters, "other")

    subjects = rec.get("subject_matters") or []
    primary = None
    for s in SUBJECT_PRIORITY:
        if s in subjects:
            primary = s
            break
    if not primary:
        if "Competition" in subjects:
            primary = "Competition"
        elif subjects:
            primary = subjects[0]
        else:
            primary = "Competition"
    folder = SUBJECT_FOLDER.get(primary, safe_slug(primary, 30).lower())

    if rec.get("resource_type") == "OPIN_AG":
        court = court + "-ag"

    cellar_uuid = rec["cellar_uuid"]
    return court, folder, year, cellar_uuid[:16]


def derive_uk_path(rec: dict) -> tuple[str, str, str]:
    """Returns (category, year, filename_stem). Filename = sha1 hash (robust)."""
    cat = rec.get("category", "uncategorized")
    cat_short = {
        "A_apex_UKSC": "uksc",
        "A_apex_CoA": "court-of-appeal",
        "B_judgment_or_ruling": "judgment-or-ruling",
    }.get(cat, safe_slug(cat, 30).lower())
    date = rec.get("date") or ""
    year = date[:4] if date and date[:4].isdigit() else "unknown"
    return cat_short, year, rec["sha1"][:16]


def safe_symlink(src: Path, dst: Path):
    if dst.is_symlink() or dst.exists():
        dst.unlink()
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.symlink_to(src)


def main():
    # Recreate clean
    if CORPUS_DIR.exists():
        import shutil
        shutil.rmtree(CORPUS_DIR)
    CORPUS_DIR.mkdir()

    # In-scope = subject-matter scope (works.jsonl) UNION whitelisted (via=whitelist)
    scope_uuids: set[str] = set()
    with (EU_DIR / "works.jsonl").open() as f:
        for line in f:
            w = json.loads(line)
            if w.get("cellar_uuid"):
                scope_uuids.add(w["cellar_uuid"])
    with (EU_DIR / "manifest.jsonl").open() as f:
        for line in f:
            r = json.loads(line)
            if r.get("via") == "whitelist" and r.get("cellar_uuid"):
                scope_uuids.add(r["cellar_uuid"])

    # Process UK CAT
    uk_records = []
    with (UK_DIR / "manifest_skeleton.jsonl").open() as f:
        for line in f:
            rec = json.loads(line)
            text_src = UK_DIR / rec.get("text_path", "")
            if not text_src.exists():
                continue
            cat, year, stem = derive_uk_path(rec)
            rel_path = Path("uk/cat") / cat / year / f"{stem}.txt"
            dst = CORPUS_DIR / rel_path
            safe_symlink(text_src, dst)
            uk_records.append({
                "doc_id": f"uk_{rec['sha1'][:12]}",
                "source": "uk_cat",
                "corpus_path": str(rel_path),
                "canonical_id": rec.get("neutral_citation") or rec.get("case_ref"),
                "neutral_citation": rec.get("neutral_citation"),
                "case_ref": rec.get("case_ref"),
                "party_slug": party_slug_uk(rec.get("parties")),
                "title": rec.get("parties"),
                "date": rec.get("date"),
                "doc_type": rec.get("doc_type"),
                "category": rec.get("category"),
                "url": rec.get("pdf_absolute_url"),
                "text_chars": rec.get("text_chars"),
                "page_count": rec.get("n_pages"),
                "sha1": rec.get("sha1"),
            })

    # Process EU
    eu_records = []
    n_eu_dropped_out_of_scope = 0
    n_eu_no_path = 0
    with (EU_DIR / "manifest_text.jsonl").open() as f:
        for line in f:
            rec = json.loads(line)
            uuid = rec["cellar_uuid"]
            if uuid not in scope_uuids:
                n_eu_dropped_out_of_scope += 1
                continue
            text_src = EU_DIR / rec.get("text_path", "")
            if not text_src.exists():
                continue
            derived = derive_eu_path(rec)
            if not derived:
                n_eu_no_path += 1
                continue
            court, subj_folder, year, stem = derived
            rel_path = Path("eu") / court / subj_folder / year / f"{stem}.txt"
            dst = CORPUS_DIR / rel_path
            safe_symlink(text_src, dst)
            celex = rec.get("celex")
            case_num = celex_to_case_num(celex) if celex else None
            eu_records.append({
                "doc_id": f"eu_{uuid[:8]}",
                "source": "eu_cellar",
                "corpus_path": str(rel_path),
                "canonical_id": case_num or celex,
                "celex": celex,
                "ecli": rec.get("ecli"),
                "case_num": case_num,
                "party_slug": party_slug_eu(rec.get("title")),
                "title": rec.get("title"),
                "date": rec.get("date"),
                "doc_type": rec.get("resource_type"),
                "subject_matters": rec.get("subject_matters"),
                "primary_subject": subj_folder,
                "court": court,
                "year": year,
                "text_chars": rec.get("text_size"),
                "format_origin": rec.get("english_manif_format"),
                "via": rec.get("via", "cellar_rest"),
                "cellar_uuid": uuid,
            })

    # Write unified manifest
    manifest_path = CORPUS_DIR / "manifest.jsonl"
    tmp = manifest_path.with_suffix(".jsonl.tmp")
    with tmp.open("w") as f:
        for r in uk_records + eu_records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, manifest_path)

    print("=== Hierarchical layout built ===")
    print(f"  UK CAT:                {len(uk_records)} docs")
    print(f"  EU CELLAR:             {len(eu_records)} docs")
    print(f"  Total:                 {len(uk_records) + len(eu_records)}")
    print(f"  EU dropped OUT-of-scope: {n_eu_dropped_out_of_scope}")
    print(f"  EU dropped no-path:    {n_eu_no_path}")
    print(f"  Manifest:              {manifest_path}")
    print(f"  Root:                  {CORPUS_DIR}")


if __name__ == "__main__":
    main()
