"""Build eval_cases.jsonl for the retrieval sweep.

For each citation in a source doc that resolves to a target doc inside the
mount, emit one row whose query is the source sentence (citation token
stripped). Aggregates by (source_doc_id, sentence) so multi-cite sentences
produce a single case with multiple expected targets.

Output schema is consumed by arke.eval.sweep:
    {"query": str, "expected_doc_ids": list[str], "_source_doc_id": str}

Run on whichever mount you want to evaluate against (default: small).
"""
from __future__ import annotations
import json
import os
import random
import re
from collections import defaultdict
from pathlib import Path
from arke.corpora._paths import DATA, ENV_FILE



CACHE = DATA
CORPUS_FULL = CACHE / "comp_corpus"
MOUNT = CACHE / "comp_corpus_small"
OUT_FILE = MOUNT / "eval_cases.jsonl"
OUT_SAMPLE = MOUNT / "eval_cases_sample.jsonl"
SAMPLE_N = 200    # sweep round-trip is sequential — keep small for fast iteration
SAMPLE_SEED = 42

# Citation regex — frozen 15 EU + 22 UK patterns from extract_citations.py.
# Inlined here (not imported) so this file is self-contained for sweep ops.
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

UK_PATTERNS: list[tuple[str, str]] = [
    ("CAT_NC",      r"\[\s*(\d{4})\s*\]\s*CAT\s+(\d+)"),
    ("UKSC_NC",     r"\[\s*(\d{4})\s*\]\s*UKSC\s+(\d+)"),
    ("EWCA_CIV",    r"\[\s*(\d{4})\s*\]\s*EWCA\s+Civ\s+(\d+)"),
    ("EWCA_CRIM",   r"\[\s*(\d{4})\s*\]\s*EWCA\s+Crim\s+(\d+)"),
    ("EWHC",        r"\[\s*(\d{4})\s*\]\s*EWHC\s+(\d+)(?:\s*\(([A-Z][a-z]+)\))?"),
    ("UKHL",        r"\[\s*(\d{4})\s*\]\s*UKHL\s+(\d+)"),
    ("UKPC",        r"\[\s*(\d{4})\s*\]\s*UKPC\s+(\d+)"),
    ("AC_REPORT",   r"\[\s*\d{4}\s*\]\s+(?:\d+\s+)?AC\s+\d+"),
    ("WLR_REPORT",  r"\[\s*\d{4}\s*\]\s+(?:\d+\s+)?WLR\s+\d+"),
    ("BCC_REPORT",  r"\[\s*\d{4}\s*\]\s+BCC\s+\d+"),
    ("CH_REPORT",   r"\[\s*\d{4}\s*\]\s+(?:\d+\s+)?Ch\s+\d+"),
    ("BUS_LR",      r"\[\s*\d{4}\s*\]\s+Bus\s*LR\s+\d+"),
    ("STC_REPORT",  r"\[\s*\d{4}\s*\]\s+STC\s+\d+"),
    ("BPIR_REPORT", r"\[\s*\d{4}\s*\]\s+BPIR\s+\d+"),
    ("RPC_REPORT",  r"\[\s*\d{4}\s*\]\s+(?:\d+\s+)?RPC\s+\d+"),
    ("FSR_REPORT",  r"\[\s*\d{4}\s*\]\s+FSR\s+\d+"),
    ("LRPC_REPORT", r"\[\s*\d{4}\s*\]\s+(?:\d+\s+)?Lloyd's\s+Rep\s+\d+"),
    ("QB_REPORT",   r"\[\s*\d{4}\s*\]\s+(?:\d+\s+)?QB\s+\d+"),
    ("KB_REPORT",   r"\[\s*\d{4}\s*\]\s+(?:\d+\s+)?KB\s+\d+"),
    ("ECC_REPORT",  r"\[\s*\d{4}\s*\]\s+ECC\s+\d+"),
    ("ALL_ER",      r"\[\s*\d{4}\s*\]\s+(?:\d+\s+)?All\s*ER\s+\d+"),
    ("CAT_REF",     r"(?<![\d/])\b\d{1,5}(?:-\d+)?/\d{1,2}/\d{1,2}/\d{2}\b"),
]


def case_num_to_celex(key: str) -> list[str]:
    m = re.search(r"(?:([CT])-)?(\d+)/(\d{2,4})", key)
    if not m:
        return []
    p, n, y = m.groups()
    yr = ("19" + y if int(y) >= 50 else "20" + y) if len(y) == 2 else y
    n_pad = n.zfill(4)
    if p == "T":
        return [f"6{yr}TJ{n_pad}", f"6{yr}TO{n_pad}"]
    return [f"6{yr}CJ{n_pad}", f"6{yr}CO{n_pad}", f"6{yr}CC{n_pad}", f"6{yr}CV{n_pad}"]


def normalize_uk_neutral(key: str) -> str | None:
    m = re.search(r"\[\s*(\d{4})\s*\]\s+(CAT|UKSC|EWCA Civ|EWCA Crim|EWHC|UKHL|UKPC)\s+(\d+)(?:\s*\(([A-Z][a-z]+)\))?", key)
    if m:
        year, court, num, div = m.group(1), m.group(2), m.group(3), m.group(4) or ""
        court_compact = court.replace(" ", "-")
        return f"{year}-{court_compact}-{num}-{div}" if div else f"{year}-{court_compact}-{num}"
    return None


def normalize_cat_ref(key: str) -> str | None:
    m = re.match(r"^(\d{4})/(\d)/(\d{1,3})/(\d{2})$", key)
    return key if m else None


def find_citations_with_pos(text: str):
    """Yield (kind, normalized_key, raw_match, side, start, end) for every match."""
    for kind, pat in EU_PATTERNS:
        for m in re.finditer(pat, text):
            raw = m.group(0)
            key = raw.strip()
            if kind == "FREE_V_COMM" and FREE_V_COMM_BLOCKLIST.match(key):
                continue
            yield (kind, key, raw, "EU", m.start(), m.end())
    for kind, pat in UK_PATTERNS:
        for m in re.finditer(pat, text):
            raw = m.group(0)
            key = re.sub(r"\s+", " ", raw).strip()
            yield (kind, key, raw, "UK", m.start(), m.end())


def extract_sentence(text: str, start: int, end: int, ctx: int = 350) -> str:
    """Sentence containing [start, end). Bounded by '. ' or newline."""
    lo = max(0, start - ctx)
    hi = min(len(text), end + ctx)
    rel_s = start - lo
    rel_e = end - lo
    win = text[lo:hi]

    s_lo = 0
    for i in range(rel_s - 1, -1, -1):
        c = win[i]
        if c == "\n" or (c == "." and i + 1 < len(win) and win[i + 1] == " "):
            s_lo = i + 1
            break
    s_hi = len(win)
    for i in range(rel_e, len(win)):
        c = win[i]
        if c == "\n" or (c == "." and i + 1 < len(win) and win[i + 1] == " "):
            s_hi = i + 1
            break
    return win[s_lo:s_hi].strip()


def build_resolver(corpus_full_manifest: Path):
    """Build lookup tables from the FULL corpus manifest, return resolve()."""
    docs = []
    with corpus_full_manifest.open() as f:
        for line in f:
            docs.append(json.loads(line))

    celex_to_doc: dict[str, str] = {}
    ecli_to_doc: dict[str, str] = {}
    neutral_to_doc: dict[str, str] = {}
    case_ref_to_doc: dict[str, str] = {}

    for d in docs:
        if d.get("celex"):
            celex_to_doc[d["celex"]] = d["doc_id"]
        if d.get("ecli"):
            ecli_to_doc[d["ecli"]] = d["doc_id"]
        if d.get("neutral_citation"):
            n = normalize_uk_neutral(d["neutral_citation"])
            if n:
                neutral_to_doc[n] = d["doc_id"]
        if d.get("case_ref"):
            case_ref_to_doc[d["case_ref"]] = d["doc_id"]

    def resolve(side: str, kind: str, key: str) -> str | None:
        if side == "EU":
            if kind == "ECLI":
                return ecli_to_doc.get(key)
            if kind == "ALT_ECLI":
                return ecli_to_doc.get("ECLI:" + key)
            if kind in ("CASE_C", "CASE_OLD", "BARE_C", "BARE_T", "JOINED"):
                for cand in case_num_to_celex(key):
                    if cand in celex_to_doc:
                        return celex_to_doc[cand]
            if kind == "REGULATION":
                m = re.search(r"(\d+)/(\d{4})", key)
                if m:
                    n, y = m.groups()
                    return celex_to_doc.get(f"3{y}R{n.zfill(4)}")
            return None
        if side == "UK":
            n = normalize_uk_neutral(key)
            if n:
                return neutral_to_doc.get(n)
            n2 = normalize_cat_ref(key)
            if n2:
                return case_ref_to_doc.get(n2)
            return None
        return None

    return resolve


def main():
    resolve = build_resolver(CORPUS_FULL / "manifest.jsonl")

    small_docs = []
    with (MOUNT / "manifest.jsonl").open() as f:
        for line in f:
            small_docs.append(json.loads(line))
    small_ids = {d["doc_id"] for d in small_docs}

    sentence_to_targets: dict[tuple[str, str], set[str]] = defaultdict(set)
    n_matches = 0
    n_resolved = 0
    n_in_mount = 0

    for sd in small_docs:
        text_path = MOUNT / sd["corpus_path"]
        text = text_path.read_text(encoding="utf-8", errors="replace")

        # Group matches by their host sentence so multi-cite sentences yield
        # ONE case with all expected targets, and ALL citation tokens stripped.
        per_sentence: dict[str, dict] = defaultdict(lambda: {"targets": set(), "raws": set()})
        for kind, key, raw, side, start, end in find_citations_with_pos(text):
            n_matches += 1
            target = resolve(side, kind, key)
            if not target:
                continue
            n_resolved += 1
            if target == sd["doc_id"] or target not in small_ids:
                continue
            n_in_mount += 1
            sentence = extract_sentence(text, start, end)
            per_sentence[sentence]["targets"].add(target)
            per_sentence[sentence]["raws"].add(raw)

        for sentence, data in per_sentence.items():
            query = sentence
            for raw in data["raws"]:
                query = query.replace(raw, "[ref]")
            query = re.sub(r"\s+", " ", query).strip()
            if len(query) < 30:
                continue
            sentence_to_targets[(sd["doc_id"], query)] |= data["targets"]

    # Build the source→side map once for stratified sampling
    src_side: dict[str, str] = {d["doc_id"]: d["source"] for d in small_docs}

    rows: list[dict] = []
    for (src_id, query), targets in sentence_to_targets.items():
        rows.append({
            "query": query,
            "expected_doc_ids": sorted(targets),
            "_source_doc_id": src_id,
            "_source_side": src_side.get(src_id, ""),
        })

    _atomic_write_jsonl(OUT_FILE, rows)

    # Stratified sample by source side. Floor each stratum at 1 if non-empty.
    rng = random.Random(SAMPLE_SEED)
    by_side: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_side[r["_source_side"]].append(r)
    total = sum(len(v) for v in by_side.values())
    sample: list[dict] = []
    for side, group in by_side.items():
        share = max(1, round(SAMPLE_N * len(group) / total))
        sample.extend(rng.sample(group, min(share, len(group))))
    rng.shuffle(sample)
    sample = sample[:SAMPLE_N]
    _atomic_write_jsonl(OUT_SAMPLE, sample)

    sample_uk = sum(1 for r in sample if r["_source_side"] == "uk_cat")
    sample_eu = sum(1 for r in sample if r["_source_side"] == "eu_cellar")
    print(f"=== eval_cases.jsonl built ===")
    print(f"  citation matches:         {n_matches}")
    print(f"  resolved (any target):    {n_resolved}")
    print(f"  resolved AND in mount:    {n_in_mount}")
    print(f"  unique cases:             {len(rows)}")
    print(f"  full output:              {OUT_FILE}")
    print(f"  sampled ({SAMPLE_N}, UK/EU={sample_uk}/{sample_eu}): {OUT_SAMPLE}")


def _atomic_write_jsonl(path: Path, rows: list[dict]) -> None:
    tmp = path.with_suffix(".jsonl.tmp")
    with tmp.open("w") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


if __name__ == "__main__":
    main()
