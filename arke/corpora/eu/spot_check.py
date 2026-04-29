"""
Spot-check regex citation extraction recall.

Pick N random docs. For each, compare regex citations (already in
citations_raw.jsonl) vs LLM-extracted citations. Compute recall.
"""

from __future__ import annotations
import json
import os
import random
import re
from pathlib import Path

import requests
from arke.corpora._paths import DATA, ENV_FILE



CACHE = DATA
CORPUS = CACHE / "comp_corpus"
MANIFEST = CORPUS / "manifest.jsonl"
RAW_CITES = CORPUS / "citations_raw.jsonl"

ENV_FILE = ENV_FILE
CLOUD_API_KEY = None
for line in ENV_FILE.read_text().splitlines() if ENV_FILE.exists() else []:
    if line.startswith("CLOUD_API_KEY="):
        CLOUD_API_KEY = line.split("=", 1)[1].strip().strip('"').strip("'")

MODEL = "gpt-4o"
N_DOCS = 5


def llm_extract(text: str) -> list[str]:
    """Ask LLM to list every case citation in text."""
    # Truncate very long
    if len(text) > 80000:
        text = text[:80000]
    prompt = f"""Extract every legal case citation from the following text.

Return ONLY a JSON array of citation strings, nothing else. Include:
- UK neutral citations: [YYYY] CAT N, [YYYY] EWCA Civ N, [YYYY] UKSC N, [YYYY] EWHC N, [YYYY] UKHL N
- UK case refs: NNNN/N/NN/NN
- UK old-style: [YYYY] AC N, [YYYY] WLR N, [YYYY] BCC N
- EU ECLIs: ECLI:EU:C:YYYY:N, ECLI:EU:T:YYYY:N
- EU case numbers: Case C-N/YY, Case T-N/YY, Case N/YY (pre-1989)
- EU reports: [YYYY] ECR I-N, [YYYY] N CMLR N
- Commission: AT.NNNNN, COMP/...
- Regulations: Regulation (EC) No N/YYYY

Output format: ["citation1", "citation2", ...]
Do NOT include explanations.

TEXT:
{text}
"""
    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {CLOUD_API_KEY}",
                 "Content-Type": "application/json"},
        json={"model": MODEL, "messages": [{"role": "user", "content": prompt}],
              "temperature": 0, "max_tokens": 4000},
        timeout=120,
    )
    r.raise_for_status()
    content = r.json()["choices"][0]["message"]["content"].strip()
    # Strip code fences
    content = re.sub(r"^```(?:json)?\s*|\s*```$", "", content, flags=re.M).strip()
    try:
        out = json.loads(content)
        if isinstance(out, list):
            return [str(x).strip() for x in out if x]
    except json.JSONDecodeError:
        pass
    return []


def normalize_for_compare(s: str) -> str:
    """Strip whitespace + lowercase to allow loose matching."""
    return re.sub(r"\s+", "", s.lower())


def main():
    random.seed(42)
    docs = []
    with MANIFEST.open() as f:
        for line in f:
            docs.append(json.loads(line))

    # Take 5 random UK docs (focus on UK side since regex is the same for both)
    uk_docs = [d for d in docs if d["source"] == "uk_cat"]
    sample = random.sample(uk_docs, N_DOCS)

    # Pre-load regex citations for these docs
    raw_by_doc: dict[str, set[str]] = {}
    with RAW_CITES.open() as f:
        for line in f:
            r = json.loads(line)
            sd = r["source_doc_id"]
            raw_by_doc.setdefault(sd, set()).add(normalize_for_compare(r["cite_target_key"]))

    print(f"=== Spot-check: regex vs LLM on {N_DOCS} random UK docs ===\n")
    total_regex = 0
    total_llm = 0
    total_overlap = 0
    total_llm_only = 0
    total_regex_only = 0

    for d in sample:
        text_path = CORPUS / d["corpus_path"]
        text = text_path.read_text(encoding="utf-8", errors="replace")
        regex_set = raw_by_doc.get(d["doc_id"], set())
        try:
            llm_list = llm_extract(text)
        except Exception as e:
            print(f"LLM fail for {d['doc_id']}: {e}")
            continue
        llm_set = {normalize_for_compare(x) for x in llm_list}

        overlap = regex_set & llm_set
        only_regex = regex_set - llm_set
        only_llm = llm_set - regex_set
        recall_vs_llm = len(overlap) / len(llm_set) if llm_set else 0
        precision_vs_llm = len(overlap) / len(regex_set) if regex_set else 0

        total_regex += len(regex_set)
        total_llm += len(llm_set)
        total_overlap += len(overlap)
        total_llm_only += len(only_llm)
        total_regex_only += len(only_regex)

        title = (d.get("title") or d.get("neutral_citation") or d["doc_id"])[:60]
        print(f"--- {title} ---")
        print(f"  regex: {len(regex_set)} | LLM: {len(llm_set)} | overlap: {len(overlap)}")
        print(f"  recall_vs_llm={recall_vs_llm:.0%}  precision_vs_llm={precision_vs_llm:.0%}")
        if only_llm:
            samples = list(only_llm)[:5]
            print(f"  LLM-only (regex MISSED): {samples}")
        if only_regex:
            samples = list(only_regex)[:3]
            print(f"  regex-only (LLM MISSED): {samples}")
        print()

    print("=" * 60)
    print(f"AGGREGATE over {N_DOCS} docs:")
    print(f"  regex total: {total_regex}")
    print(f"  LLM total:   {total_llm}")
    print(f"  overlap:     {total_overlap}")
    print(f"  regex MISSED (LLM only): {total_llm_only}")
    print(f"  LLM MISSED (regex only): {total_regex_only}")
    if total_llm:
        print(f"  REGEX RECALL vs LLM: {total_overlap/total_llm:.1%}")


if __name__ == "__main__":
    main()
