#!/usr/bin/env python3
"""Phase B retry — runs ONLY on standalone UNKEYED entries left from a prior
Phase B run, with smaller chunks and longer sleep to stay under TPM ceiling.

Same logic as dedup_phase_b.py but:
  * Input  = eu_clusters_phaseB.jsonl  (previous Phase B output)
  * Filter = entries still flagged UNKEYED (folds preserved)
  * Chunks = 100 (vs 200) → ~16k tokens per call
  * Sleep  = 60s between calls to respect TPM
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from pathlib import Path

import httpx
from arke.corpora._paths import DATA, ENV_FILE



ROOT = DATA / "cat_skeleton"
INPUT = ROOT / "eu_clusters_phaseB.jsonl"     # output of prior Phase B
OUTPUT = ROOT / "eu_clusters_phaseB.jsonl"    # overwrite
RAW_DIR = ROOT / "phaseB_retry_raw"
ENV_FILE = ENV_FILE

MODEL = os.environ.get("PHASEB_MODEL", "gpt-4o")
BASE_URL = "https://api.openai.com/v1/chat/completions"
CHUNK_SIZE = 100
SLEEP_S = 60
TIMEOUT_S = 600

SYSTEM_PROMPT = """\
You are mapping unkeyed citation strings (ECR/CMLR report references, free-form
case names) to existing canonical case clusters. Each existing cluster has a key
(case number / ECLI / Commission code) and a typical name hint.

For each input UNKEYED citation, examine its CONTEXT window. If the context
explicitly contains an identifier that matches one of the existing clusters'
keys, return that key. Otherwise return null.

CRITICAL RULE: rely ONLY on the provided context. Do NOT use your own memory
of EU case law. If the context does not contain an explicit key match, return
null — even if you "know" what case it is.

Output STRICTLY this JSON object (no prose, no markdown fence):
{"mappings": [
  {"input_index": <int>, "matches_key": "<existing_cluster_key>" or null, "evidence": "<short quote from context>"}
]}

Every input MUST appear exactly once in mappings.
"""


def load_env() -> dict:
    out = {}
    for line in ENV_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def http_post(api_key: str, body: dict) -> dict:
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    with httpx.Client(timeout=TIMEOUT_S) as c:
        r = c.post(BASE_URL, headers=headers, json=body)
        r.raise_for_status()
    return r.json()


def build_lookup_str(keyed: list[dict]) -> str:
    lines = []
    for c in keyed:
        name_hint = c["names_seen"][0] if c["names_seen"] else ""
        name_hint = re.sub(r"\s*(EU|ECLI|,)\s*$", "", name_hint).strip()
        lines.append(f"  {c['canonical_key']} :: {name_hint[:60]}")
    return "\n".join(lines)


def build_user_prompt(lookup: str, chunk: list[dict], start_idx: int) -> str:
    lines = [
        "EXISTING CLUSTERS (canonical_key :: name_hint):",
        lookup,
        "",
        f"UNKEYED CITATIONS (indices {start_idx}..{start_idx + len(chunk) - 1}):",
    ]
    for i, c in enumerate(chunk):
        ctx = c["context_samples"][0] if c["context_samples"] else "(no context)"
        ctx = ctx[:240]
        lines.append(f"[{start_idx + i}] {c['canonical_key']!r} | ctx: {ctx!r}")
    return "\n".join(lines)


def parse_response(raw: str) -> list[dict]:
    s = raw.strip()
    s = re.sub(r"^```(?:json)?\s*", "", s)
    s = re.sub(r"\s*```$", "", s)
    obj = json.loads(s)
    return obj.get("mappings", []) if isinstance(obj, dict) else obj


def call_llm(api_key: str, system: str, user: str, label: str) -> str:
    body = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
    }
    t = time.time()
    data = http_post(api_key, body)
    elapsed = time.time() - t
    content = data["choices"][0]["message"]["content"]
    usage = data.get("usage", {})
    print(f"  [{label}] latency={elapsed:.1f}s prompt={usage.get('prompt_tokens')}t completion={usage.get('completion_tokens')}t",
          file=sys.stderr)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    (RAW_DIR / f"{label}.txt").write_text(content, encoding="utf-8")
    return content


def main() -> int:
    env = load_env()
    api_key = env.get("CLOUD_API_KEY")
    if not api_key:
        print("ERROR: CLOUD_API_KEY not set", file=sys.stderr)
        return 1

    all_clusters = [json.loads(l) for l in INPUT.open()]
    keyed = [c for c in all_clusters if c["key_kind"] != "UNKEYED"]
    unkeyed = [c for c in all_clusters if c["key_kind"] == "UNKEYED"]
    print(f"input: {len(all_clusters)} clusters", file=sys.stderr)
    print(f"  keyed:   {len(keyed)}", file=sys.stderr)
    print(f"  standalone UNKEYED to retry: {len(unkeyed)}", file=sys.stderr)

    lookup_str = build_lookup_str(keyed)
    print(f"lookup table size: {len(lookup_str):,} chars", file=sys.stderr)

    chunks = [unkeyed[i:i + CHUNK_SIZE] for i in range(0, len(unkeyed), CHUNK_SIZE)]
    print(f"chunks: {len(chunks)} of up to {CHUNK_SIZE} entries each", file=sys.stderr)
    print(f"sleep between calls: {SLEEP_S}s\n", file=sys.stderr)

    all_mappings: list[dict] = []
    failed_chunks: list[int] = []
    start = 0
    for i, chunk in enumerate(chunks):
        label = f"phaseB_retry_chunk{i:02d}"
        print(f"chunk {i + 1}/{len(chunks)} ({len(chunk)} entries)...", file=sys.stderr)
        user_prompt = build_user_prompt(lookup_str, chunk, start)
        size_chars = len(SYSTEM_PROMPT) + len(user_prompt)
        print(f"  prompt size: {size_chars:,} chars (~{size_chars // 4:,} tokens)", file=sys.stderr)
        try:
            raw = call_llm(api_key, SYSTEM_PROMPT, user_prompt, label)
            mappings = parse_response(raw)
            print(f"  -> {len(mappings)} mappings returned", file=sys.stderr)
            all_mappings.extend(mappings)
        except Exception as e:
            print(f"  !! chunk {i} failed: {e}", file=sys.stderr)
            failed_chunks.append(i)
        start += len(chunk)
        if i < len(chunks) - 1:
            time.sleep(SLEEP_S)

    # Apply mappings: fold matched UNKEYED into existing keyed clusters.
    keyed_by_key = {c["canonical_key"]: c for c in keyed}
    folded_count = 0
    standalone_unkeyed: list[dict] = []
    mapping_by_idx = {m["input_index"]: m for m in all_mappings if "input_index" in m}

    for idx, uc in enumerate(unkeyed):
        m = mapping_by_idx.get(idx)
        target = m.get("matches_key") if m else None
        if target and target in keyed_by_key:
            tgt = keyed_by_key[target]
            tgt["members"].extend(uc["members"])
            tgt["total_mentions"] += uc["total_mentions"]
            for n in uc["names_seen"]:
                if n not in tgt["names_seen"]:
                    tgt["names_seen"].append(n)
            for ctx in uc["context_samples"]:
                if len(tgt["context_samples"]) < 5:
                    tgt["context_samples"].append(ctx)
            tgt.setdefault("folded_from_unkeyed", []).append({
                "src_key": uc["canonical_key"],
                "evidence": (m.get("evidence") or "")[:120],
            })
            folded_count += 1
        else:
            standalone_unkeyed.append(uc)

    final = list(keyed_by_key.values()) + standalone_unkeyed
    final.sort(key=lambda c: -c["total_mentions"])

    if OUTPUT.exists():
        OUTPUT.unlink()
    with OUTPUT.open("w", encoding="utf-8") as f:
        for c in final:
            f.write(json.dumps(c, ensure_ascii=False) + "\n")

    print(f"\n=== Phase B retry done ===", file=sys.stderr)
    print(f"folded into existing clusters this pass: {folded_count}", file=sys.stderr)
    print(f"standalone UNKEYED remaining:            {len(standalone_unkeyed)}", file=sys.stderr)
    print(f"failed chunks (need another pass):       {failed_chunks}", file=sys.stderr)
    print(f"final cluster count:                     {len(final)}", file=sys.stderr)
    print(f"output: {OUTPUT}", file=sys.stderr)
    return 0 if not failed_chunks else 2


if __name__ == "__main__":
    sys.exit(main())
