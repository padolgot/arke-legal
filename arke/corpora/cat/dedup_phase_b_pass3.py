#!/usr/bin/env python3
"""Phase B pass 3: final attempt to fold residual UNKEYED entries by feeding
ALL their context samples (not just one). For ECR/CMLR refs whose first
context window had no case-num, samples 2 and 3 might.
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
INPUT = ROOT / "eu_clusters_phaseB.jsonl"
OUTPUT = ROOT / "eu_clusters_phaseB.jsonl"
RAW_DIR = ROOT / "phaseB_pass3_raw"
ENV_FILE = ENV_FILE

MODEL = "gpt-4o"
BASE_URL = "https://api.openai.com/v1/chat/completions"
CHUNK_SIZE = 60
SLEEP_S = 60

SYSTEM_PROMPT = """\
You are mapping unkeyed citation strings to existing canonical case clusters.
Each input has MULTIPLE context windows showing different places it appears in
the corpus. Examine all windows. If ANY window contains an identifier that
matches one of the existing clusters' keys, return that key. Otherwise null.

CRITICAL RULE: rely ONLY on the provided contexts. Do NOT use memory.

Output STRICTLY this JSON object:
{"mappings": [{"input_index": <int>, "matches_key": "<key>" or null, "evidence": "<quote>"}]}
Every input MUST appear exactly once.
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


def main() -> int:
    env = load_env()
    api_key = env.get("CLOUD_API_KEY")
    if not api_key:
        print("ERROR: CLOUD_API_KEY", file=sys.stderr)
        return 1

    all_clusters = [json.loads(l) for l in INPUT.open()]
    keyed = [c for c in all_clusters if c["key_kind"] != "UNKEYED"]
    unkeyed = [c for c in all_clusters if c["key_kind"] == "UNKEYED"]
    print(f"residual UNKEYED to retry: {len(unkeyed)}", file=sys.stderr)

    lookup_lines = []
    for c in keyed:
        nh = (c["names_seen"][0] if c["names_seen"] else "")
        nh = re.sub(r"\s*(EU|ECLI|,)\s*$", "", nh).strip()
        lookup_lines.append(f"  {c['canonical_key']} :: {nh[:60]}")
    lookup_str = "\n".join(lookup_lines)

    chunks = [unkeyed[i:i + CHUNK_SIZE] for i in range(0, len(unkeyed), CHUNK_SIZE)]
    print(f"chunks: {len(chunks)} of {CHUNK_SIZE}\n", file=sys.stderr)

    all_mappings = []
    start = 0
    for ci, chunk in enumerate(chunks):
        label = f"phaseB_pass3_chunk{ci:02d}"
        print(f"chunk {ci + 1}/{len(chunks)}", file=sys.stderr)
        lines = ["EXISTING CLUSTERS:", lookup_str, "", f"UNKEYED ({start}..):"]
        for i, c in enumerate(chunk):
            lines.append(f"\n[{start + i}] {c['canonical_key']!r}")
            for j, ctx in enumerate(c.get("context_samples", [])):
                lines.append(f"  ctx{j}: {ctx[:240]!r}")
        user = "\n".join(lines)
        size = len(SYSTEM_PROMPT) + len(user)
        print(f"  prompt: {size:,} chars (~{size // 4:,}t)", file=sys.stderr)
        body = {"model": MODEL, "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user},
        ], "temperature": 0}
        try:
            t = time.time()
            with httpx.Client(timeout=600) as cli:
                r = cli.post(BASE_URL,
                             headers={"Authorization": f"Bearer {api_key}",
                                      "Content-Type": "application/json"},
                             json=body)
                r.raise_for_status()
            d = r.json()
            content = d["choices"][0]["message"]["content"]
            usage = d.get("usage", {})
            print(f"  latency={time.time() - t:.1f}s prompt={usage.get('prompt_tokens')}t completion={usage.get('completion_tokens')}t",
                  file=sys.stderr)
            RAW_DIR.mkdir(parents=True, exist_ok=True)
            (RAW_DIR / f"{label}.txt").write_text(content)
            s = re.sub(r"^```(?:json)?\s*|\s*```$", "", content.strip())
            obj = json.loads(s)
            mp = obj.get("mappings", [])
            print(f"  -> {len(mp)} mappings", file=sys.stderr)
            all_mappings.extend(mp)
        except Exception as e:
            print(f"  !! failed: {e}", file=sys.stderr)
        start += len(chunk)
        if ci < len(chunks) - 1:
            time.sleep(SLEEP_S)

    keyed_by_key = {c["canonical_key"]: c for c in keyed}
    folded = 0
    standalone = []
    mp_by_idx = {m["input_index"]: m for m in all_mappings if "input_index" in m}
    for idx, uc in enumerate(unkeyed):
        m = mp_by_idx.get(idx)
        target = m.get("matches_key") if m else None
        if target and target in keyed_by_key:
            tgt = keyed_by_key[target]
            tgt["members"].extend(uc["members"])
            tgt["total_mentions"] += uc["total_mentions"]
            for n in uc["names_seen"]:
                if n not in tgt["names_seen"]:
                    tgt["names_seen"].append(n)
            tgt.setdefault("folded_from_unkeyed", []).append({
                "src_key": uc["canonical_key"],
                "evidence": (m.get("evidence") or "")[:120],
            })
            folded += 1
        else:
            standalone.append(uc)

    final = list(keyed_by_key.values()) + standalone
    final.sort(key=lambda c: -c["total_mentions"])
    OUTPUT.write_text("")
    with OUTPUT.open("w", encoding="utf-8") as f:
        for c in final:
            f.write(json.dumps(c, ensure_ascii=False) + "\n")
    print(f"\nfolded this pass:           {folded}", file=sys.stderr)
    print(f"standalone UNKEYED final:   {len(standalone)}", file=sys.stderr)
    print(f"total final clusters:       {len(final)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
