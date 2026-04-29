"""
Build the cited-subset physical mount.

Mirrors comp_corpus/ structure for the 2,311 docs with cite_in_count >= 1.
Output: comp_corpus_cited/ with own symlink tree + filtered manifest + filtered graph.

Both mounts coexist:
  comp_corpus/         — full corpus (3,377 docs) — mount for production demo
  comp_corpus_cited/   — cited subset (2,311 docs) — mount for EVAL/sweep
"""

from __future__ import annotations
import json
import os
import shutil
from pathlib import Path
from arke.corpora._paths import DATA, ENV_FILE



CACHE = DATA
CORPUS = CACHE / "comp_corpus"
CORPUS_CITED = CACHE / "comp_corpus_cited"


def safe_symlink(src: Path, dst: Path):
    if dst.is_symlink() or dst.exists():
        dst.unlink()
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.symlink_to(src)


def main():
    # Load full manifest
    docs = []
    with (CORPUS / "manifest.jsonl").open() as f:
        for line in f:
            docs.append(json.loads(line))

    cited = [d for d in docs if d.get("cite_in_count", 0) >= 1]
    cited_ids = {d["doc_id"] for d in cited}

    # Recreate clean
    if CORPUS_CITED.exists():
        shutil.rmtree(CORPUS_CITED)
    CORPUS_CITED.mkdir()

    # Symlink farm — point each cited doc into comp_corpus_cited/ at same relative path
    for d in cited:
        rel = d["corpus_path"]
        src = CORPUS / rel
        # Resolve to underlying real file (not symlink-of-symlink)
        if src.is_symlink():
            src = src.resolve()
        dst = CORPUS_CITED / rel
        safe_symlink(src, dst)

    # Filtered manifest
    tmp = (CORPUS_CITED / "manifest.jsonl.tmp")
    with tmp.open("w") as f:
        for d in cited:
            f.write(json.dumps(d, ensure_ascii=False) + "\n")
        f.flush(); os.fsync(f.fileno())
    os.replace(tmp, CORPUS_CITED / "manifest.jsonl")

    # Filter citation graph to edges where BOTH source and target are in cited subset
    full_graph = CORPUS / "citation_graph.jsonl"
    cited_graph = CORPUS_CITED / "citation_graph.jsonl"
    n_full = 0
    n_kept = 0
    tmp = cited_graph.with_suffix(".jsonl.tmp")
    with full_graph.open() as fin, tmp.open("w") as fout:
        for line in fin:
            n_full += 1
            edge = json.loads(line)
            if edge["source_doc_id"] in cited_ids and edge["target_doc_id"] in cited_ids:
                fout.write(line)
                n_kept += 1
        fout.flush(); os.fsync(fout.fileno())
    os.replace(tmp, cited_graph)

    # Recompute cite_in/out within cited subset (since we dropped edges)
    from collections import Counter
    cin: Counter[str] = Counter()
    cout: Counter[str] = Counter()
    with cited_graph.open() as f:
        for line in f:
            e = json.loads(line)
            cin[e["target_doc_id"]] += e["count"]
            cout[e["source_doc_id"]] += e["count"]

    # Write recomputed manifest with subset-internal counts
    tmp = (CORPUS_CITED / "manifest.jsonl.tmp")
    with tmp.open("w") as f:
        for d in cited:
            d2 = dict(d)
            # Preserve full-corpus counts under different name for reference
            d2["cite_in_count_full"] = d.get("cite_in_count", 0)
            d2["cite_out_count_full"] = d.get("cite_out_count", 0)
            d2["cite_in_count"] = cin.get(d["doc_id"], 0)
            d2["cite_out_count"] = cout.get(d["doc_id"], 0)
            f.write(json.dumps(d2, ensure_ascii=False) + "\n")
        f.flush(); os.fsync(f.fileno())
    os.replace(tmp, CORPUS_CITED / "manifest.jsonl")

    print(f"=== comp_corpus_cited/ built ===")
    print(f"  symlinked text files: {len(cited)}")
    print(f"  manifest entries:     {len(cited)}")
    print(f"  citation graph edges: {n_kept}/{n_full} ({100*n_kept/n_full:.1f}% retained)")
    print(f"  mount root:           {CORPUS_CITED}")


if __name__ == "__main__":
    main()
