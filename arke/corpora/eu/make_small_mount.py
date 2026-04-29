"""Build the small-subset physical mount (~300 docs, stratified by source).

Used for fast tuning sweeps: chunk_size/overlap × alpha × k iterations against
a self-contained dense citation subgraph. Embed cost per config ≈ $0.01.

Selection: per-source top-N by total citation degree. Stratified UK/EU split
prevents EU dominance — pure top-by-degree gives 7/293 because EU CELLAR is
densely self-citing. The product needs UK-flavoured retrieval, so UK is
floor-set at 100.

  comp_corpus/        — 3,377 docs — production demo
  comp_corpus_cited/  — 2,311 docs — full cited subset, EVAL-tier
  comp_corpus_small/  —   ~300 docs — tuning-tier, this script
"""
from __future__ import annotations
import json
import os
import shutil
from pathlib import Path
from arke.corpora._paths import DATA, ENV_FILE



CACHE = DATA
CORPUS = CACHE / "comp_corpus"
CORPUS_SMALL = CACHE / "comp_corpus_small"
TOP_UK = 100
TOP_EU = 200


def safe_symlink(src: Path, dst: Path):
    if dst.is_symlink() or dst.exists():
        dst.unlink()
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.symlink_to(src)


def main():
    docs = []
    with (CORPUS / "manifest.jsonl").open() as f:
        for line in f:
            docs.append(json.loads(line))

    def degree_key(d):
        return (
            -(d.get("cite_in_count", 0) + d.get("cite_out_count", 0)),
            -d.get("cite_in_count", 0),
        )

    uk_docs = sorted([d for d in docs if d["source"] == "uk_cat"], key=degree_key)[:TOP_UK]
    eu_docs = sorted([d for d in docs if d["source"] == "eu_cellar"], key=degree_key)[:TOP_EU]
    small = uk_docs + eu_docs
    small_ids = {d["doc_id"] for d in small}

    if CORPUS_SMALL.exists():
        shutil.rmtree(CORPUS_SMALL)
    CORPUS_SMALL.mkdir()

    for d in small:
        rel = d["corpus_path"]
        src = CORPUS / rel
        if src.is_symlink():
            src = src.resolve()
        dst = CORPUS_SMALL / rel
        safe_symlink(src, dst)

    full_graph = CORPUS / "citation_graph.jsonl"
    small_graph = CORPUS_SMALL / "citation_graph.jsonl"
    n_full = 0
    n_kept = 0
    tmp = small_graph.with_suffix(".jsonl.tmp")
    with full_graph.open() as fin, tmp.open("w") as fout:
        for line in fin:
            n_full += 1
            edge = json.loads(line)
            if edge["source_doc_id"] in small_ids and edge["target_doc_id"] in small_ids:
                fout.write(line)
                n_kept += 1
        fout.flush(); os.fsync(fout.fileno())
    os.replace(tmp, small_graph)

    from collections import Counter
    cin: Counter[str] = Counter()
    cout: Counter[str] = Counter()
    with small_graph.open() as f:
        for line in f:
            e = json.loads(line)
            cin[e["target_doc_id"]] += e["count"]
            cout[e["source_doc_id"]] += e["count"]

    tmp = (CORPUS_SMALL / "manifest.jsonl.tmp")
    with tmp.open("w") as f:
        for d in small:
            d2 = dict(d)
            d2["cite_in_count_full"] = d.get("cite_in_count", 0)
            d2["cite_out_count_full"] = d.get("cite_out_count", 0)
            d2["cite_in_count"] = cin.get(d["doc_id"], 0)
            d2["cite_out_count"] = cout.get(d["doc_id"], 0)
            f.write(json.dumps(d2, ensure_ascii=False) + "\n")
        f.flush(); os.fsync(f.fileno())
    os.replace(tmp, CORPUS_SMALL / "manifest.jsonl")

    isolated = sum(1 for d in small if cin.get(d["doc_id"], 0) == 0 and cout.get(d["doc_id"], 0) == 0)
    print(f"=== comp_corpus_small/ built ===")
    print(f"  docs:                    {len(small)}")
    print(f"  isolated (no edge):      {isolated}")
    print(f"  citation graph edges:    {n_kept}/{n_full} ({100*n_kept/n_full:.2f}% retained)")
    print(f"  uk / eu split:           {sum(1 for d in small if d['source']=='uk_cat')} / {sum(1 for d in small if d['source']=='eu_cellar')}")
    print(f"  mount root:              {CORPUS_SMALL}")


if __name__ == "__main__":
    main()
