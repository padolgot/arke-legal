"""Sweep — run retrieval eval across preset configs and rank by recall.

For each config: stop the server, restart with the new config (full re-ingest
from cold), run all eval cases through the server's `search` handler, score.

Restart-per-config is the design — Arke is a real-time process whose config
is immutable for its lifetime. No in-process swaps.

Usage:
    arke-eval --workspace cat --cases path/to/cases.jsonl --level medium
"""
import json
import logging
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from arke.server import mailbox, workspace
from arke.server.config import Config

from .presets import get_preset

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EvalCase:
    """One retrieval probe — query plus the set of doc_ids the corpus
    expects the system to recover. For citation-graph eval, expected_doc_ids
    is the set of cases the source judgment cited."""
    query: str
    expected_doc_ids: set[str]


@dataclass(frozen=True)
class EvalMetrics:
    recall: float        # mean fraction of expected docs found in top-k
    mrr: float           # mean reciprocal rank of the FIRST expected match


@dataclass(frozen=True)
class SweepRow:
    cfg: Config
    metrics: EvalMetrics


def run(workspace_name: str, cases_path: Path, level: str) -> list[SweepRow]:
    base_cfg = Config.from_env().resolved()
    configs = get_preset(level, base_cfg)

    cases = _load_cases(cases_path)
    if not cases:
        logger.error("no eval cases loaded from %s", cases_path)
        sys.exit(1)
    logger.info("loaded %d cases from %s", len(cases), cases_path)

    rows: list[SweepRow] = []
    for idx, cfg in enumerate(configs):
        logger.info(
            "[%d/%d] chunk=%d overlap=%.2f alpha=%.2f k=%d",
            idx + 1, len(configs), cfg.chunk_size, cfg.overlap, cfg.alpha, cfg.k,
        )
        metrics = _run_row(workspace_name, cfg, cases)
        rows.append(SweepRow(cfg=cfg, metrics=metrics))
        logger.info("  → recall=%.3f MRR=%.3f", metrics.recall, metrics.mrr)

    rows.sort(key=lambda r: r.metrics.recall, reverse=True)
    _print_table(rows)
    return rows


def _load_cases(path: Path) -> list[EvalCase]:
    """Read JSONL: one {"query": str, "expected_doc_ids": [str, ...]} per line."""
    cases: list[EvalCase] = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        cases.append(EvalCase(
            query=obj["query"],
            expected_doc_ids=set(obj["expected_doc_ids"]),
        ))
    return cases


def _run_row(workspace_name: str, cfg: Config, cases: list[EvalCase]) -> EvalMetrics:
    proc = _start_server(workspace_name, cfg)
    ws_path = workspace.path_for(workspace_name)
    try:
        _wait_ready(proc, ws_path)
        results: list[tuple[list[str], set[str]]] = []
        for case in cases:
            msg_id = mailbox.send({"cmd": "search", "query": case.query}, ws_path)
            response = mailbox.receive(msg_id, ws_path)
            citations = response.get("citations", []) if response and response.get("ok") else []
            # Preserve order (server returns sorted by score) and dedupe doc-ids
            # while keeping the FIRST occurrence — MRR cares about earliest hit.
            seen: set[str] = set()
            retrieved: list[str] = []
            for c in citations:
                did = c["doc_id"]
                if did in seen:
                    continue
                seen.add(did)
                retrieved.append(did)
            results.append((retrieved, case.expected_doc_ids))
        return _score(results)
    finally:
        _stop_server(proc)


def _start_server(workspace_name: str, cfg: Config) -> subprocess.Popen:
    env = {
        **os.environ,
        "ARKE_WORKSPACE": workspace_name,
        "CHUNK_SIZE": str(cfg.chunk_size),
        "OVERLAP": str(cfg.overlap),
        "ALPHA": str(cfg.alpha),
        "K": str(cfg.k),
    }
    return subprocess.Popen(["arke-server"], env=env)


def _wait_ready(proc: subprocess.Popen, ws_path: Path, timeout: float = 1200.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"arke-server exited early (code={proc.returncode})")
        try:
            msg_id = mailbox.send({"cmd": "ping"}, ws_path)
            response = mailbox.receive(msg_id, ws_path)
            if response and response.get("pong"):
                return
        except Exception:
            pass
        time.sleep(1.0)
    raise RuntimeError("arke-server did not start in time")


def _stop_server(proc: subprocess.Popen) -> None:
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


def _score(results: list[tuple[list[str], set[str]]]) -> EvalMetrics:
    n = len(results)
    if n == 0:
        return EvalMetrics(recall=0.0, mrr=0.0)

    sum_recall = sum_rr = 0.0
    for retrieved, expected in results:
        if not expected:
            continue
        sum_recall += len(set(retrieved) & expected) / len(expected)
        for i, did in enumerate(retrieved):
            if did in expected:
                sum_rr += 1.0 / (i + 1)
                break

    return EvalMetrics(recall=sum_recall / n, mrr=sum_rr / n)


def _print_table(rows: list[SweepRow]) -> None:
    header = f"{'chunk':>6} {'overlap':>7} {'alpha':>6} {'k':>4} {'recall':>7} {'MRR':>7}"
    print(f"\n{'Sweep Results (sorted by recall)':^50}")
    print(header)
    print("-" * len(header))
    best_recall = rows[0].metrics.recall if rows else 0.0
    for r in rows:
        recall_str = f"{r.metrics.recall:.3f}"
        if r.metrics.recall == best_recall:
            recall_str += " <-- best"
        print(
            f"{r.cfg.chunk_size:>6} {r.cfg.overlap:>7.2f} {r.cfg.alpha:>6.2f} {r.cfg.k:>4}"
            f" {recall_str:>7} {r.metrics.mrr:>7.3f}"
        )
    print()


def main() -> None:
    import argparse

    from dotenv import find_dotenv, load_dotenv
    load_dotenv(find_dotenv())
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--cases", required=True, type=Path,
                        help="JSONL file: {query, expected_doc_ids} per line")
    parser.add_argument("--level", default="medium", help="fast | medium | thorough")
    args = parser.parse_args()
    run(args.workspace, args.cases, args.level)


if __name__ == "__main__":
    main()
