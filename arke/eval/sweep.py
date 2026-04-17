"""Sweep — run eval across preset configs and rank by MRR.

Each config row = full arke-server restart with that config.
Communicates through mailbox like any real client.

Usage:
    python -m arke.eval.sweep --workspace legalbench --level medium --limit 50
"""
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from arke.server import mailbox
from arke.server.config import Config
from arke.server.models import Models
from arke.server.types import Doc

from .gen import EvalCase, make_cases
from .presets import get_preset


@dataclass(frozen=True)
class EvalMetrics:
    precision: float
    recall: float
    mrr: float


@dataclass(frozen=True)
class SweepRow:
    cfg: Config
    metrics: EvalMetrics


def run(workspace: str, level: str, limit: int) -> list[SweepRow]:
    base_cfg = Config.from_env().resolved()
    configs = get_preset(level, base_cfg)

    # Generate eval cases using base config (one-time)
    print("generating eval cases...")
    cases = _generate_cases(workspace, base_cfg, limit)
    if not cases:
        print("error: no eval cases generated", file=sys.stderr)
        sys.exit(1)
    print(f"generated {len(cases)} cases\n")

    rows: list[SweepRow] = []
    for idx, cfg in enumerate(configs):
        print(f"[{idx + 1}/{len(configs)}] chunk={cfg.chunk_size} overlap={cfg.overlap} alpha={cfg.alpha} k={cfg.k}")
        metrics = _run_row(workspace, cfg, cases)
        rows.append(SweepRow(cfg=cfg, metrics=metrics))
        print(f"  → precision={metrics.precision:.3f} recall={metrics.recall:.3f} MRR={metrics.mrr:.3f}\n")

    rows.sort(key=lambda r: r.metrics.mrr, reverse=True)
    _print_table(rows)
    return rows


def _generate_cases(workspace: str, cfg: Config, limit: int) -> list[EvalCase]:
    """Start server, sample chunks, generate questions, stop server."""
    proc = _start_server(workspace, cfg)
    try:
        _wait_ready()
        # ask server to sample chunks for case generation
        msg_id = mailbox.send({"cmd": "sample", "limit": limit})
        response = mailbox.receive(msg_id)
        if not response or not response.get("ok"):
            raise RuntimeError(f"sample failed: {response}")

        # load models for case generation (cloud preferred for quality)
        models = Models.load(cfg)
        from arke.server.types import Chunk
        chunks = [
            Chunk(doc_id=c["doc_id"], chunk_index=c["chunk_index"], clean=c["clean"], head=c["head"], tail=c["tail"])
            for c in response["chunks"]
        ]
        return make_cases(models.llm, chunks, limit)
    finally:
        _stop_server(proc)


def _run_row(workspace: str, cfg: Config, cases: list[EvalCase]) -> EvalMetrics:
    proc = _start_server(workspace, cfg)
    try:
        _wait_ready()
        results = []
        for case in cases:
            msg_id = mailbox.send({"cmd": "ask", "query": case.query})
            response = mailbox.receive(msg_id)
            hits = response.get("citations", []) if response and response.get("ok") else []
            results.append((hits, case.expected_key))
        return _score(results)
    finally:
        _stop_server(proc)


def _start_server(workspace: str, cfg: Config) -> subprocess.Popen:
    env = {
        **os.environ,
        "ARKE_WORKSPACE": workspace,
        "CHUNK_SIZE": str(cfg.chunk_size),
        "OVERLAP": str(cfg.overlap),
        "ALPHA": str(cfg.alpha),
        "K": str(cfg.k),
    }
    proc = subprocess.Popen(["arke-server"], env=env)
    return proc


def _wait_ready(timeout: float = 60.0) -> None:
    """Poll ping until server responds or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            msg_id = mailbox.send({"cmd": "ping"})
            response = mailbox.receive.__wrapped__(msg_id, poll_timeout=2.0) if hasattr(mailbox.receive, "__wrapped__") else None
            # fallback: try direct file check
            outbox = Path.home() / ".arke" / "outbox" / f"{msg_id}.json"
            time.sleep(0.5)
            if outbox.exists():
                outbox.unlink(missing_ok=True)
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


def _score(results: list[tuple[list, str]]) -> EvalMetrics:
    n = len(results)
    if n == 0:
        return EvalMetrics(precision=0.0, recall=0.0, mrr=0.0)

    sum_p = sum_r = sum_rr = 0.0
    for hits, expected_key in results:
        hit_keys = [f"{h['source']}:{h.get('chunk_index', '')}" for h in hits]
        matched = sum(1 for k in hit_keys if k == expected_key)

        sum_p += matched / len(hits) if hits else 0.0
        sum_r += float(matched > 0)

        for i, k in enumerate(hit_keys):
            if k == expected_key:
                sum_rr += 1.0 / (i + 1)
                break

    return EvalMetrics(precision=sum_p / n, recall=sum_r / n, mrr=sum_rr / n)


def _print_table(rows: list[SweepRow]) -> None:
    header = f"{'chunk':>6} {'overlap':>7} {'alpha':>6} {'k':>4} {'prec':>7} {'recall':>7} {'MRR':>7}"
    print(f"\n{'Sweep Results (sorted by MRR)':^50}")
    print(header)
    print("-" * len(header))
    best_mrr = rows[0].metrics.mrr if rows else 0.0
    for r in rows:
        mrr_str = f"{r.metrics.mrr:.3f}"
        if r.metrics.mrr == best_mrr:
            mrr_str += " <-- best"
        print(
            f"{r.cfg.chunk_size:>6} {r.cfg.overlap:>7.1f} {r.cfg.alpha:>6.1f} {r.cfg.k:>4}"
            f" {r.metrics.precision:>7.3f} {r.metrics.recall:>7.3f} {mrr_str}"
        )
    print()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--level", default="medium")
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()
    run(args.workspace, args.level, args.limit)
