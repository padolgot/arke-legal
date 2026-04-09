from ..core.config import MnemeConfig
from .metrics import EvalMetrics
from .sweep import SweepRow, run_sweep


class Eval:
    """Evaluation harness over a Mneme configuration. Holds the base config
    and runs sweeps against it. Each sweep creates fresh Mneme instances
    internally, so the same Eval can be reused across runs."""

    def __init__(self, cfg: MnemeConfig) -> None:
        self._cfg = cfg

    async def sweep(self, level: str, limit: int, source_path: str) -> list[SweepRow]:
        return await run_sweep(self._cfg, level, limit, source_path)


__all__ = ["Eval", "SweepRow", "EvalMetrics"]
