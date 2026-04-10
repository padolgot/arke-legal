from ..core.config import Config
from .metrics import EvalMetrics, EvalResult
from .sweep import SweepRow, run_sweep


class Eval:

    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg

    async def sweep(self, level: str, limit: int, source_path: str = "") -> list[SweepRow]:
        return await run_sweep(self._cfg, level, limit, source_path)


__all__ = [
    "Eval",
    "SweepRow",
    "EvalMetrics",
    "EvalResult",
]
