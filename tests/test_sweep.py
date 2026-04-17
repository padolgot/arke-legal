import asyncio

import pytest
from dotenv import load_dotenv

pytest.skip("obsolete: Arke class removed during sdb unification; rewrite in phase 2", allow_module_level=True)

from arke import Arke, Config  # noqa: E402

load_dotenv()


@pytest.mark.skip(reason="requires running Postgres + Ollama with matching embedding_dim")
def test_sweep_fast():
    cfg = Config.from_env()
    rows = asyncio.run(Arke.sweep(cfg, "fast", limit=5))
    assert len(rows) > 0
    for row in rows:
        assert row.metrics.recall >= 0
        assert row.metrics.precision >= 0
