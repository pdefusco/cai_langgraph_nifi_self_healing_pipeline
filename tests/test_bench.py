"""The no-LLM bench (eval/bench.py, NFH-94): every stage timed, and nothing of it left behind."""

import io
import os

import config
from flows import packs
from nifi_healer.eval import bench


def test_it_times_every_stage_and_puts_everything_back(tenant, tmp_path):
    before = (dict(packs.PACKS), {name: getattr(config, name) for name in ("MOCK_WORLD_FILE", "DB_PATH", "ARMED", "FLOWS_OUT_DIR")})

    found = bench.run([50], repeats=1, directory=str(tmp_path / "bench"), out=io.StringIO())

    (result,) = found
    assert set(result["seconds"]) == set(bench.STAGES)
    assert 45 <= result["sizes"]["processors"] <= 50 and result["sizes"]["plan_ok"]
    assert result["sizes"]["prompt_tokens"] > 0
    assert "fleet monitor" in bench.table(found)
    assert not any(n.startswith("bench-") for n in os.listdir(config.FLOWS_OUT_DIR))
    assert (dict(packs.PACKS), {name: getattr(config, name) for name in before[1]}) == before
