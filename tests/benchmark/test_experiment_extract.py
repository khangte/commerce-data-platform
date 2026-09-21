"""Full Extract와 Incremental Extract가 같은 T1 상태에서 같은 Result Hash를 내는지 검증한다."""

from __future__ import annotations

import os
from datetime import UTC, datetime

import pytest

from src.benchmark.config import RunConfig, ScaleProfile, new_benchmark_id
from src.benchmark.experiments.extract import EXTRACT_SCENARIO, run_extract_experiment

_INTEGRATION_READY = (
    os.environ.get("RUN_POSTGRES_INTEGRATION") == "1"
    and os.environ.get("RUN_SEAWEEDFS_INTEGRATION") == "1"
)
pytestmark = pytest.mark.skipif(
    not _INTEGRATION_READY,
    reason="Set RUN_POSTGRES_INTEGRATION=1 and RUN_SEAWEEDFS_INTEGRATION=1 with live containers.",
)


def test_full_and_incremental_arms_reach_the_same_result_hash() -> None:
    """작은 Order Count에서 두 Arm이 같은 Bronze 상태·Result Hash에 도달한다."""
    scale = ScaleProfile(name="S", order_count=3, random_seed=20260921)
    benchmark_id = new_benchmark_id("extract", scale.name, datetime.now(UTC))
    config = RunConfig(
        scenario=EXTRACT_SCENARIO,
        scale=scale,
        benchmark_id=benchmark_id,
        repeats=5,
        is_cold_run=False,
        parameters={"change_rate": 0.5, "cursor_range": "t0-t1"},
    )

    arms = run_extract_experiment(config)

    assert arms["full"].result_hash == arms["incremental"].result_hash
    assert arms["full"].counts.rows_scanned is not None
    assert arms["incremental"].counts.rows_scanned is not None
    assert arms["full"].counts.rows_scanned > arms["incremental"].counts.rows_scanned
