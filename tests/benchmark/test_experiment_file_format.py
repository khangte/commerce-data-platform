"""CSV와 Parquet Arm이 같은 Result Hash를 내면서 파일 크기는 다른지 검증한다."""

from __future__ import annotations

import os
from datetime import UTC, datetime

import pytest

from src.benchmark.config import RunConfig, ScaleProfile, new_benchmark_id
from src.benchmark.experiments.file_format import (
    FILE_FORMAT_SCENARIO,
    run_file_format_experiment,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set RUN_SEAWEEDFS_INTEGRATION=1 with a live SeaweedFS container.",
)


def test_csv_and_parquet_arms_match_hash_with_different_file_sizes() -> None:
    """같은 고정 Row를 두 형식으로 읽으면 Result Hash는 같고 Byte 수는 다르다."""
    scale = ScaleProfile(name="S", order_count=200, random_seed=20260921)
    benchmark_id = new_benchmark_id("file_format", scale.name, datetime.now(UTC))
    config = RunConfig(
        scenario=FILE_FORMAT_SCENARIO,
        scale=scale,
        benchmark_id=benchmark_id,
        repeats=5,
        is_cold_run=False,
    )

    arms = run_file_format_experiment(config)

    assert arms["csv"].result_hash == arms["parquet"].result_hash
    assert arms["csv"].counts.rows_scanned == arms["parquet"].counts.rows_scanned == 200
    assert arms["csv"].counts.input_bytes != arms["parquet"].counts.input_bytes
