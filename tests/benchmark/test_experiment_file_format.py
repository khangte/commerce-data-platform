"""CSV와 Parquet Arm이 같은 Result Hash를 내면서 파일 크기는 다른지 검증한다."""

from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime

import duckdb
import pytest

from src.benchmark.config import RunConfig, ScaleProfile, new_benchmark_id
from src.benchmark.experiments.file_format import (
    FILE_FORMAT_SCENARIO,
    _hash_rows_with_payload_size,
    run_file_format_experiment,
)
from src.benchmark.runner import has_invalid_runs, run_experiment
from src.common.row_hash import canonical_row_json

requires_seaweedfs = pytest.mark.skipif(
    os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set RUN_SEAWEEDFS_INTEGRATION=1 with a live SeaweedFS container.",
)


def test_hash_rows_uses_arrow_batches_without_changing_canonical_result() -> None:
    """Arrow Batch 경로도 기존 Canonical JSON Hash·Row 수·Byte 수를 보존한다."""
    with duckdb.connect(":memory:") as connection:
        cursor = connection.execute(
            "SELECT * FROM (VALUES (2, 'busan'), (1, 'seoul')) AS sellers(seller_no, city) "
            "ORDER BY seller_no"
        )
        result_hash, row_count, payload_bytes = _hash_rows_with_payload_size(cursor)

    expected_rows = [
        {"seller_no": 1, "city": "seoul"},
        {"seller_no": 2, "city": "busan"},
    ]
    payloads = [canonical_row_json(row).encode("utf-8") for row in expected_rows]
    digest = hashlib.sha256()
    for payload in payloads:
        digest.update(payload)
        digest.update(b"\n")

    assert result_hash == digest.hexdigest()
    assert row_count == len(expected_rows)
    assert payload_bytes == sum(len(payload) for payload in payloads)


@requires_seaweedfs
def test_csv_and_parquet_arms_match_hash_with_different_file_sizes() -> None:
    """같은 고정 Row를 두 형식으로 읽으면 Result Hash는 같고 Byte 수는 다르다.

    Fixture는 이제 Scale 이름으로 재사용되므로, 실제 S/M/L 측정과 겹치지 않게
    Test 전용 Scale 이름을 쓴다.
    """
    scale = ScaleProfile(name="TEST-FMT-A", order_count=200, random_seed=20260921)
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


@requires_seaweedfs
def test_run_experiment_survives_five_repeats_with_same_benchmark_id() -> None:
    """`run_experiment`의 반복 5회는 매번 같은 Object Key로 Fixture를 다시 올리지 않는다.

    Object Key가 Scale에 묶여 있지 않고 benchmark_id에 묶여 있으면, 이미 있는
    Key에 다시 올리는 순간 `upload_new_file`의 불변 Key 검사가 두 번째 회차부터
    FileExistsError를 낸다. 실제 S/M/L 측정과 겹치지 않게 Test 전용 Scale 이름을 쓴다.
    """
    scale = ScaleProfile(name="TEST-FMT-B", order_count=50, random_seed=20260921)
    benchmark_id = new_benchmark_id("file_format", scale.name, datetime.now(UTC))
    config = RunConfig(
        scenario=FILE_FORMAT_SCENARIO,
        scale=scale,
        benchmark_id=benchmark_id,
        repeats=5,
        is_cold_run=False,
    )

    runs = run_experiment(FILE_FORMAT_SCENARIO, config)

    assert len(runs) == 10
    assert not has_invalid_runs(runs)
