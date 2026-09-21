"""Full Scan과 Filtered Scan이 같은 결과에 도달하는지, Scan Row 수는 다른지 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime

from src.benchmark.config import RunConfig, ScaleProfile, new_benchmark_id
from src.benchmark.experiments.scan import SCAN_SCENARIO, run_scan_experiment


def test_full_and_filtered_arms_match_hash_with_fewer_scanned_rows() -> None:
    """Filtered Arm은 Full Arm과 같은 집계를 내면서 더 적은 Row를 Scan한다."""
    scale = ScaleProfile(name="S", order_count=100, random_seed=20260921)
    benchmark_id = new_benchmark_id("scan", scale.name, datetime.now(UTC))
    config = RunConfig(
        scenario=SCAN_SCENARIO,
        scale=scale,
        benchmark_id=benchmark_id,
        repeats=5,
        is_cold_run=False,
    )

    arms = run_scan_experiment(config)

    assert arms["full_scan"].result_hash == arms["filtered_scan"].result_hash
    assert arms["filtered_scan"].counts.rows_scanned < arms["full_scan"].counts.rows_scanned
