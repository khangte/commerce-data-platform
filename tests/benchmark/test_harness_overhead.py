"""No-op Scenario가 Harness 오버헤드 하한선으로 쓸 만큼 안정적인지 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime

from src.benchmark.config import RunConfig, ScaleProfile, new_benchmark_id
from src.benchmark.experiments.harness_overhead import (
    HARNESS_OVERHEAD_SCENARIO,
    run_harness_overhead,
)
from src.benchmark.runner import run_experiment
from src.benchmark.store import median_duration


def test_noop_scenario_produces_five_valid_runs_with_a_measurable_floor() -> None:
    """5회 반복 모두 VALID이고 Median Duration을 낼 수 있다."""
    scale = ScaleProfile(name="S", order_count=100_000, random_seed=20260921)
    benchmark_id = new_benchmark_id("harness_overhead", scale.name, datetime.now(UTC))
    config = RunConfig(
        scenario=HARNESS_OVERHEAD_SCENARIO,
        scale=scale,
        benchmark_id=benchmark_id,
        repeats=5,
        is_cold_run=False,
    )

    runs = run_experiment(HARNESS_OVERHEAD_SCENARIO, config)

    assert len(runs) == 5
    assert all(run.status == "VALID" for run in runs)
    floor = median_duration(runs, is_cold_run=False)
    assert floor is not None
    assert floor >= 0.0


def test_run_harness_overhead_arm_result_shape() -> None:
    """단일 Arm ``noop``의 결과 형태를 확인한다."""
    scale = ScaleProfile(name="S", order_count=100_000, random_seed=20260921)
    benchmark_id = new_benchmark_id("harness_overhead", scale.name, datetime.now(UTC))
    config = RunConfig(
        scenario=HARNESS_OVERHEAD_SCENARIO,
        scale=scale,
        benchmark_id=benchmark_id,
        repeats=5,
        is_cold_run=False,
    )

    arms = run_harness_overhead(config)

    assert set(arms) == {"noop"}
    assert arms["noop"].counts.rows_scanned is None
