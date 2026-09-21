"""Cold와 Warm 모집단이 서로 다른 Benchmark ID로 따로 기록되는지 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.benchmark.config import RunConfig, ScaleProfile, new_benchmark_id
from src.benchmark.experiments.cache_effect import (
    CACHE_EFFECT_SCENARIO,
    run_cold_warm_populations,
)
from src.benchmark.store import load_runs, median_duration


def test_cold_and_warm_populations_are_recorded_separately() -> None:
    """Cold 5회·Warm 5회가 서로 다른 Benchmark ID Median으로 따로 집계된다."""
    scale = ScaleProfile(name="S", order_count=50, random_seed=20260921)
    now = datetime.now(UTC)
    cold_id = new_benchmark_id("cache_effect", scale.name, now)
    warm_id = new_benchmark_id("cache_effect", scale.name, now + timedelta(seconds=1))

    cold_config = RunConfig(
        scenario=CACHE_EFFECT_SCENARIO,
        scale=scale,
        benchmark_id=cold_id,
        repeats=5,
        is_cold_run=True,
    )
    warm_config = RunConfig(
        scenario=CACHE_EFFECT_SCENARIO,
        scale=scale,
        benchmark_id=warm_id,
        repeats=5,
        is_cold_run=False,
    )

    cold_runs, warm_runs = run_cold_warm_populations(cold_config, warm_config)

    assert len(cold_runs) == 5
    assert len(warm_runs) == 5
    assert all(run.cache_reset_method is not None for run in cold_runs)
    assert all(run.cache_reset_method is None for run in warm_runs)

    cold_median = median_duration(load_runs(cold_id), is_cold_run=True)
    warm_median = median_duration(load_runs(warm_id), is_cold_run=False)
    assert cold_median is not None
    assert warm_median is not None
