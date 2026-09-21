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
    """Cold 5회·Warm 5회가 서로 다른 Benchmark ID Median으로 따로 집계된다.

    Fixture는 Scale 이름으로 재사용되므로, 실제 S/M/L 측정과 겹치지 않게
    Test 전용 Scale 이름("TEST-SCAN")을 쓴다.
    """
    scale = ScaleProfile(name="TEST-SCAN", order_count=50, random_seed=20260921)
    now = datetime.now(UTC)
    cold_id = new_benchmark_id("cache_effect", scale.name, now)
    warm_id = new_benchmark_id("cache_effect", scale.name, now + timedelta(seconds=1))

    # test_experiment_scan.py와 같은 Scale "TEST-SCAN" Fixture를 공유하니 두 값을 맞춰 둔다.
    fixture_params = {"fixture_old_rows": 2000, "fixture_current_rows": 100}
    cold_config = RunConfig(
        scenario=CACHE_EFFECT_SCENARIO,
        scale=scale,
        benchmark_id=cold_id,
        repeats=5,
        is_cold_run=True,
        parameters=fixture_params,
    )
    warm_config = RunConfig(
        scenario=CACHE_EFFECT_SCENARIO,
        scale=scale,
        benchmark_id=warm_id,
        repeats=5,
        is_cold_run=False,
        parameters=fixture_params,
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
    # cache_reset_method가 process_restart_only면 Page Cache가 실제로 비워지지
    # 않으므로 Cold/Warm 차이가 작거나 없을 수 있다 — 실패 조건이 아니라 관측 사실이다.
    reset_method = cold_runs[0].cache_reset_method
    print(f"cache_reset_method={reset_method} cold_median={cold_median} warm_median={warm_median}")
