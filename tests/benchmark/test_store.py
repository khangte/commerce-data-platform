"""Benchmark Raw Store의 적재·Median 집계·비교표 렌더링을 검증한다."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.benchmark.store import (
    BenchmarkRun,
    append_run,
    load_runs,
    median_duration,
    render_comparison,
)


def _run(
    *,
    benchmark_id: str = "scan-S-20260921T000000Z",
    scenario: str = "scan-full_scan",
    run_number: int = 1,
    duration_seconds: float = 1.0,
    is_cold_run: bool = False,
    status: str = "VALID",
    result_hash: str = "hash-a",
) -> BenchmarkRun:
    """테스트용 최소 BenchmarkRun을 만든다."""
    return BenchmarkRun(
        benchmark_id=benchmark_id,
        git_commit="deadbeef",
        started_at="2026-09-21T00:00:00+00:00",
        host_wsl_spec={"cpu_model": "test-cpu"},
        python_version="3.12.3",
        dependency_lock_hash="lockhash",
        image_versions={"postgres": "postgres:18.6"},
        dataset_scale="S",
        random_seed=20260921,
        scenario=scenario,
        run_number=run_number,
        is_cold_run=is_cold_run,
        duration_seconds=duration_seconds,
        rows_scanned=100,
        rows_changed=None,
        input_bytes=1024,
        output_bytes=512,
        result_hash=result_hash,
        cache_reset_method=None,
        change_rate=None,
        cursor_range=None,
        scenario_config_hash="confighash",
        status=status,
    )


def test_append_and_load_round_trips_runs(tmp_path: Path, monkeypatch) -> None:
    """적재한 Run을 그대로 다시 읽을 수 있다."""
    monkeypatch.setattr("src.benchmark.store.BENCHMARK_DATA_ROOT", tmp_path)
    run = _run()

    path = append_run(run)
    loaded = load_runs(run.benchmark_id)

    assert path.exists()
    assert loaded == (run,)


def test_load_runs_returns_empty_tuple_when_nothing_recorded(tmp_path: Path, monkeypatch) -> None:
    """기록이 없는 Benchmark ID는 빈 Tuple을 돌려준다."""
    monkeypatch.setattr("src.benchmark.store.BENCHMARK_DATA_ROOT", tmp_path)
    assert load_runs("no-such-benchmark") == ()


def test_median_duration_returns_none_below_five_valid_runs() -> None:
    """유효 Run이 5회 미만이면 대표값 대신 None을 돌려준다."""
    runs = [_run(run_number=index, duration_seconds=float(index)) for index in range(1, 4)]
    assert median_duration(runs, is_cold_run=False) is None


def test_median_duration_computes_median_of_five_valid_warm_runs() -> None:
    """유효한 Warm Run 5개의 Duration Median을 계산한다."""
    runs = [
        _run(run_number=index, duration_seconds=value, is_cold_run=False)
        for index, value in enumerate([1.0, 3.0, 2.0, 5.0, 4.0], start=1)
    ]
    assert median_duration(runs, is_cold_run=False) == 3.0


def test_median_duration_never_mixes_cold_and_warm() -> None:
    """Cold와 Warm Run이 섞여 있으면 요청한 상태만 집계 대상이 된다."""
    cold_runs = [
        _run(run_number=index, duration_seconds=10.0, is_cold_run=True) for index in range(1, 6)
    ]
    warm_runs = [
        _run(run_number=index, duration_seconds=1.0, is_cold_run=False) for index in range(1, 6)
    ]
    runs = cold_runs + warm_runs

    assert median_duration(runs, is_cold_run=True) == 10.0
    assert median_duration(runs, is_cold_run=False) == 1.0


def test_median_duration_excludes_invalid_runs() -> None:
    """Hash 불일치로 INVALID 처리된 Run은 대표값 계산에서 빠진다."""
    valid_runs = [
        _run(run_number=index, duration_seconds=1.0, status="VALID") for index in range(1, 6)
    ]
    invalid_run = _run(run_number=6, duration_seconds=999.0, status="INVALID")

    assert median_duration([*valid_runs, invalid_run], is_cold_run=False) == 1.0


def test_median_duration_rejects_mixed_scenarios() -> None:
    """서로 다른 scenario(Arm) Run이 섞이면 모집단 혼합 대신 ValueError로 거부한다."""
    full_runs = [
        _run(run_number=index, duration_seconds=280.0, scenario="extract-full")
        for index in range(1, 6)
    ]
    incremental_runs = [
        _run(run_number=index, duration_seconds=6.0, scenario="extract-incremental")
        for index in range(1, 6)
    ]

    with pytest.raises(ValueError, match="scenario"):
        median_duration(full_runs + incremental_runs, is_cold_run=False)


def test_render_comparison_reports_raw_median_hash_and_percent_change() -> None:
    """비교표에 Raw 값·Median·Result Hash·증감률이 모두 나타난다."""
    baseline = [
        _run(run_number=index, duration_seconds=2.0, result_hash="same-hash")
        for index in range(1, 6)
    ]
    improved = [
        _run(run_number=index, duration_seconds=1.0, result_hash="same-hash")
        for index in range(1, 6)
    ]

    report = render_comparison(baseline, improved)

    assert "median=2.0" in report
    assert "median=1.0" in report
    assert "same-hash" in report
    assert "-50.0%" in report


def test_render_comparison_reports_not_computable_below_five_runs() -> None:
    """5회 미만이면 증감률을 계산하지 않고 그 사실을 명시한다."""
    baseline = [_run(run_number=1, duration_seconds=2.0)]
    improved = [_run(run_number=1, duration_seconds=1.0)]

    report = render_comparison(baseline, improved)

    assert "not computable" in report
