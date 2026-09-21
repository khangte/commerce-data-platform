"""Benchmark Scale·Scenario 설정과 Config Hash의 재현성을 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from src.benchmark.config import (
    SCALE_PROFILES,
    BenchmarkScenario,
    RunConfig,
    new_benchmark_id,
    resolve_scale,
    scenario_config_hash,
)


def test_scale_profiles_share_one_seed_and_expected_order_counts() -> None:
    """S/M/L 모두 같은 Seed를 쓰고 정해진 Order 수를 가진다."""
    seeds = {profile.random_seed for profile in SCALE_PROFILES.values()}
    assert seeds == {SCALE_PROFILES["S"].random_seed}
    assert SCALE_PROFILES["S"].order_count == 100_000
    assert SCALE_PROFILES["M"].order_count == 1_000_000
    assert SCALE_PROFILES["L"].order_count == 5_000_000


def test_resolve_scale_rejects_unknown_name() -> None:
    """등록되지 않은 Scale 이름은 거부한다."""
    with pytest.raises(ValueError, match="Unknown scale"):
        resolve_scale("XL")


def test_benchmark_scenario_rejects_fewer_than_five_repeats() -> None:
    """대표값은 Median이므로 5회 미만 반복은 거부한다."""
    with pytest.raises(ValueError, match="repeats"):
        BenchmarkScenario(
            scenario="scan", experiment="C", arms=("full_scan",), cold=False,
            description="", repeats=3,
        )


def test_new_benchmark_id_formats_scenario_scale_and_utc_timestamp() -> None:
    """Benchmark ID는 Scenario-Scale-UTC Compact Timestamp 형식이다."""
    now = datetime(2026, 9, 21, 1, 2, 3, tzinfo=UTC)
    assert new_benchmark_id("scan", "M", now) == "scan-M-20260921T010203Z"


def test_scenario_config_hash_ignores_benchmark_id_and_timestamp() -> None:
    """같은 Scenario 정의면 Benchmark ID·시각이 달라도 같은 Hash를 낸다."""
    scenario = BenchmarkScenario(
        scenario="scan", experiment="C", arms=("full_scan", "filtered_scan"), cold=False,
        description="Full vs filtered scan",
    )
    first = RunConfig(
        scenario=scenario, scale=SCALE_PROFILES["S"], benchmark_id="scan-S-1",
        repeats=5, is_cold_run=False,
    )
    second = RunConfig(
        scenario=scenario, scale=SCALE_PROFILES["S"], benchmark_id="scan-S-2",
        repeats=5, is_cold_run=False,
    )

    assert scenario_config_hash(scenario, first) == scenario_config_hash(scenario, second)


def test_scenario_config_hash_changes_when_scale_changes() -> None:
    """Scale이 다르면 측정 조건이 다르므로 Hash도 달라진다."""
    scenario = BenchmarkScenario(
        scenario="scan", experiment="C", arms=("full_scan", "filtered_scan"), cold=False,
        description="Full vs filtered scan",
    )
    small = RunConfig(
        scenario=scenario, scale=SCALE_PROFILES["S"], benchmark_id="scan-S-1",
        repeats=5, is_cold_run=False,
    )
    medium = RunConfig(
        scenario=scenario, scale=SCALE_PROFILES["M"], benchmark_id="scan-M-1",
        repeats=5, is_cold_run=False,
    )

    assert scenario_config_hash(scenario, small) != scenario_config_hash(scenario, medium)


def test_run_config_rejects_fewer_than_five_repeats() -> None:
    """RunConfig도 5회 미만 반복을 거부한다."""
    scenario = BenchmarkScenario(
        scenario="scan", experiment="C", arms=("full_scan",), cold=False, description="",
    )
    with pytest.raises(ValueError, match="repeats"):
        RunConfig(
            scenario=scenario, scale=SCALE_PROFILES["S"], benchmark_id="scan-S-1",
            repeats=1, is_cold_run=False,
        )
