"""실험 D — 같은 Filtered Scan Query를 Cold Cache와 Warm Cache에서 각각 측정한다.

실험 C의 Filtered Scan Workload 하나만 재사용하는 단일 Arm 실험이다. Cold 5회·
Warm 5회를 서로 다른 Benchmark ID로 따로 기록해 두 Cache 상태의 결과가 섞이지
않게 한다. 이 실험의 결과는 그 자체로만 해석하고 A/B/C 개선 여부 판단에
섞어 쓰지 않는다.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping

from src.benchmark.cache import warm_up
from src.benchmark.config import BenchmarkScenario, RunConfig
from src.benchmark.experiments import EXPERIMENTS, PREPARE_HOOKS, SCENARIOS
from src.benchmark.experiments.scan import (
    _aggregate_sql,
    _connect,
    _run_and_hash,
    ensure_scan_fixture,
)
from src.benchmark.measure import RowCounts, measure
from src.benchmark.runner import ArmResult, run_experiment
from src.benchmark.store import BenchmarkRun

CACHE_EFFECT_SCENARIO = BenchmarkScenario(
    scenario="cache_effect",
    experiment="D",
    arms=("filtered_scan",),
    cold=False,
    description="같은 Filtered Scan Query를 Cold·Warm Cache에서 각각 측정한다",
)
SCENARIOS[CACHE_EFFECT_SCENARIO.scenario] = CACHE_EFFECT_SCENARIO


def run_cache_experiment(config: RunConfig) -> Mapping[str, ArmResult]:
    """실험 C의 Filtered Scan Query 하나를 현재 Cache 상태에서 측정한다.

    Fixture는 이미 만들어져 있다고 가정하고 경로만 확인한다(없으면 만든다).
    `measure()` 안에는 Query 실행만 남겨, Fixture 준비 비용이나 방금 쓴
    파일이 Page Cache에 올라가는 부작용이 측정 구간에 섞이지 않게 한다.
    """
    old_rows = int(config.parameters["fixture_old_rows"])
    current_rows = int(config.parameters["fixture_current_rows"])
    old_path, current_path = ensure_scan_fixture(config.scale.name, old_rows, current_rows)
    sql = _aggregate_sql(old_path, current_path)

    with measure() as collector:
        result_hash, output_bytes = _run_and_hash(_connect(), sql)
    measurement = collector.result()

    return {
        "filtered_scan": ArmResult(
            "filtered_scan",
            measurement,
            RowCounts(
                rows_scanned=None,
                rows_changed=None,
                input_bytes=None,
                output_bytes=output_bytes,
            ),
            result_hash,
        )
    }


EXPERIMENTS[CACHE_EFFECT_SCENARIO.scenario] = run_cache_experiment


def prepare_cache_effect_fixture(config: RunConfig) -> RunConfig:
    """`run_experiment` 반복 밖에서 Fixture 존재와 Cache 상태를 미리 맞춘다.

    Fixture가 없으면 여기서 만든다 — `run_experiment` 첫 Cold 회차 안에서 처음
    만들어지면, 방금 쓴 파일이라 이미 Page Cache에 올라간 채로 "Cold" 측정이
    시작되는 문제(028 Task 12)가 생긴다. Cold Config는 `reset_caches()`가
    File 단위 fadvise로 대체될 때 쓸 `cache_reset_paths`를 심어 둔다. Warm
    Config는 측정에 넣지 않는 예열 1회를 먼저 버려, 직전 Cold 실행이 fadvise로
    비워 둔 Page Cache를 Warm 회차 시작 전에 다시 채운다.
    """
    old_rows = int(config.parameters["fixture_old_rows"])
    current_rows = int(config.parameters["fixture_current_rows"])
    old_path, current_path = ensure_scan_fixture(config.scale.name, old_rows, current_rows)
    if config.is_cold_run:
        return dataclasses.replace(
            config,
            parameters={**config.parameters, "cache_reset_paths": (old_path, current_path)},
        )
    warm_up(lambda: run_cache_experiment(config))
    return config


PREPARE_HOOKS[CACHE_EFFECT_SCENARIO.scenario] = prepare_cache_effect_fixture


def run_cold_warm_populations(
    cold_config: RunConfig, warm_config: RunConfig
) -> tuple[tuple[BenchmarkRun, ...], tuple[BenchmarkRun, ...]]:
    """Cold 모집단과 Warm 모집단을 서로 다른 Benchmark ID로 따로 실행하고 기록한다."""
    cold_config = prepare_cache_effect_fixture(cold_config)
    cold_runs = run_experiment(CACHE_EFFECT_SCENARIO, cold_config)
    warm_config = prepare_cache_effect_fixture(warm_config)
    warm_runs = run_experiment(CACHE_EFFECT_SCENARIO, warm_config)
    return cold_runs, warm_runs
