"""실험 D — 같은 Filtered Scan Query를 Cold Cache와 Warm Cache에서 각각 측정한다.

실험 C의 Filtered Scan Workload 하나만 재사용하는 단일 Arm 실험이다. Cold 5회·
Warm 5회를 서로 다른 Benchmark ID로 따로 기록해 두 Cache 상태의 결과가 섞이지
않게 한다. 이 실험의 결과는 그 자체로만 해석하고 A/B/C 개선 여부 판단에
섞어 쓰지 않는다.
"""

from __future__ import annotations

from collections.abc import Mapping

from src.benchmark.cache import warm_up
from src.benchmark.config import BenchmarkScenario, RunConfig
from src.benchmark.experiments import EXPERIMENTS, SCENARIOS
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


def run_cold_warm_populations(
    cold_config: RunConfig, warm_config: RunConfig
) -> tuple[tuple[BenchmarkRun, ...], tuple[BenchmarkRun, ...]]:
    """Cold 모집단과 Warm 모집단을 서로 다른 Benchmark ID로 따로 실행하고 기록한다.

    Fixture를 `run_experiment` 호출 전에 미리 만들어 둔다. `run_experiment`는
    매 회차 전에 `reset_caches()`를 부르므로, Fixture가 그 앞에서 이미
    디스크에 있어야 Cold 회차가 진짜 Cold Read가 된다. Warm은 측정에 넣지
    않는 예열 1회를 먼저 버린 뒤 5회를 측정한다.
    """
    old_rows = int(cold_config.parameters["fixture_old_rows"])
    current_rows = int(cold_config.parameters["fixture_current_rows"])
    ensure_scan_fixture(cold_config.scale.name, old_rows, current_rows)

    cold_runs = run_experiment(CACHE_EFFECT_SCENARIO, cold_config)
    warm_up(lambda: run_cache_experiment(warm_config))
    warm_runs = run_experiment(CACHE_EFFECT_SCENARIO, warm_config)
    return cold_runs, warm_runs
