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
from src.benchmark.experiments.scan import run_filtered_scan_workload
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
    """실험 C의 Filtered Scan Workload 하나를 현재 Cache 상태에서 측정한다."""
    with measure() as collector:
        result_hash, output_bytes = run_filtered_scan_workload(config.scale.order_count)
    measurement = collector.result()
    return {
        "filtered_scan": ArmResult(
            "filtered_scan",
            measurement,
            RowCounts(
                rows_scanned=config.scale.order_count,
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

    Cold는 `run_experiment`가 매 회차 전에 `reset_caches()`를 호출하는 기본
    동작을 그대로 쓴다. Warm은 측정에 넣지 않는 예열 1회를 먼저 버린 뒤
    5회를 측정한다.
    """
    cold_runs = run_experiment(CACHE_EFFECT_SCENARIO, cold_config)
    warm_up(lambda: run_cache_experiment(warm_config))
    warm_runs = run_experiment(CACHE_EFFECT_SCENARIO, warm_config)
    return cold_runs, warm_runs
