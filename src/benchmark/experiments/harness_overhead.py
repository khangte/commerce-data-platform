"""Harness 자체의 측정 오버헤드 하한선을 잰다.

Query나 I/O 없이 `measure()`만 감싸는 무측정(No-op) Scenario다. 다른 실험의
Duration이 이 하한선보다 뚜렷이 커야(Task 13 기준 20배 이상) Cache 효과·
Pushdown 효과 같은 실제 신호와 Harness 자체의 잡음을 구분할 수 있다.
"""

from __future__ import annotations

from collections.abc import Mapping

from src.benchmark.config import BenchmarkScenario, RunConfig
from src.benchmark.experiments import EXPERIMENTS, SCENARIOS
from src.benchmark.measure import RowCounts, measure
from src.benchmark.runner import ArmResult

HARNESS_OVERHEAD_SCENARIO = BenchmarkScenario(
    scenario="harness_overhead",
    experiment="OVERHEAD",
    arms=("noop",),
    cold=False,
    description="measure() 구간 자체의 오버헤드 하한선을 잰다",
)
SCENARIOS[HARNESS_OVERHEAD_SCENARIO.scenario] = HARNESS_OVERHEAD_SCENARIO


def run_harness_overhead(config: RunConfig) -> Mapping[str, ArmResult]:
    """아무 작업도 하지 않는 `measure()` 구간 하나를 측정한다."""
    with measure() as collector:
        pass
    measurement = collector.result()
    return {
        "noop": ArmResult(
            "noop",
            measurement,
            RowCounts(),
            "n/a",
        )
    }


EXPERIMENTS[HARNESS_OVERHEAD_SCENARIO.scenario] = run_harness_overhead
