"""Scenario를 반복 실행하며 Cold/Warm을 분리하고 Arm Hash Gate를 강제한다."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from src.benchmark.cache import reset_caches
from src.benchmark.config import BenchmarkScenario, RunConfig, scenario_config_hash
from src.benchmark.measure import Measurement, RowCounts
from src.benchmark.metadata import collect_environment
from src.benchmark.result_hash import ResultHashMismatch, assert_arms_match
from src.benchmark.store import BenchmarkRun, append_run


@dataclass(frozen=True)
class ArmResult:
    """Scenario Arm 하나를 한 번 실행한 결과 — 측정값·행 수·Result Hash를 담는다."""

    arm: str
    measurement: Measurement
    counts: RowCounts
    result_hash: str


def run_experiment(scenario: BenchmarkScenario, config: RunConfig) -> tuple[BenchmarkRun, ...]:
    """Scenario를 `config.repeats`회 반복하고 매 회차 모든 Arm의 결과를 기록한다.

    Cold 실행은 매 회차 전에 Cache를 초기화한다. 회차마다 모든 Arm의 Result Hash가
    일치하는지 확인하고, 불일치하면 그 회차의 Run 전부를 `INVALID`로 남기되 Raw 값은
    그대로 보존한다. Scenario별 세부 로직은 `EXPERIMENTS` Registry 뒤에 둔다.
    """
    from src.benchmark.experiments import EXPERIMENTS  # 순환 Import 회피

    experiment_fn = EXPERIMENTS[scenario.scenario]
    config_hash = scenario_config_hash(scenario, config)
    runs: list[BenchmarkRun] = []
    for run_number in range(1, config.repeats + 1):
        cache_reset_method = reset_caches().method if config.is_cold_run else None
        arm_results = experiment_fn(config)
        hashes = {arm: result.result_hash for arm, result in arm_results.items()}
        try:
            assert_arms_match(hashes)
            status = "VALID"
        except ResultHashMismatch:
            status = "INVALID"
        environment = collect_environment()
        started_at = datetime.now(UTC).isoformat()
        for arm, result in arm_results.items():
            run = BenchmarkRun(
                benchmark_id=config.benchmark_id,
                git_commit=environment.git_commit,
                started_at=started_at,
                host_wsl_spec=environment.host_wsl_spec,
                python_version=environment.python_version,
                dependency_lock_hash=environment.dependency_lock_hash,
                image_versions=environment.docker_image_versions,
                dataset_scale=config.scale.name,
                random_seed=config.scale.random_seed,
                scenario=f"{scenario.scenario}-{arm}",
                run_number=run_number,
                is_cold_run=config.is_cold_run,
                duration_seconds=result.measurement.duration_seconds,
                rows_scanned=result.counts.rows_scanned,
                rows_changed=result.counts.rows_changed,
                input_bytes=result.counts.input_bytes,
                output_bytes=result.counts.output_bytes,
                result_hash=result.result_hash,
                cache_reset_method=cache_reset_method,
                change_rate=config.parameters.get("change_rate"),
                cursor_range=config.parameters.get("cursor_range"),
                scenario_config_hash=config_hash,
                status=status,
            )
            append_run(run)
            runs.append(run)
    return tuple(runs)


def has_invalid_runs(runs: tuple[BenchmarkRun, ...]) -> bool:
    """Hash 불일치로 무효 처리된 Run이 하나라도 있는지 확인한다."""
    return any(run.status == "INVALID" for run in runs)
