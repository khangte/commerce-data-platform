"""Benchmark Harness CLI. `uv run python -m src.benchmark run|report`로 실행한다."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import UTC, datetime

from src.benchmark.config import BenchmarkScenario, RunConfig, new_benchmark_id, resolve_scale
from src.benchmark.experiments import PREPARE_HOOKS, SCENARIOS
from src.benchmark.runner import has_invalid_runs, run_experiment
from src.benchmark.store import BenchmarkRun, load_runs, median_duration, render_comparison


def main(argv: list[str] | None = None) -> int:
    """CLI Entrypoint. `run`은 실행 후 무효 회차가 있으면 0이 아닌 값을 반환한다."""
    parser = argparse.ArgumentParser(
        prog="python -m src.benchmark", description="Phase 9 Benchmark Harness"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Benchmark Scenario를 실행한다")
    run_parser.add_argument("--scenario", required=True, help="등록된 Scenario 이름")
    run_parser.add_argument("--scale", required=True, choices=("S", "M", "L"))
    cache_group = run_parser.add_mutually_exclusive_group()
    cache_group.add_argument("--cold", dest="cold", action="store_true", default=None)
    cache_group.add_argument("--warm", dest="cold", action="store_false")
    run_parser.add_argument("--repeats", type=int, default=None)
    run_parser.add_argument("--fixture-old-rows", dest="fixture_old_rows", type=int, default=None)
    run_parser.add_argument(
        "--fixture-current-rows", dest="fixture_current_rows", type=int, default=None
    )

    report_parser = subparsers.add_parser("report", help="Benchmark 비교표를 출력한다")
    report_parser.add_argument("--benchmark-id", dest="benchmark_id", required=True)

    args = parser.parse_args(argv)
    if args.command == "run":
        return _run_command(args)
    return _report_command(args)


def _run_command(args: argparse.Namespace) -> int:
    """`run` Subcommand — Scenario를 지정한 Scale·Cache 상태·반복 횟수로 실행한다."""
    scenario = SCENARIOS[args.scenario]
    scale = resolve_scale(args.scale)
    is_cold_run = scenario.cold if args.cold is None else args.cold
    repeats = scenario.repeats if args.repeats is None else args.repeats
    benchmark_id = new_benchmark_id(scenario.scenario, scale.name, datetime.now(UTC))
    parameters: dict[str, object] = {}
    if args.fixture_old_rows is not None:
        parameters["fixture_old_rows"] = args.fixture_old_rows
    if args.fixture_current_rows is not None:
        parameters["fixture_current_rows"] = args.fixture_current_rows
    config = RunConfig(
        scenario=scenario,
        scale=scale,
        benchmark_id=benchmark_id,
        repeats=repeats,
        is_cold_run=is_cold_run,
        parameters=parameters,
    )
    prepare = PREPARE_HOOKS.get(scenario.scenario)
    if prepare is not None:
        config = prepare(config)
    runs = run_experiment(scenario, config)
    print(f"benchmark_id={benchmark_id} runs={len(runs)}")
    return 1 if has_invalid_runs(runs) else 0


def _report_command(args: argparse.Namespace) -> int:
    """`report` Subcommand — 하나의 Benchmark ID를 Cold/Warm 별 비교표로 출력한다."""
    scenario_name, _scale_name, _timestamp = args.benchmark_id.rsplit("-", 2)
    scenario = SCENARIOS[scenario_name]
    runs = load_runs(args.benchmark_id)
    if len(scenario.arms) == 1:
        return _report_single_arm(scenario, runs)
    baseline_arm, improved_arm = scenario.arms[0], scenario.arms[1]
    for is_cold_run, label in ((True, "cold"), (False, "warm")):
        baseline = [
            run
            for run in runs
            if run.scenario == f"{scenario.scenario}-{baseline_arm}"
            and run.is_cold_run == is_cold_run
        ]
        improved = [
            run
            for run in runs
            if run.scenario == f"{scenario.scenario}-{improved_arm}"
            and run.is_cold_run == is_cold_run
        ]
        if not baseline and not improved:
            continue
        print(f"== {label} ==")
        print(render_comparison(baseline, improved))
    return 0


def _report_single_arm(scenario: BenchmarkScenario, runs: Sequence[BenchmarkRun]) -> int:
    """Arm이 하나뿐인 Scenario(실험 D)의 Cold·Warm 모집단을 각각 따로 출력한다."""
    arm = scenario.arms[0]
    for is_cold_run, label in ((True, "cold"), (False, "warm")):
        population = [
            run
            for run in runs
            if run.scenario == f"{scenario.scenario}-{arm}" and run.is_cold_run == is_cold_run
        ]
        if not population:
            continue
        valid_runs = [run for run in population if run.status == "VALID"]
        raw_values = [run.duration_seconds for run in valid_runs]
        median = median_duration(population, is_cold_run=is_cold_run)
        print(f"== {label} ==")
        print(
            f"{arm}: raw={raw_values} median={median} valid_runs={len(valid_runs)}/{len(population)}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
