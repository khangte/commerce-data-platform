"""Runner의 반복 실행·Cold/Warm 분리·Arm Hash Gate 강제를 검증한다."""

from __future__ import annotations

from pathlib import Path

from src.benchmark.config import SCALE_PROFILES, BenchmarkScenario, RunConfig
from src.benchmark.experiments import EXPERIMENTS
from src.benchmark.measure import Measurement, RowCounts
from src.benchmark.metadata import EnvironmentMetadata
from src.benchmark.runner import ArmResult, has_invalid_runs, run_experiment

_MEASUREMENT = Measurement(
    duration_seconds=0.1,
    cpu_user_seconds=0.05,
    cpu_system_seconds=0.01,
    max_rss_bytes=1024,
    read_bytes=0,
    write_bytes=0,
)
_FAKE_ENVIRONMENT = EnvironmentMetadata(
    git_commit="deadbeef",
    python_version="3.12.3",
    dependency_lock_hash="lockhash",
    docker_image_versions={},
    host_wsl_spec={},
)


def _scenario(name: str) -> BenchmarkScenario:
    """테스트용 두 Arm짜리 Scenario를 만든다."""
    return BenchmarkScenario(
        scenario=name, experiment="C", arms=("arm_a", "arm_b"), cold=False,
        description="fake scenario for runner tests",
    )


def _config(scenario: BenchmarkScenario, benchmark_id: str) -> RunConfig:
    """테스트용 Warm RunConfig를 만든다."""
    return RunConfig(
        scenario=scenario, scale=SCALE_PROFILES["S"], benchmark_id=benchmark_id,
        repeats=5, is_cold_run=False,
    )


def _patch_common(monkeypatch, tmp_path: Path) -> None:
    """실제 Filesystem·환경 수집을 건드리지 않도록 공통 대체물을 심는다."""
    monkeypatch.setattr("src.benchmark.store.BENCHMARK_DATA_ROOT", tmp_path)
    monkeypatch.setattr("src.benchmark.runner.collect_environment", lambda: _FAKE_ENVIRONMENT)


def test_matching_arms_produce_five_valid_runs_per_arm(monkeypatch, tmp_path: Path) -> None:
    """모든 Arm의 Result Hash가 같으면 5회 반복 모두 VALID로 기록된다."""
    _patch_common(monkeypatch, tmp_path)
    scenario = _scenario("fake-matching")

    def fake_experiment(config: RunConfig) -> dict[str, ArmResult]:
        return {
            "arm_a": ArmResult("arm_a", _MEASUREMENT, RowCounts(), "same-hash"),
            "arm_b": ArmResult("arm_b", _MEASUREMENT, RowCounts(), "same-hash"),
        }

    monkeypatch.setitem(EXPERIMENTS, scenario.scenario, fake_experiment)
    config = _config(scenario, "fake-matching-S-20260921T000000Z")

    runs = run_experiment(scenario, config)

    assert len(runs) == 10
    assert not has_invalid_runs(runs)
    assert sum(1 for run in runs if run.scenario == "fake-matching-arm_a") == 5
    assert sum(1 for run in runs if run.scenario == "fake-matching-arm_b") == 5
    assert all(run.status == "VALID" for run in runs)


def test_mismatching_arms_mark_every_run_invalid_but_keep_raw_values(
    monkeypatch, tmp_path: Path
) -> None:
    """Arm 간 Result Hash가 다르면 해당 회차 전부 INVALID이지만 측정값은 남는다."""
    _patch_common(monkeypatch, tmp_path)
    scenario = _scenario("fake-mismatching")

    def fake_experiment(config: RunConfig) -> dict[str, ArmResult]:
        return {
            "arm_a": ArmResult("arm_a", _MEASUREMENT, RowCounts(), "hash-a"),
            "arm_b": ArmResult("arm_b", _MEASUREMENT, RowCounts(), "hash-b"),
        }

    monkeypatch.setitem(EXPERIMENTS, scenario.scenario, fake_experiment)
    config = _config(scenario, "fake-mismatching-S-20260921T000000Z")

    runs = run_experiment(scenario, config)

    assert has_invalid_runs(runs)
    assert all(run.status == "INVALID" for run in runs)
    assert all(run.duration_seconds == 0.1 for run in runs)
