"""Benchmark Raw 결과를 JSONL로 적재하고 Median 집계·비교표를 만든다."""

from __future__ import annotations

import json
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BENCHMARK_DATA_ROOT = PROJECT_ROOT / "data" / "benchmarks"
MIN_VALID_RUNS_FOR_MEDIAN = 5


@dataclass(frozen=True)
class BenchmarkRun:
    """PRD §20 Run Metadata 전체와 Phase 9 전용 4개 필드를 담는 실행 한 회차 기록이다."""

    benchmark_id: str
    git_commit: str | None
    started_at: str
    host_wsl_spec: Mapping[str, str | None]
    python_version: str
    dependency_lock_hash: str | None
    image_versions: Mapping[str, str]
    dataset_scale: str
    random_seed: int
    scenario: str
    run_number: int
    is_cold_run: bool
    duration_seconds: float
    rows_scanned: int | None
    rows_changed: int | None
    input_bytes: int | None
    output_bytes: int | None
    result_hash: str
    cache_reset_method: Literal["drop_caches", "fadvise_dontneed", "process_restart_only"] | None
    change_rate: float | None
    cursor_range: str | None
    scenario_config_hash: str
    status: Literal["VALID", "INVALID"]


def _runs_path(benchmark_id: str) -> Path:
    """`benchmark_id`에 대응하는 Raw JSONL 경로를 만든다."""
    return BENCHMARK_DATA_ROOT / benchmark_id / "runs.jsonl"


def append_run(run: BenchmarkRun) -> Path:
    """실행 한 회차를 JSON 한 줄로 `data/benchmarks/{benchmark_id}/runs.jsonl`에 덧붙인다."""
    path = _runs_path(run.benchmark_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(asdict(run), ensure_ascii=False, sort_keys=True))
        handle.write("\n")
    return path


def load_runs(benchmark_id: str) -> tuple[BenchmarkRun, ...]:
    """`benchmark_id`에 적재된 모든 Raw 실행 기록을 읽는다."""
    path = _runs_path(benchmark_id)
    if not path.exists():
        return ()
    with path.open(encoding="utf-8") as handle:
        return tuple(BenchmarkRun(**json.loads(line)) for line in handle if line.strip())


def median_duration(runs: Sequence[BenchmarkRun], *, is_cold_run: bool) -> float | None:
    """유효(`VALID`)하고 요청한 Cache 상태에 속한 Run의 Duration Median을 구한다.

    5회 미만이면 대표값으로 신뢰할 수 없으므로 숫자 대신 None을 반환한다.
    """
    durations = [
        run.duration_seconds
        for run in runs
        if run.status == "VALID" and run.is_cold_run == is_cold_run
    ]
    if len(durations) < MIN_VALID_RUNS_FOR_MEDIAN:
        return None
    return statistics.median(durations)


def render_comparison(baseline: Sequence[BenchmarkRun], improved: Sequence[BenchmarkRun]) -> str:
    """두 Run 집합의 Raw 값·Median·Result Hash·증감률을 Phase 9 결과 문서 형식으로 만든다."""
    baseline_block = _render_arm("baseline", baseline)
    improved_block = _render_arm("improved", improved)
    baseline_median = median_duration(baseline, is_cold_run=_single_cache_state(baseline))
    improved_median = median_duration(improved, is_cold_run=_single_cache_state(improved))
    lines = [baseline_block, improved_block]
    if baseline_median is not None and improved_median is not None and baseline_median > 0:
        percent_change = (improved_median - baseline_median) / baseline_median * 100
        lines.append(f"change: {percent_change:+.1f}% (baseline median -> improved median)")
    else:
        lines.append("change: not computable (fewer than 5 valid runs in one arm)")
    return "\n".join(lines)


def _single_cache_state(runs: Sequence[BenchmarkRun]) -> bool:
    """비교 대상 Run 집합이 Cold/Warm 중 하나로만 이뤄졌다고 가정하고 그 상태를 반환한다."""
    if not runs:
        return False
    return runs[0].is_cold_run


def _render_arm(label: str, runs: Sequence[BenchmarkRun]) -> str:
    """한 Arm의 Raw 5회 값·Median·Result Hash를 사람이 읽을 한 블록으로 만든다."""
    valid_runs = [run for run in runs if run.status == "VALID"]
    raw_values = [run.duration_seconds for run in valid_runs]
    median = statistics.median(raw_values) if len(raw_values) >= 1 else None
    result_hashes = {run.result_hash for run in valid_runs}
    result_hash = result_hashes.pop() if len(result_hashes) == 1 else "MIXED"
    return (
        f"{label}: raw={raw_values} median={median} "
        f"result_hash={result_hash} valid_runs={len(valid_runs)}/{len(runs)}"
    )
