"""실험 D 귀속 판정용 대조군 2종(037 §4)을 실측한다.

대조군 A: os.sync()만 하고 fadvise는 하지 않는 Arm 5회 — Cold와 값이 같으면
원인은 sync Writeback이지 Page Cache 재적재가 아니다.
교차 실행: Cold·Warm을 번갈아 5쌍 — Arm/시간 순서 교락을 없앤다.

기존 cache_effect M Scale Fixture(old_rows=5,000,000 / current_rows=1,000,000)를
그대로 재사용한다. BenchmarkRun/store.py 파이프라인에 적재하지 않는 1회성 진단
스크립트다 — Scenario로 등록하지 않는다(037은 이 두 측정을 귀속을 가르는
진단으로 요청했지 영구 Scenario로 요청하지 않았다).

재현:
    PYTHONPATH=. uv run python scripts/d_control_experiments.py
"""

from __future__ import annotations

import json
import subprocess

from src.benchmark.cache import reset_caches, warm_up
from src.benchmark.experiments.scan import (
    _aggregate_sql,
    _connect,
    _run_and_hash,
    ensure_scan_fixture,
)
from src.benchmark.measure import measure

OLD_ROWS = 5_000_000
CURRENT_ROWS = 1_000_000


def run_query() -> float:
    """Filtered Scan Query 1회를 측정 구간만 재고 Duration(초)을 돌려준다."""
    old_path, current_path = ensure_scan_fixture("M", OLD_ROWS, CURRENT_ROWS)
    sql = _aggregate_sql(old_path, current_path)
    with measure() as collector:
        _run_and_hash(_connect(), sql)
    return collector.result().duration_seconds


def sync_only() -> None:
    """대조군 A: os.sync()만 부르고 fadvise는 하지 않는다."""
    subprocess.run(["sync"], check=True, capture_output=True, text=True, timeout=30)


def cold_reset() -> None:
    """정상 Cold 경로: fadvise 포함 전체 reset_caches()."""
    old_path, current_path = ensure_scan_fixture("M", OLD_ROWS, CURRENT_ROWS)
    reset_caches(paths=(old_path, current_path))


def main() -> None:
    """대조군 A와 교차 실행을 순서대로 돌리고 JSON으로 결과를 찍는다."""
    ensure_scan_fixture("M", OLD_ROWS, CURRENT_ROWS)

    # Fixture 존재 보장 후 Page Cache를 Warm 상태로 맞춘다(대조군 A 시작 전 예열).
    warm_up(run_query)

    control_a: list[float] = []
    for _ in range(5):
        sync_only()
        control_a.append(run_query())

    # 교차 실행: Cold(fadvise 포함 reset) -> Warm(reset 없음) 순서로 5쌍.
    interleaved_cold: list[float] = []
    interleaved_warm: list[float] = []
    for _ in range(5):
        cold_reset()
        interleaved_cold.append(run_query())
        interleaved_warm.append(run_query())

    print(
        json.dumps(
            {
                "control_a_sync_only": control_a,
                "interleaved_cold": interleaved_cold,
                "interleaved_warm": interleaved_warm,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
