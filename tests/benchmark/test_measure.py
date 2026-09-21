"""자원 측정 Context Manager의 동작을 검증한다."""

from __future__ import annotations

import pytest

from src.benchmark import measure as measure_module
from src.benchmark.measure import measure


def test_busy_loop_reports_nonzero_duration_and_cpu() -> None:
    """실제로 CPU를 쓰는 구간은 0보다 큰 Duration과 CPU 시간을 남긴다."""
    with measure() as collector:
        total = 0
        for value in range(2_000_000):
            total += value

    result = collector.result()
    assert result.duration_seconds > 0
    assert result.cpu_user_seconds >= 0
    assert result.max_rss_bytes > 0


def test_result_before_exit_raises() -> None:
    """측정이 끝나기 전에 결과를 조회하면 예외를 낸다."""
    collector = measure()
    with pytest.raises(RuntimeError, match="not finished"):
        collector.result()


def test_reentering_a_collector_raises() -> None:
    """같은 Collector를 중첩해서 다시 열면 예외를 낸다."""
    collector = measure()
    with collector, pytest.raises(RuntimeError, match="not reentrant"):
        collector.__enter__()


def test_missing_proc_self_io_yields_none_not_zero(monkeypatch) -> None:
    """`/proc/self/io`를 읽지 못하면 Byte 필드는 0이 아니라 None이 된다."""
    monkeypatch.setattr(measure_module, "_read_proc_self_io", lambda: None)

    with measure() as collector:
        pass

    result = collector.result()
    assert result.read_bytes is None
    assert result.write_bytes is None
