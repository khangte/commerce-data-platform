"""측정 구간의 Wall Time·CPU·Max RSS·I/O Byte를 stdlib만으로 수집한다."""

from __future__ import annotations

import resource
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Self

PROC_SELF_IO = Path("/proc/self/io")
KILOBYTE = 1024


@dataclass(frozen=True)
class Measurement:
    """측정 구간 하나가 끝난 뒤 확정되는 자원 사용량이다."""

    duration_seconds: float
    cpu_user_seconds: float
    cpu_system_seconds: float
    max_rss_bytes: int
    read_bytes: int | None
    write_bytes: int | None


@dataclass(frozen=True)
class RowCounts:
    """Scenario Arm이 처리한 행·Byte 수. 계산할 수 없는 항목은 None으로 둔다."""

    rows_scanned: int | None = None
    rows_changed: int | None = None
    input_bytes: int | None = None
    output_bytes: int | None = None


class MeasurementCollector:
    """`measure()`가 반환하는 Context Manager이자 결과 보관소다. 재진입은 지원하지 않는다."""

    def __init__(self, *, include_children: bool) -> None:
        self._include_children = include_children
        self._entered = False
        self._result: Measurement | None = None

    def __enter__(self) -> Self:
        if self._entered:
            raise RuntimeError("measure() context manager is not reentrant")
        self._entered = True
        self._start_time = time.monotonic()
        self._start_usage = _combined_usage(self._include_children)
        self._start_io = _read_proc_self_io()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        end_time = time.monotonic()
        end_usage = _combined_usage(self._include_children)
        end_io = _read_proc_self_io()
        read_bytes: int | None = None
        write_bytes: int | None = None
        if self._start_io is not None and end_io is not None:
            read_bytes = end_io[0] - self._start_io[0]
            write_bytes = end_io[1] - self._start_io[1]
        self._result = Measurement(
            duration_seconds=end_time - self._start_time,
            cpu_user_seconds=end_usage[0] - self._start_usage[0],
            cpu_system_seconds=end_usage[1] - self._start_usage[1],
            max_rss_bytes=end_usage[2] * KILOBYTE,
            read_bytes=read_bytes,
            write_bytes=write_bytes,
        )

    def result(self) -> Measurement:
        """측정이 끝난 뒤 확정된 Measurement를 반환한다."""
        if self._result is None:
            raise RuntimeError("measure() context has not finished yet")
        return self._result


def measure(*, include_children: bool = False) -> MeasurementCollector:
    """측정 구간을 여는 Context Manager를 만든다. 자식 Process 자원은 옵션이다."""
    return MeasurementCollector(include_children=include_children)


def _combined_usage(include_children: bool) -> tuple[float, float, int]:
    """`(user, system, max_rss_kb)`를 RUSAGE_SELF 기준, 옵션에 따라 자식 포함으로 계산한다."""
    self_usage = resource.getrusage(resource.RUSAGE_SELF)
    if not include_children:
        return self_usage.ru_utime, self_usage.ru_stime, self_usage.ru_maxrss
    children_usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return (
        self_usage.ru_utime + children_usage.ru_utime,
        self_usage.ru_stime + children_usage.ru_stime,
        max(self_usage.ru_maxrss, children_usage.ru_maxrss),
    )


def _read_proc_self_io() -> tuple[int, int] | None:
    """`/proc/self/io`에서 `(read_bytes, write_bytes)`를 읽는다. 실패하면 None."""
    try:
        text = PROC_SELF_IO.read_text(encoding="utf-8")
    except OSError:
        return None
    values: dict[str, int] = {}
    for line in text.splitlines():
        key, _, raw_value = line.partition(":")
        key = key.strip()
        if key in ("read_bytes", "write_bytes"):
            values[key] = int(raw_value.strip())
    if "read_bytes" not in values or "write_bytes" not in values:
        return None
    return values["read_bytes"], values["write_bytes"]
