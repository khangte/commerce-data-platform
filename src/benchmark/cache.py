"""Cold/Warm 측정을 위한 Cache 초기화 절차와 Warm-up 실행을 제공한다."""

from __future__ import annotations

import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

DROP_CACHES_PATH = Path("/proc/sys/vm/drop_caches")


@dataclass(frozen=True)
class CacheResetResult:
    """실제로 어떤 방식으로 Cache를 초기화했는지와 그 근거를 담는다."""

    method: Literal["drop_caches", "process_restart_only"]
    detail: str


def reset_caches(*, services: Sequence[str] = ()) -> CacheResetResult:
    """OS Page Cache Drop을 시도하고, 권한이 없으면 Process/Compose 재시작으로 대체한다."""
    try:
        subprocess.run(["sync"], check=True, capture_output=True, text=True, timeout=30)
        DROP_CACHES_PATH.write_text("3")
    except (PermissionError, OSError, subprocess.SubprocessError) as error:
        return _fallback_reset(services=services, reason=str(error))
    return CacheResetResult(method="drop_caches", detail="wrote 3 to /proc/sys/vm/drop_caches")


def _fallback_reset(*, services: Sequence[str], reason: str) -> CacheResetResult:
    """`drop_caches`를 쓸 수 없을 때 Compose Service 재시작으로 대체한다."""
    detail = f"drop_caches unavailable ({reason}); OS page cache was not dropped"
    if not services:
        return CacheResetResult(method="process_restart_only", detail=detail)
    try:
        subprocess.run(
            ["docker", "compose", "restart", *services],
            check=True,
            capture_output=True,
            text=True,
            timeout=120,
        )
    except (OSError, subprocess.SubprocessError) as restart_error:
        return CacheResetResult(
            method="process_restart_only",
            detail=f"{detail}; docker compose restart also failed: {restart_error}",
        )
    return CacheResetResult(
        method="process_restart_only",
        detail=f"{detail}; restarted services: {', '.join(services)}",
    )


def warm_up(callable_: Callable[[], object]) -> None:
    """Warm Run 측정 전에 결과를 버리는 한 번의 예열 호출을 실행한다."""
    callable_()
