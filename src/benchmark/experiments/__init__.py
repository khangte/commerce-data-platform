"""Scenario 이름을 정의와 실행 함수에 연결하는 Registry.

개별 실험 모듈(`extract`, `file_format`, `scan`, `cache_effect`)이 이 모듈을 import해
자신의 `BenchmarkScenario`와 실행 함수를 `SCENARIOS`/`EXPERIMENTS`에 등록한다.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING

from src.benchmark.config import BenchmarkScenario, RunConfig

if TYPE_CHECKING:
    from src.benchmark.runner import ArmResult

SCENARIOS: dict[str, BenchmarkScenario] = {}
EXPERIMENTS: dict[str, Callable[[RunConfig], Mapping[str, ArmResult]]] = {}

from src.benchmark.experiments import cache_effect as _cache_effect  # noqa: F401
from src.benchmark.experiments import extract as _extract  # noqa: F401
from src.benchmark.experiments import file_format as _file_format  # noqa: F401
from src.benchmark.experiments import scan as _scan  # noqa: F401
