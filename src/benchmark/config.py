"""Benchmark Scale·Scenario·실행 설정을 정의하고 재현 가능한 Config Hash를 계산한다."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

FIXED_RANDOM_SEED = 20260921
MIN_REPEATS = 5

_NON_DEFINING_PARAMETERS = frozenset(
    {"cache_reset_paths", "t0", "t_boundary", "t1", "t0_incremental_results", "change_stats"}
)


@dataclass(frozen=True)
class ScaleProfile:
    """Benchmark Dataset 규모 하나(S/M/L)를 정의한다."""

    name: Literal["S", "M", "L"]
    order_count: int
    random_seed: int


SCALE_PROFILES: dict[str, ScaleProfile] = {
    "S": ScaleProfile("S", 100_000, FIXED_RANDOM_SEED),
    "M": ScaleProfile("M", 1_000_000, FIXED_RANDOM_SEED),
    "L": ScaleProfile("L", 5_000_000, FIXED_RANDOM_SEED),
}


def resolve_scale(name: str) -> ScaleProfile:
    """Scale 이름을 등록된 ScaleProfile로 바꾼다. 미등록 이름은 거부한다."""
    try:
        return SCALE_PROFILES[name]
    except KeyError as error:
        supported = ", ".join(sorted(SCALE_PROFILES))
        raise ValueError(f"Unknown scale: {name}. Supported: {supported}") from error


@dataclass(frozen=True)
class BenchmarkScenario:
    """실험 하나의 정의 — 어떤 Arm을 몇 번 반복하고 Cache를 어떻게 다루는지를 담는다."""

    scenario: str
    experiment: Literal["A", "B", "C", "D", "OVERHEAD"]
    arms: tuple[str, ...]
    cold: bool
    description: str
    repeats: int = MIN_REPEATS

    def __post_init__(self) -> None:
        """반복 횟수가 대표값(Median) 산출에 필요한 최소치를 만족하는지 확인한다."""
        if self.repeats < MIN_REPEATS:
            raise ValueError(f"repeats must be at least {MIN_REPEATS}, got {self.repeats}")


@dataclass(frozen=True)
class RunConfig:
    """실행 한 번(Benchmark ID 하나)에 필요한 해석된 설정을 담는다."""

    scenario: BenchmarkScenario
    scale: ScaleProfile
    benchmark_id: str
    repeats: int
    is_cold_run: bool
    parameters: Mapping[str, object] = field(default_factory=dict)
    run_number: int = 0  # 실행 배선 값. parameters Dict가 아니라 Hash 대상 밖이다.

    def __post_init__(self) -> None:
        """반복 횟수가 대표값(Median) 산출에 필요한 최소치를 만족하는지 확인한다."""
        if self.repeats < MIN_REPEATS:
            raise ValueError(f"repeats must be at least {MIN_REPEATS}, got {self.repeats}")


def new_benchmark_id(scenario: str, scale: str, now: datetime) -> str:
    """`{scenario}-{scale}-{UTC Compact Timestamp}` 형식의 Benchmark ID를 만든다."""
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")
    return f"{scenario}-{scale}-{timestamp}"


def scenario_config_hash(scenario: BenchmarkScenario, config: RunConfig) -> str:
    """Scenario 정의와 Scale·Parameter를 SHA-256으로 묶어 같은 조건 측정임을 증명한다.

    `benchmark_id`와 실행 시각은 대상에서 제외해, 같은 Scenario 정의로 실행한
    서로 다른 시점의 두 실행이 같은 Hash를 갖게 한다. `_NON_DEFINING_PARAMETERS`에
    속한 Key(예: `cache_reset_paths`)도 제외한다 — Cold Run 배선용 값이라 Cold/Warm
    Hash가 갈리고, 절대 경로라 장비마다 값이 달라지기 때문이다.
    """
    payload = {
        "scenario": scenario.scenario,
        "experiment": scenario.experiment,
        "arms": list(scenario.arms),
        "cold": scenario.cold,
        "description": scenario.description,
        "repeats": config.repeats,
        "scale_name": config.scale.name,
        "scale_order_count": config.scale.order_count,
        "scale_random_seed": config.scale.random_seed,
        "parameters": {
            key: value
            for key, value in config.parameters.items()
            if key not in _NON_DEFINING_PARAMETERS
        },
    }
    canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
