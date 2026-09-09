"""결정적 Generator 실행 입력의 검증과 정규화를 제공한다."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

GENERATOR_VERSION = "1.0.0"
SUPPORTED_GENERATOR_VERSIONS = frozenset({GENERATOR_VERSION})
SUPPORTED_ANOMALY_PROFILES = frozenset(
    {"default", "late-arrival", "delayed-payment", "membership-change"}
)
EXECUTABLE_ANOMALY_PROFILES = frozenset({"default", "late-arrival", "membership-change"})


@dataclass(frozen=True)
class GeneratorConfig:
    """하나의 Generator 실행을 재현하는 정규화된 입력 묶음이다."""

    source_snapshot_id: str
    random_seed: int
    logical_date: datetime
    order_count: int
    anomaly_profile: str
    generator_version: str

    def __post_init__(self) -> None:
        """입력값이 결정성 계약과 Metadata 저장 범위를 만족하는지 확인한다."""
        if not self.source_snapshot_id or len(self.source_snapshot_id) > 256:
            raise ValueError("source_snapshot_id must contain 1 to 256 characters")
        if isinstance(self.random_seed, bool) or not isinstance(self.random_seed, int):
            raise TypeError("random_seed must be an integer")
        if not -(2**63) <= self.random_seed < 2**63:
            raise ValueError("random_seed must fit in PostgreSQL BIGINT")
        if self.logical_date.tzinfo is None:
            raise ValueError("logical_date must include a UTC offset")
        if self.logical_date.utcoffset() is None:
            raise ValueError("logical_date must include a UTC offset")
        if isinstance(self.order_count, bool) or not isinstance(self.order_count, int):
            raise TypeError("order_count must be an integer")
        if self.order_count < 0:
            raise ValueError("order_count must be zero or greater")
        if self.anomaly_profile not in SUPPORTED_ANOMALY_PROFILES:
            supported = ", ".join(sorted(SUPPORTED_ANOMALY_PROFILES))
            raise ValueError(
                f"Unsupported anomaly_profile: {self.anomaly_profile}. Supported: {supported}"
            )
        assert_generator_version_supported(self.generator_version)

    @classmethod
    def from_values(
        cls,
        *,
        source_snapshot_id: str,
        random_seed: int,
        logical_date: str,
        order_count: int,
        anomaly_profile: str,
        generator_version: str,
    ) -> GeneratorConfig:
        """CLI 문자열을 UTC 논리 시각을 포함한 실행 Config로 변환한다."""
        return cls(
            source_snapshot_id=source_snapshot_id,
            random_seed=random_seed,
            logical_date=parse_logical_date(logical_date),
            order_count=order_count,
            anomaly_profile=anomaly_profile,
            generator_version=generator_version,
        )

    def deterministic_inputs(self) -> dict[str, int | str]:
        """결정성 검증과 실행 이력 비교에 사용할 표준 입력을 반환한다."""
        return {
            "source_snapshot_id": self.source_snapshot_id,
            "random_seed": self.random_seed,
            "logical_date": self.logical_date.isoformat(),
            "order_count": self.order_count,
            "anomaly_profile": self.anomaly_profile,
            "generator_version": self.generator_version,
        }


def parse_logical_date(value: str) -> datetime:
    """UTC Offset이 포함된 논리 시각 문자열을 UTC로 정규화한다."""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError("--logical-date must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(
            "--logical-date must include a UTC offset, for example 2026-09-04T00:00:00Z"
        )
    return parsed.astimezone(UTC)


def assert_generator_version_supported(generator_version: str) -> None:
    """현재 구현이 읽고 실행할 수 있는 Generator 버전인지 확인한다."""
    if generator_version not in SUPPORTED_GENERATOR_VERSIONS:
        supported = ", ".join(sorted(SUPPORTED_GENERATOR_VERSIONS))
        raise ValueError(
            f"Unsupported generator_version: {generator_version}. Supported: {supported}"
        )
