"""Phase 2 결정성 입력 계약을 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from src.generator.config import GENERATOR_VERSION, GeneratorConfig, parse_logical_date


def test_generator_config_normalizes_logical_date_and_exposes_stable_inputs() -> None:
    """Offset이 있는 논리 시각을 UTC 입력 계약으로 정규화한다."""
    config = GeneratorConfig.from_values(
        source_snapshot_id="seed:abc123",
        random_seed=42,
        logical_date="2026-09-04T09:00:00+09:00",
        order_count=1000,
        anomaly_profile="default",
        generator_version=GENERATOR_VERSION,
    )

    assert config.logical_date == datetime(2026, 9, 4, tzinfo=UTC)
    assert config.deterministic_inputs() == {
        "source_snapshot_id": "seed:abc123",
        "random_seed": 42,
        "logical_date": "2026-09-04T00:00:00+00:00",
        "order_count": 1000,
        "anomaly_profile": "default",
        "generator_version": GENERATOR_VERSION,
    }


@pytest.mark.parametrize("value", ("2026-09-04T00:00:00", "not-a-timestamp"))
def test_parse_logical_date_rejects_invalid_or_naive_values(value: str) -> None:
    """UTC Offset 없는 논리 시각은 Source Mutation Time으로 사용할 수 없다."""
    with pytest.raises(ValueError):
        parse_logical_date(value)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("source_snapshot_id", "", "source_snapshot_id"),
        ("order_count", -1, "order_count"),
        ("anomaly_profile", "unknown", "anomaly_profile"),
        ("generator_version", "2.0.0", "generator_version"),
    ),
)
def test_generator_config_rejects_unsupported_inputs(field: str, value: object, message: str) -> None:
    """지원하지 않는 결정성 입력은 Source 변경 전에 거부한다."""
    values: dict[str, object] = {
        "source_snapshot_id": "seed:abc123",
        "random_seed": 42,
        "logical_date": datetime(2026, 9, 4, tzinfo=UTC),
        "order_count": 1,
        "anomaly_profile": "default",
        "generator_version": GENERATOR_VERSION,
    }
    values[field] = value

    with pytest.raises(ValueError, match=message):
        GeneratorConfig(**values)  # type: ignore[arg-type]
