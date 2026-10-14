"""Phase 2 결정성 입력 계약을 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from src.generator.core.config import (
    GENERATOR_VERSION,
    GeneratorConfig,
    int_from_environment,
    parse_logical_date,
)


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
        "progress_time_scale": 1,
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
        ("progress_time_scale", 0, "progress_time_scale"),
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


def test_generator_version_is_1_15_0() -> None:
    """배율 입력 추가로 결정성 입력이 바뀌었으므로 Generator 버전을 올린다."""
    assert GENERATOR_VERSION == "1.15.0"


def test_from_values_accepts_progress_time_scale() -> None:
    """배율은 결정성 입력으로 보존된다."""
    config = GeneratorConfig.from_values(
        source_snapshot_id="seed:abc123",
        random_seed=42,
        logical_date="2026-09-04T00:00:00Z",
        order_count=1,
        anomaly_profile="default",
        generator_version=GENERATOR_VERSION,
        progress_time_scale=144,
    )
    assert config.progress_time_scale == 144
    assert config.deterministic_inputs()["progress_time_scale"] == 144


@pytest.mark.parametrize(
    ("raw", "expected"),
    ((None, 1), ("", 1), ("   ", 1), ("5", 5), (" 7 ", 7), ("0", 0)),
)
def test_int_from_environment_reads_value_or_default(
    monkeypatch: pytest.MonkeyPatch, raw: str | None, expected: int
) -> None:
    """미설정·빈 값·공백은 기본값, 정수 문자열은 그 값을 반환한다."""
    if raw is None:
        monkeypatch.delenv("TEST_INT_ENV", raising=False)
    else:
        monkeypatch.setenv("TEST_INT_ENV", raw)
    assert int_from_environment("TEST_INT_ENV", default=1, minimum=0) == expected


@pytest.mark.parametrize(("raw", "minimum"), (("abc", 0), ("1.5", 0), ("-1", 0), ("0", 1)))
def test_int_from_environment_rejects_invalid_values(
    monkeypatch: pytest.MonkeyPatch, raw: str, minimum: int
) -> None:
    """정수가 아니거나 최솟값 미만이면 변수명이 든 오류로 실패한다."""
    monkeypatch.setenv("TEST_INT_ENV", raw)
    with pytest.raises(ValueError, match="TEST_INT_ENV"):
        int_from_environment("TEST_INT_ENV", default=1, minimum=minimum)


def test_generator_config_rejects_boolean_progress_time_scale() -> None:
    """bool은 정수 배율로 받지 않는다."""
    with pytest.raises(TypeError, match="progress_time_scale"):
        GeneratorConfig("seed:abc123", 42, datetime(2026, 9, 4, tzinfo=UTC), 1, "default", GENERATOR_VERSION, True)


def test_generator_config_rejects_delayed_payment_profile() -> None:
    """지연 결제는 실행 Profile이 아니라 Scenario Fixture라서 Config 단계에서 거부한다."""
    with pytest.raises(ValueError, match="Unsupported anomaly_profile: delayed-payment"):
        GeneratorConfig.from_values(
            source_snapshot_id="seed:abc123",
            random_seed=42,
            logical_date="2026-09-04T00:00:00Z",
            order_count=1,
            anomaly_profile="delayed-payment",
            generator_version=GENERATOR_VERSION,
        )
