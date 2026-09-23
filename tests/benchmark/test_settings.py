"""Benchmark 격리 설정의 fail-closed 계약을 검증한다."""

from __future__ import annotations

import pytest

from src.benchmark.settings import benchmark_settings


def test_benchmark_settings_rejects_missing_isolation_value(monkeypatch) -> None:
    """전용 저장소 값 하나라도 없으면 실행을 거부한다."""
    values = _isolation_values()
    del values["BENCHMARK_SEAWEEDFS_BUCKET"]
    monkeypatch.setattr("src.benchmark.settings.environment_values", lambda: values)

    with pytest.raises(ValueError, match="BENCHMARK_SEAWEEDFS_BUCKET"):
        benchmark_settings()


def test_benchmark_settings_rejects_operational_storage(monkeypatch) -> None:
    """전용 DB나 Bucket이 운영 값과 같으면 실행을 거부한다."""
    values = _isolation_values()
    values["BENCHMARK_PIPELINE_METADATA_DB"] = values["PIPELINE_METADATA_DB"]
    monkeypatch.setattr("src.benchmark.settings.environment_values", lambda: values)

    with pytest.raises(ValueError, match="must differ"):
        benchmark_settings()


def _isolation_values() -> dict[str, str]:
    """격리 Guard 검증에 필요한 환경 변수 값을 반환한다."""
    return {
        "COMMERCE_SOURCE_DB": "commerce_source",
        "PIPELINE_METADATA_DB": "pipeline_metadata",
        "SEAWEEDFS_BUCKET": "commerce-lake",
        "BENCHMARK_COMMERCE_SOURCE_DB": "benchmark_source",
        "BENCHMARK_PIPELINE_METADATA_DB": "benchmark_metadata",
        "BENCHMARK_SEAWEEDFS_BUCKET": "benchmark-lake",
    }
