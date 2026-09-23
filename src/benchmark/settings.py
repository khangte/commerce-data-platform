"""운영 저장소와 분리된 Benchmark 연결 설정을 제공한다."""

from __future__ import annotations

from dataclasses import dataclass, replace

from src.common.database import PostgresSettings, environment_values
from src.ingestion.storage import SeaweedFSSettings


@dataclass(frozen=True)
class BenchmarkSettings:
    """격리된 Benchmark의 PostgreSQL·SeaweedFS 연결 설정 묶음이다."""

    postgres: PostgresSettings
    storage: SeaweedFSSettings


def benchmark_settings() -> BenchmarkSettings:
    """전용 DB와 Bucket이 명시되고 운영 저장소와 다를 때만 설정을 반환한다."""
    values = environment_values()
    _assert_isolated(values)
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    return BenchmarkSettings(
        postgres=replace(
            postgres,
            source_database=values["BENCHMARK_COMMERCE_SOURCE_DB"],
            pipeline_database=values["BENCHMARK_PIPELINE_METADATA_DB"],
        ),
        storage=replace(storage, bucket=values["BENCHMARK_SEAWEEDFS_BUCKET"]),
    )


def _assert_isolated(values: dict[str, str]) -> None:
    """필수 전용 저장소 값의 누락과 운영 저장소 재사용을 fail-closed로 거부한다."""
    pairs = (
        ("BENCHMARK_COMMERCE_SOURCE_DB", "COMMERCE_SOURCE_DB"),
        ("BENCHMARK_PIPELINE_METADATA_DB", "PIPELINE_METADATA_DB"),
        ("BENCHMARK_SEAWEEDFS_BUCKET", "SEAWEEDFS_BUCKET"),
    )
    missing = [benchmark_name for benchmark_name, _ in pairs if not values.get(benchmark_name)]
    if missing:
        raise ValueError(
            "Benchmark isolation requires environment variables: " + ", ".join(missing)
        )
    shared = [
        benchmark_name
        for benchmark_name, operational_name in pairs
        if values[benchmark_name] == values.get(operational_name)
    ]
    if shared:
        raise ValueError(
            "Benchmark isolation values must differ from operational values: "
            + ", ".join(shared)
        )
