"""실험 A — Full Extract와 Incremental Extract가 같은 T1 상태에 도달하는 비용을 비교한다.

Generator 실행, Watermark 되감기, Fixture 준비는 `measure()` 밖에서 실행하고 Ingestion
자체만 측정한다. Full Arm은 `src/ingestion/reprocess.py`의 기존 되감기만 재사용하고
새 Full-refresh 경로를 만들지 않는다.
"""

from __future__ import annotations

import hashlib
import tempfile
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

from src.benchmark.config import BenchmarkScenario, RunConfig
from src.benchmark.experiments import EXPERIMENTS, SCENARIOS
from src.benchmark.measure import RowCounts, measure
from src.benchmark.runner import ArmResult
from src.common.database import PostgresSettings
from src.common.row_hash import hash_cursor_rows
from src.generator.config import GENERATOR_VERSION, GeneratorConfig
from src.generator.service import resolve_source_snapshot_id, run_generator
from src.ingestion.reprocess import rewind_tables
from src.ingestion.service import TableIngestionRequest, TableIngestionResult, ingest_table
from src.ingestion.storage import SeaweedFSSettings, stored_object_from_head
from src.ingestion.tables import TABLE_CONFIGS, table_config

EXTRACT_SCENARIO = BenchmarkScenario(
    scenario="extract",
    experiment="A",
    arms=("full", "incremental"),
    cold=False,
    description="Full Extract와 Incremental Extract가 같은 T1 상태에 도달하는 시간을 비교한다",
)
SCENARIOS[EXTRACT_SCENARIO.scenario] = EXTRACT_SCENARIO


def run_extract_experiment(config: RunConfig) -> Mapping[str, ArmResult]:
    """T0에서 `change_rate`만큼 늘린 T1까지, Incremental과 Full Extract를 각각 측정한다.

    T0 선적재·Full Arm 되감기·T1 Generator 실행은 측정 밖에서 수행해 두 Arm 모두
    "T1 상태로 Ingest를 마치는" 순수 Ingestion 비용만 비교하도록 만든다.
    """
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    change_rate = config.parameters["change_rate"]
    order_count_t0 = config.scale.order_count
    order_count_t1 = order_count_t0 + max(1, round(order_count_t0 * change_rate))
    t0, t1 = _anchor_times(config.benchmark_id)

    pipeline_incremental = f"bench-extract-{config.benchmark_id}-incremental"
    pipeline_full = f"bench-extract-{config.benchmark_id}-full"

    with tempfile.TemporaryDirectory(prefix="bench-extract-") as tmp:
        tmp_path = Path(tmp)
        source_snapshot_id = resolve_source_snapshot_id(postgres)

        run_generator(
            _generator_config(source_snapshot_id, config.scale.random_seed, t0, order_count_t0),
            postgres,
        )
        t0_incremental = _ingest_all_tables(
            postgres, storage, pipeline_incremental, "t0", t0, tmp_path
        )
        # Full Arm은 T0 적재로 Watermark를 한 번 전진시켜야 되감기가 유효해진다.
        # (이미 Empty인 Watermark는 다시 Empty로 되감을 수 없다.) Row가 없어 Watermark가
        # 전진하지 않은 Table은 이미 되감긴 상태이므로 되감기 대상에서 뺀다.
        t0_full = _ingest_all_tables(postgres, storage, pipeline_full, "t0", t0, tmp_path)
        advanced_tables = tuple(
            result.run.source_table for result in t0_full if result.status == "SUCCESS"
        )
        if advanced_tables:
            rewind_tables(
                postgres,
                source_tables=advanced_tables,
                boundary=datetime(1970, 1, 1, tzinfo=UTC),
                pipeline_name=pipeline_full,
            )
        run_generator(
            _generator_config(source_snapshot_id, config.scale.random_seed, t1, order_count_t1),
            postgres,
        )

        with measure() as collector:
            incremental_t1 = _ingest_all_tables(
                postgres, storage, pipeline_incremental, "t1", t1, tmp_path
            )
        incremental_measurement = collector.result()

        with measure() as collector:
            full_t1 = _ingest_all_tables(postgres, storage, pipeline_full, "t1", t1, tmp_path)
        full_measurement = collector.result()

        incremental_hash, _ = _catalog_hash(
            tmp_path / "incremental-catalog.duckdb", t0_incremental + incremental_t1
        )
        # Full Arm은 T1 단계에서 이미 빈 상태부터 전체를 다시 추출했으므로
        # T1 결과만으로 전체 상태가 완성된다. T0 결과를 더하면 중복 집계된다.
        full_hash, _ = _catalog_hash(tmp_path / "full-catalog.duckdb", full_t1)

    return {
        "incremental": ArmResult(
            "incremental",
            incremental_measurement,
            _row_counts(incremental_t1, storage),
            incremental_hash,
        ),
        "full": ArmResult("full", full_measurement, _row_counts(full_t1, storage), full_hash),
    }


EXPERIMENTS[EXTRACT_SCENARIO.scenario] = run_extract_experiment


def _anchor_times(benchmark_id: str) -> tuple[datetime, datetime]:
    """Benchmark ID의 Timestamp 조각으로 반복마다 같은 T0·T1 논리 시각을 만든다."""
    _, _, compact = benchmark_id.rsplit("-", 2)
    t0 = datetime.strptime(compact, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    return t0, t0 + timedelta(hours=1)


def _generator_config(
    source_snapshot_id: str, random_seed: int, logical_date: datetime, order_count: int
) -> GeneratorConfig:
    """Benchmark 전용 결정적 Generator 입력을 만든다."""
    return GeneratorConfig(
        source_snapshot_id=source_snapshot_id,
        random_seed=random_seed,
        logical_date=logical_date,
        order_count=order_count,
        anomaly_profile="default",
        generator_version=GENERATOR_VERSION,
    )


def _ingest_all_tables(
    postgres: PostgresSettings,
    storage: SeaweedFSSettings,
    pipeline_name: str,
    phase: str,
    logical_date: datetime,
    tmp_path: Path,
) -> list[TableIngestionResult]:
    """등록된 9개 Table을 지정 Pipeline·Logical Date로 순서대로 수집한다."""
    results = []
    for source_table in TABLE_CONFIGS:
        request = TableIngestionRequest.for_dag_run(
            source_table=source_table,
            dag_id=f"{pipeline_name}-{phase}",
            logical_date=logical_date,
            pipeline_name=pipeline_name,
        )
        result = ingest_table(postgres, storage, request, local_directory=tmp_path / "bronze")
        if result.status not in ("SUCCESS", "SUCCESS_NO_DATA"):
            raise RuntimeError(f"{source_table} ingest failed: {result.status}")
        results.append(result)
    return results


def _row_counts(results: list[TableIngestionResult], storage: SeaweedFSSettings) -> RowCounts:
    """측정 구간 Ingest 결과의 Row 수·Object 크기 합계를 집계한다.

    Postgres에서 읽은 Byte 수는 깨끗하게 잴 방법이 없어 `input_bytes`는 항상 None이다.
    """
    rows = sum(result.row_count for result in results)
    output_bytes = sum(
        stored_object_from_head(storage, result.object_key).size
        for result in results
        if result.object_key is not None
    )
    return RowCounts(
        rows_scanned=rows, rows_changed=rows, input_bytes=None, output_bytes=output_bytes
    )


def _catalog_hash(catalog_path: Path, results: list[TableIngestionResult]) -> tuple[str, int]:
    """Ingest 결과로 격리 Catalog를 만들고 9개 Table의 결합 Hash를 계산한다."""
    _write_bronze_catalog(catalog_path, results)
    per_table = {table: bronze_logical_hash(catalog_path, table) for table in TABLE_CONFIGS}
    digest = hashlib.sha256()
    total_rows = 0
    for table in sorted(per_table):
        table_hash, row_count = per_table[table]
        digest.update(f"{table}:{table_hash}:{row_count}\n".encode())
        total_rows += row_count
    return digest.hexdigest(), total_rows


def _write_bronze_catalog(catalog_path: Path, results: list[TableIngestionResult]) -> None:
    """Ingest 결과의 Object Key만으로 격리된 Bronze Catalog DuckDB 파일을 만든다."""
    with duckdb.connect(str(catalog_path)) as connection:
        connection.execute("CREATE SCHEMA control")
        connection.execute(
            "CREATE TABLE control.bronze_files (source_table VARCHAR, object_key VARCHAR)"
        )
        rows = [
            (result.run.source_table, result.object_key)
            for result in results
            if result.object_key is not None
        ]
        connection.executemany("INSERT INTO control.bronze_files VALUES (?, ?)", rows)


def bronze_logical_hash(catalog_path: Path, table: str) -> tuple[str, int]:
    """Catalog에 등록된 한 Table의 커밋된 Bronze Row를 PK 순서로 Canonical Hash한다."""
    config = table_config(table)
    storage = SeaweedFSSettings.from_environment()
    column_list = ", ".join(f'"{column}"' for column in config.source_column_names)
    order_by = ", ".join(f'"{column}"' for column in config.primary_key_columns)
    with duckdb.connect(str(catalog_path)) as connection:
        object_keys = [
            row[0]
            for row in connection.execute(
                "SELECT object_key FROM control.bronze_files "
                "WHERE source_table = ? ORDER BY object_key",
                [table],
            ).fetchall()
        ]
        if not object_keys:
            return hashlib.sha256(b"").hexdigest(), 0
        _configure_s3(connection, storage)
        paths = ", ".join(f"'s3://{storage.bucket}/{key}'" for key in object_keys)
        cursor = connection.execute(
            f"SELECT {column_list} FROM read_parquet([{paths}], union_by_name=true) "
            f"ORDER BY {order_by}"
        )
        return hash_cursor_rows(cursor)


def _configure_s3(connection: duckdb.DuckDBPyConnection, storage: SeaweedFSSettings) -> None:
    """SeaweedFS S3 API를 읽기 위한 DuckDB httpfs 설정을 적용한다."""

    def _escaped(value: str) -> str:
        """SQL 문자열 리터럴에 넣을 수 있게 홑따옴표를 이스케이프한다."""
        return value.replace("'", "''")

    connection.execute("INSTALL httpfs")
    connection.execute("LOAD httpfs")
    connection.execute(f"SET s3_endpoint='{_escaped(f'{storage.host}:{storage.port}')}'")
    connection.execute("SET s3_region='us-east-1'")
    connection.execute("SET s3_url_style='path'")
    connection.execute("SET s3_use_ssl=false")
    connection.execute(f"SET s3_access_key_id='{_escaped(storage.access_key)}'")
    connection.execute(f"SET s3_secret_access_key='{_escaped(storage.secret_key)}'")
