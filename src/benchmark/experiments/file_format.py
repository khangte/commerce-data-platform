"""실험 B — 같은 Bronze 내용을 CSV와 Parquet으로 읽을 때 비용을 비교한다.

Postgres·Generator에 의존하지 않는다. `sellers` Schema로 결정적 고정 Row Bronze
Parquet 하나를 SeaweedFS에 직접 올리고, 그 Parquet에서 CSV Mirror를 만들어 두
형식을 같은 Projection·ORDER BY로 읽는다. CSV Export는 측정 밖에서 수행하고
`data/benchmarks/` 밖에는 쓰지 않으며, 어떤 Pipeline 코드도 이 CSV를 읽지 않는다.
"""

from __future__ import annotations

import hashlib
import tempfile
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from src.benchmark.config import BenchmarkScenario, RunConfig
from src.benchmark.duckdb_s3 import configure_s3
from src.benchmark.experiments import EXPERIMENTS, SCENARIOS
from src.benchmark.measure import RowCounts, measure
from src.benchmark.runner import ArmResult
from src.benchmark.settings import benchmark_settings
from src.benchmark.store import BENCHMARK_DATA_ROOT
from src.common.row_hash import canonical_row_json
from src.ingestion.storage import (
    SeaweedFSSettings,
    StoredObject,
    ensure_bucket,
    stored_object_from_head,
    upload_new_file,
)
from src.ingestion.tables import table_config

FIXTURE_TABLE = "sellers"

FILE_FORMAT_SCENARIO = BenchmarkScenario(
    scenario="file_format",
    experiment="B",
    arms=("csv", "parquet"),
    cold=False,
    description="같은 Bronze 내용을 CSV와 Parquet으로 읽는 비용을 비교한다",
)
SCENARIOS[FILE_FORMAT_SCENARIO.scenario] = FILE_FORMAT_SCENARIO


def run_file_format_experiment(config: RunConfig) -> Mapping[str, ArmResult]:
    """고정 Row 수의 Bronze Fixture를 CSV·Parquet 두 형식으로 읽어 비용을 비교한다.

    Fixture 생성과 CSV Export는 측정 밖에서 수행하고, 읽어서 Canonical Hash를
    계산하는 구간만 측정한다.
    """
    storage = benchmark_settings().storage
    row_count = config.scale.order_count

    with tempfile.TemporaryDirectory(prefix="bench-file-format-") as tmp:
        tmp_path = Path(tmp)
        catalog_path = tmp_path / "catalog.duckdb"
        object_key = f"benchmark/file_format/{config.scale.name}/{FIXTURE_TABLE}.parquet"
        stored = _ensure_fixture(storage, tmp_path, object_key, row_count)
        _write_catalog(catalog_path, object_key)
        csv_path = export_csv_mirror(
            catalog_path,
            FIXTURE_TABLE,
            BENCHMARK_DATA_ROOT / config.benchmark_id / "csv_mirror" / f"{FIXTURE_TABLE}.csv",
        )
        csv_bytes = csv_path.stat().st_size

        with measure() as collector:
            parquet_hash, parquet_rows, parquet_payload_bytes = _read_parquet_arm(
                catalog_path, storage
            )
        parquet_measurement = collector.result()

        with measure() as collector:
            csv_hash, csv_rows, csv_payload_bytes = _read_csv_arm(catalog_path, storage, csv_path)
        csv_measurement = collector.result()

    return {
        "parquet": ArmResult(
            "parquet",
            parquet_measurement,
            RowCounts(
                rows_scanned=parquet_rows,
                rows_changed=None,
                input_bytes=stored.size,
                output_bytes=parquet_payload_bytes,
            ),
            parquet_hash,
        ),
        "csv": ArmResult(
            "csv",
            csv_measurement,
            RowCounts(
                rows_scanned=csv_rows,
                rows_changed=None,
                input_bytes=csv_bytes,
                output_bytes=csv_payload_bytes,
            ),
            csv_hash,
        ),
    }


EXPERIMENTS[FILE_FORMAT_SCENARIO.scenario] = run_file_format_experiment


def _ensure_fixture(
    storage: SeaweedFSSettings, tmp_path: Path, object_key: str, row_count: int
) -> StoredObject:
    """Scale별 `sellers` Fixture를 한 번만 올리고, 있으면 그대로 재사용한다.

    Object Key를 Scale에 묶어 두므로 반복 회차마다 다시 올리면 두 번째 회차부터
    `upload_new_file`의 불변 Key 검사에 걸린다. HEAD로 존재를 먼저 확인하고
    없을 때만 새로 만들어 올린다.
    """
    ensure_bucket(storage)
    try:
        return stored_object_from_head(storage, object_key)
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code")
        if error_code not in {"404", "NoSuchKey", "NotFound"}:
            raise
    config = table_config(FIXTURE_TABLE)
    schema = pa.schema([column.field for column in config.source_columns])
    base = datetime(2024, 1, 1, tzinfo=UTC)
    table = pa.table(
        {
            "seller_id": [f"seller-{index:06d}" for index in range(row_count)],
            "seller_city": [f"city-{index % 20}" for index in range(row_count)],
            "seller_state": [f"ST{index % 5}" for index in range(row_count)],
            "created_at": [base + timedelta(days=index % 3650) for index in range(row_count)],
            "updated_at": [
                base + timedelta(days=index % 3650, hours=1) for index in range(row_count)
            ],
        },
        schema=schema,
    )
    local_path = tmp_path / "fixture.parquet"
    pq.write_table(table, local_path)
    return upload_new_file(storage, object_key, local_path)


def _write_catalog(catalog_path: Path, object_key: str) -> None:
    """단일 Object Key만 담은 격리된 Bronze Catalog DuckDB 파일을 만든다."""
    with duckdb.connect(str(catalog_path)) as connection:
        connection.execute("CREATE SCHEMA control")
        connection.execute(
            "CREATE TABLE control.bronze_files (source_table VARCHAR, object_key VARCHAR)"
        )
        connection.execute(
            "INSERT INTO control.bronze_files VALUES (?, ?)", [FIXTURE_TABLE, object_key]
        )


def _object_keys(connection: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    """Catalog에서 한 Table의 Object Key 목록을 정렬해 가져온다."""
    return [
        row[0]
        for row in connection.execute(
            "SELECT object_key FROM control.bronze_files "
            "WHERE source_table = ? ORDER BY object_key",
            [table],
        ).fetchall()
    ]


def export_csv_mirror(catalog_path: Path, table: str, destination: Path) -> Path:
    """Catalog에 등록된 Table의 committed Bronze Parquet에서 Benchmark 전용 CSV Mirror를 만든다.

    Pipeline 코드는 이 CSV를 읽지 않는다. `data/benchmarks/` 밖에는 쓰지 않는다.
    """
    if BENCHMARK_DATA_ROOT not in destination.parents:
        raise ValueError("CSV mirror must live under data/benchmarks/")
    config = table_config(table)
    storage = benchmark_settings().storage
    column_list = ", ".join(f'"{column}"' for column in config.source_column_names)
    order_by = ", ".join(f'"{column}"' for column in config.primary_key_columns)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(catalog_path)) as connection:
        object_keys = _object_keys(connection, table)
        configure_s3(connection, storage)
        paths = ", ".join(f"'s3://{storage.bucket}/{key}'" for key in object_keys)
        connection.execute(
            f"COPY (SELECT {column_list} FROM read_parquet([{paths}], union_by_name=true) "
            f"ORDER BY {order_by}) TO '{destination}' (FORMAT CSV, HEADER)"
        )
    return destination


def _read_parquet_arm(catalog_path: Path, storage: SeaweedFSSettings) -> tuple[str, int, int]:
    """S3 Parquet을 직접 읽어 Canonical Hash·Row 수·Payload Byte 수를 계산한다."""
    config = table_config(FIXTURE_TABLE)
    column_list = ", ".join(f'"{column}"' for column in config.source_column_names)
    order_by = ", ".join(f'"{column}"' for column in config.primary_key_columns)
    with duckdb.connect(str(catalog_path)) as connection:
        object_keys = _object_keys(connection, FIXTURE_TABLE)
        configure_s3(connection, storage)
        paths = ", ".join(f"'s3://{storage.bucket}/{key}'" for key in object_keys)
        cursor = connection.execute(
            f"SELECT {column_list} FROM read_parquet([{paths}], union_by_name=true) "
            f"ORDER BY {order_by}"
        )
        return _hash_rows_with_payload_size(cursor)


def _read_csv_arm(
    catalog_path: Path, storage: SeaweedFSSettings, csv_path: Path
) -> tuple[str, int, int]:
    """CSV Mirror를 Parquet 원본과 같은 Column Type으로 읽어 Canonical Hash를 계산한다.

    Type을 Parquet Schema에 맞춰 강제하지 않으면 CSV 자동 추론 차이로 Hash가
    Parquet Arm과 어긋날 수 있다.
    """
    config = table_config(FIXTURE_TABLE)
    column_list = ", ".join(f'"{column}"' for column in config.source_column_names)
    order_by = ", ".join(f'"{column}"' for column in config.primary_key_columns)
    with duckdb.connect(str(catalog_path)) as connection:
        object_keys = _object_keys(connection, FIXTURE_TABLE)
        configure_s3(connection, storage)
        paths = ", ".join(f"'s3://{storage.bucket}/{key}'" for key in object_keys)
        schema_rows = connection.execute(
            f"DESCRIBE SELECT {column_list} FROM read_parquet([{paths}], union_by_name=true)"
        ).fetchall()
        columns = ", ".join(f"'{name}': '{duck_type}'" for name, duck_type, *_ in schema_rows)
        cursor = connection.execute(
            f"SELECT {column_list} FROM read_csv('{csv_path}', columns={{{columns}}}, "
            f"header=true) ORDER BY {order_by}"
        )
        return _hash_rows_with_payload_size(cursor)


def _hash_rows_with_payload_size(cursor) -> tuple[str, int, int]:
    """정렬된 Cursor를 Hash하며 Row 수와 Canonical JSON Payload Byte 수를 함께 센다.

    Row를 `fetchmany()`로 Python Tuple 1개씩 변환하는 대신 `to_arrow_reader()`
    Columnar Batch로 받아 Column 단위로 `to_pylist()`한 뒤 `zip`으로 Row를
    구성한다([[038_phase9-task16-bottleneck-selection]]) — DuckDB가 Row를
    1개씩 Python 객체로 변환하는 구간이 M Scale Duration의 절반을 차지하는
    것이 측정으로 확인됐다(git 이력의 `scripts/profile_file_format_read.py`). Hash
    알고리즘·JSON 직렬화·Row 순서는 그대로라 결과 Hash는 바뀌지 않는다.
    """
    column_names = [descriptor[0] for descriptor in cursor.description]
    digest = hashlib.sha256()
    row_count = 0
    payload_bytes = 0
    for batch in cursor.to_arrow_reader(batch_size=10_000):
        columns = [batch.column(index).to_pylist() for index in range(batch.num_columns)]
        for values in zip(*columns, strict=True):
            payload = canonical_row_json(dict(zip(column_names, values, strict=True))).encode(
                "utf-8"
            )
            digest.update(payload)
            digest.update(b"\n")
            payload_bytes += len(payload)
            row_count += 1
    return digest.hexdigest(), row_count, payload_bytes
