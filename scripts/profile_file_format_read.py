"""file_format M Scale 읽기 구간(약 20초)이 어디서 걸리는지 가른다.

Task 16(038_phase9-task16-bottleneck-selection) 병목 선정 근거. 측정 3구간:
1. DuckDB S3 Read + `cursor.fetchmany(10_000)` Tuple 적재
2. Python `canonical_row_json` + SHA-256 누적 Hash Loop (위 결과에 대해)
3. `cursor.to_arrow_reader()` Columnar 적재(Row 단위 Python 변환 없음, 비교용)

`benchmark/file_format/M/sellers.parquet`가 이미 SeaweedFS에 있어야 한다
(`uv run python -m src.benchmark run --scenario file_format --scale M`을
먼저 한 번 실행해 Fixture를 만들어 둔다). 1회성 진단 스크립트다 — 정식
Scenario로 등록하지 않는다.

재현:
    PYTHONPATH=. uv run python scripts/profile_file_format_read.py
"""

from __future__ import annotations

import hashlib
import time

import duckdb

from src.benchmark.duckdb_s3 import configure_s3
from src.common.row_hash import canonical_row_json
from src.ingestion.storage import SeaweedFSSettings
from src.ingestion.tables import table_config

OBJECT_KEY = "benchmark/file_format/M/sellers.parquet"


def main() -> None:
    """세 구간을 순서대로 실측하고 초 단위로 찍는다."""
    storage = SeaweedFSSettings.from_environment()
    config = table_config("sellers")
    column_list = ", ".join(f'"{column}"' for column in config.source_column_names)
    order_by = ", ".join(f'"{column}"' for column in config.primary_key_columns)
    path = f"s3://{storage.bucket}/{OBJECT_KEY}"

    connection = duckdb.connect(":memory:")
    configure_s3(connection, storage)
    t0 = time.perf_counter()
    cursor = connection.execute(
        f"SELECT {column_list} FROM read_parquet(['{path}']) ORDER BY {order_by}"
    )
    column_names = [descriptor[0] for descriptor in cursor.description]
    rows = []
    while True:
        batch = cursor.fetchmany(10_000)
        if not batch:
            break
        rows.extend(batch)
    t1 = time.perf_counter()
    print(f"1. DuckDB read + fetchmany tuples: {t1 - t0:.3f}s, rows={len(rows)}")

    digest = hashlib.sha256()
    t2 = time.perf_counter()
    for row in rows:
        payload = canonical_row_json(dict(zip(column_names, row, strict=True))).encode("utf-8")
        digest.update(payload)
        digest.update(b"\n")
    t3 = time.perf_counter()
    print(f"2. Python canonical_row_json + hash loop: {t3 - t2:.3f}s")
    print(f"   subtotal (1+2): {t3 - t0:.3f}s")

    connection2 = duckdb.connect(":memory:")
    configure_s3(connection2, storage)
    t4 = time.perf_counter()
    cursor2 = connection2.execute(
        f"SELECT {column_list} FROM read_parquet(['{path}']) ORDER BY {order_by}"
    )
    table = cursor2.to_arrow_reader().read_all()
    t5 = time.perf_counter()
    print(
        f"3. DuckDB read + to_arrow_reader (columnar, no per-row Python): {t5 - t4:.3f}s, rows={table.num_rows}"
    )


if __name__ == "__main__":
    main()
