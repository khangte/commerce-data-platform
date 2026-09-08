"""SeaweedFS Bronze Parquet과 VERIFIED Manifest를 터미널에서 조회한다."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.service import table_object_keys
from src.ingestion.storage import SeaweedFSSettings, seaweedfs_s3_client
from src.ingestion.tables import TABLE_CONFIGS


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """조회할 Source Table·Batch·논리 시각과 샘플 행 수를 CLI 인자로 읽는다."""
    parser = argparse.ArgumentParser(
        description="Print a SeaweedFS Bronze manifest, Parquet schema, row count, and sample rows."
    )
    parser.add_argument("--source-table", required=True, choices=sorted(TABLE_CONFIGS))
    parser.add_argument("--batch-id", required=True)
    parser.add_argument(
        "--logical-date", required=True, help="UTC logical date in ISO-8601 format."
    )
    parser.add_argument(
        "--limit", type=int, default=5, help="Number of leading Parquet rows to print."
    )
    return parser.parse_args(argv)


def parse_logical_date(value: str) -> datetime:
    """UTC Offset을 포함한 논리 시각 문자열을 Object Key용 UTC 시각으로 정규화한다."""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError("--logical-date must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(
            "--logical-date must include a UTC offset, for example 2026-09-08T00:00:00Z"
        )
    return parsed.astimezone(UTC)


def inspect_bronze(
    settings: SeaweedFSSettings,
    *,
    source_table: str,
    batch_id: str,
    logical_date: datetime,
    limit: int = 5,
) -> dict[str, object]:
    """Final Bronze Object의 Manifest·Schema·행 수·선두 행을 조회 가능한 값으로 반환한다."""
    if limit < 0:
        raise ValueError("--limit must be greater than or equal to zero")
    object_key, manifest_key = table_object_keys(source_table, batch_id, logical_date)
    client = seaweedfs_s3_client(settings)
    manifest = json.loads(
        client.get_object(Bucket=settings.bucket, Key=manifest_key)["Body"].read()
    )
    payload = client.get_object(Bucket=settings.bucket, Key=object_key)["Body"].read()
    table = pq.read_table(pa.BufferReader(payload))
    return {
        "manifest": manifest,
        "object_key": object_key,
        "parquet_row_count": table.num_rows,
        "schema": str(table.schema),
        "sample_rows": table.slice(0, limit).to_pylist(),
    }


def main(argv: list[str] | None = None) -> None:
    """환경 설정을 읽고 지정 Bronze Object의 확인 결과를 JSON으로 출력한다."""
    args = parse_args(argv)
    result = inspect_bronze(
        SeaweedFSSettings.from_environment(),
        source_table=args.source_table,
        batch_id=args.batch_id,
        logical_date=parse_logical_date(args.logical_date),
        limit=args.limit,
    )
    print(json.dumps(result, default=str, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
