"""Phase 3 Bronze 수집을 수동 실행하는 CLI를 제공한다."""

from __future__ import annotations

import argparse
import json
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path

from src.common.database import PROJECT_ROOT, PostgresSettings
from src.ingestion.catalog import sync_bronze_catalog
from src.ingestion.lease import warehouse_source_freeze
from src.ingestion.service import TableIngestionRequest, TableIngestionResult, ingest_table
from src.ingestion.storage import SeaweedFSSettings
from src.ingestion.tables import TABLE_CONFIGS

DEFAULT_LOCAL_DIRECTORY = Path(tempfile.gettempdir()) / "commerce-data-platform" / "ingestion"
DEFAULT_CATALOG_PATH = PROJECT_ROOT / "data" / "warehouse" / "warehouse.duckdb"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """DAG 식별자·UTC 논리 시각·선택 Table을 수동 수집 CLI 인자로 읽는다."""
    parser = argparse.ArgumentParser(
        description="Extract selected source tables into verified SeaweedFS Bronze Parquet objects."
    )
    parser.add_argument("--dag-id", required=True, help="Stable DAG or manual-run identifier.")
    parser.add_argument(
        "--logical-date", required=True, help="UTC logical date in ISO-8601 format."
    )
    parser.add_argument(
        "--tables",
        required=True,
        nargs="+",
        choices=sorted(TABLE_CONFIGS),
        help="One or more source tables to ingest in the supplied order.",
    )
    parser.add_argument(
        "--local-directory",
        type=Path,
        default=DEFAULT_LOCAL_DIRECTORY,
        help="Temporary local Parquet directory. Defaults to the system temporary directory.",
    )
    parser.add_argument(
        "--sync-catalog",
        action="store_true",
        help="Synchronize committed Bronze objects to the DuckDB file catalog after success.",
    )
    parser.add_argument(
        "--catalog-path",
        type=Path,
        default=DEFAULT_CATALOG_PATH,
        help="DuckDB catalog path used only with --sync-catalog.",
    )
    return parser.parse_args(argv)


def parse_logical_date(value: str) -> datetime:
    """UTC Offset을 포함한 CLI 논리 시각을 UTC로 정규화한다."""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError("--logical-date must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(
            "--logical-date must include a UTC offset, for example 2026-09-08T00:00:00Z"
        )
    return parsed.astimezone(UTC)


def main(argv: list[str] | None = None) -> None:
    """선택 Table을 하나의 Warehouse Global Lease 안에서 적재하고 결과 JSON을 출력한다."""
    args = parse_args(argv)
    logical_date = parse_logical_date(args.logical_date)
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    owner_id = uuid.uuid4()
    results: list[TableIngestionResult] = []
    with warehouse_source_freeze(postgres, owner_id=owner_id) as source_lease:
        for source_table in args.tables:
            request = TableIngestionRequest.for_dag_run(
                source_table=source_table,
                dag_id=args.dag_id,
                logical_date=logical_date,
            )
            results.append(
                ingest_table(
                    postgres,
                    storage,
                    request,
                    local_directory=args.local_directory.resolve(),
                    source_lease=source_lease,
                )
            )
    catalog_entries = (
        sync_bronze_catalog(postgres, args.catalog_path.resolve()) if args.sync_catalog else ()
    )
    print(
        json.dumps(
            {
                "dag_id": args.dag_id,
                "logical_date": logical_date.isoformat(),
                "catalog_entry_count": len(catalog_entries),
                "catalog_synced": args.sync_catalog,
                "tables": [_result_json(result) for result in results],
            },
            sort_keys=True,
        )
    )


def _result_json(result: TableIngestionResult) -> dict[str, object]:
    """CLI 출력에 필요한 실행 상태·Object Key·행 수만 공개 JSON으로 변환한다."""
    return {
        "manifest_key": result.manifest_key,
        "object_key": result.object_key,
        "row_count": result.row_count,
        "rows_rejected": result.rows_rejected,
        "run_id": str(result.run.run_id),
        "source_table": result.run.source_table,
        "status": result.status,
    }


if __name__ == "__main__":
    main()
