"""Membership Grain 분리 후 Source·Bronze 기준 스냅샷을 안전하게 다시 만든다."""

from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from src.common.database import PROJECT_ROOT, PostgresSettings, apply_sql_file
from src.generator.lease import (
    SourceMutationLease,
    acquire_source_mutation_lease,
    assert_source_mutation_lease,
    release_source_mutation_lease,
)
from src.ingestion.catalog import sync_bronze_catalog
from src.ingestion.lease import WAREHOUSE_OWNER_TYPE
from src.ingestion.service import TableIngestionRequest, ingest_table
from src.ingestion.storage import SeaweedFSSettings, list_object_keys, seaweedfs_s3_client
from src.ingestion.tables import TABLE_CONFIGS
from src.seed.loader import parse_seeded_at, run_seed

REBASELINE_DAG_ID = "membership_grain_rebaseline"
REBASELINE_PREFIXES = ("bronze/", "quarantine/", "_staging/")
SOURCE_TABLES = (
    "order_payments",
    "order_items",
    "orders",
    "customer_memberships",
    "customers",
    "products",
    "sellers",
)
PIPELINE_METADATA_TABLES = (
    "quarantine_batches",
    "bronze_objects",
    "pipeline_runs",
    "watermarks",
    "generator_runs",
    "seed_runs",
)


@dataclass(frozen=True)
class RebaselineInventory:
    """재기준화 전에 삭제될 Source·메타데이터·Object의 수를 나타낸다."""

    source_row_counts: dict[str, int]
    metadata_row_counts: dict[str, int]
    object_key_counts: dict[str, int]
    warehouse_exists: bool


@dataclass(frozen=True)
class RebaselineResult:
    """새 Seed·Bronze Catalog까지 완료한 재기준화 결과를 나타낸다."""

    inventory: RebaselineInventory
    seed_row_counts: dict[str, int]
    bronze_row_counts: dict[str, int]
    catalog_entry_count: int


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """재기준화에 필요한 Seed 시각과 파괴적 실행 확인 인자를 읽는다."""
    parser = argparse.ArgumentParser(
        description="Reset source, metadata, and Bronze before rebuilding the membership-grain baseline."
    )
    parser.add_argument(
        "--seeded-at",
        required=True,
        help="UTC baseline snapshot timestamp, for example 2026-09-03T00:00:00Z.",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "raw" / "olist",
        help="Directory containing the six required Olist CSV files.",
    )
    parser.add_argument(
        "--catalog-path",
        type=Path,
        default=PROJECT_ROOT / "data" / "warehouse" / "warehouse.duckdb",
        help="DuckDB catalog file rebuilt from the new committed Bronze objects.",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Actually delete the listed local Source, metadata, Bronze, and catalog state.",
    )
    return parser.parse_args(argv)


def inspect_rebaseline(
    postgres: PostgresSettings, storage: SeaweedFSSettings, catalog_path: Path
) -> RebaselineInventory:
    """현재 기준 상태의 정확한 삭제 범위를 읽기 전용으로 집계한다."""
    with postgres.source_connection() as connection:
        source_row_counts = {
            table_name: connection.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
            for table_name in _existing_source_tables(connection)
        }
    with postgres.pipeline_connection() as connection:
        metadata_row_counts = {
            table_name: connection.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
            for table_name in PIPELINE_METADATA_TABLES
        }
    object_key_counts = {
        prefix: len(list_object_keys(storage, prefix)) for prefix in REBASELINE_PREFIXES
    }
    return RebaselineInventory(
        source_row_counts=source_row_counts,
        metadata_row_counts=metadata_row_counts,
        object_key_counts=object_key_counts,
        warehouse_exists=catalog_path.is_file(),
    )


def run_rebaseline(
    *,
    input_dir: Path,
    seeded_at: datetime,
    catalog_path: Path,
    postgres: PostgresSettings,
    storage: SeaweedFSSettings,
) -> RebaselineResult:
    """기존 상태를 지운 뒤 Seed·7개 Bronze Table·DuckDB Catalog를 순서대로 다시 만든다."""
    inventory = inspect_rebaseline(postgres, storage, catalog_path)
    lease = acquire_source_mutation_lease(
        postgres, owner_type=WAREHOUSE_OWNER_TYPE, owner_id=uuid.uuid4()
    )
    try:
        _reset_bronze_objects(storage)
        _reset_catalog_file(catalog_path)
        _reset_source(postgres, lease)
        _reset_pipeline_metadata(postgres)
        seed_result = run_seed(input_dir, seeded_at, postgres)
        bronze_row_counts = _ingest_baseline(postgres, storage, seeded_at, lease)
        catalog_entries = sync_bronze_catalog(postgres, catalog_path)
    finally:
        release_source_mutation_lease(postgres, lease)
    return RebaselineResult(
        inventory=inventory,
        seed_row_counts=seed_result.table_row_counts,
        bronze_row_counts=bronze_row_counts,
        catalog_entry_count=len(catalog_entries),
    )


def _existing_source_tables(connection) -> tuple[str, ...]:
    """구 스키마에도 동작하도록 현재 존재하는 재기준화 대상 Source Table만 반환한다."""
    return tuple(
        table_name
        for table_name in SOURCE_TABLES
        if connection.execute("SELECT to_regclass(%s)", (f"public.{table_name}",)).fetchone()[0]
        is not None
    )


def _reset_bronze_objects(storage: SeaweedFSSettings) -> None:
    """Bronze·Quarantine·임시 Prefix의 Object만 1,000개 단위로 삭제하고 빈 상태를 확인한다."""
    client = seaweedfs_s3_client(storage)
    for prefix in REBASELINE_PREFIXES:
        keys = list_object_keys(storage, prefix)
        for offset in range(0, len(keys), 1_000):
            client.delete_objects(
                Bucket=storage.bucket,
                Delete={"Objects": [{"Key": key} for key in keys[offset : offset + 1_000]], "Quiet": True},
            )
        if list_object_keys(storage, prefix):
            raise RuntimeError(f"Failed to delete every object under {prefix}")


def _reset_catalog_file(catalog_path: Path) -> None:
    """정확히 지정된 DuckDB Catalog 파일만 제거해 새 Bronze 목록으로 다시 만들게 한다."""
    expected_path = PROJECT_ROOT / "data" / "warehouse" / "warehouse.duckdb"
    if catalog_path.resolve() != expected_path.resolve():
        raise ValueError("catalog_path must be the project warehouse.duckdb file")
    catalog_path.unlink(missing_ok=True)


def _reset_source(postgres: PostgresSettings, lease: SourceMutationLease) -> None:
    """새 Source DDL을 적용한 뒤 주문·계정·Membership Source 행을 한 Transaction으로 비운다."""
    assert_source_mutation_lease(postgres, lease)
    with postgres.source_connection() as connection:
        apply_sql_file(connection, "sql/source/001_create_source_tables.sql")
        with connection.transaction():
            connection.execute(f"TRUNCATE TABLE {', '.join(SOURCE_TABLES)}")


def _reset_pipeline_metadata(postgres: PostgresSettings) -> None:
    """Seed Guard와 Watermark를 포함한 재기준화 대상 실행 메타데이터를 한 Transaction으로 비운다."""
    with postgres.pipeline_connection() as connection, connection.transaction():
        connection.execute(f"TRUNCATE TABLE {', '.join(PIPELINE_METADATA_TABLES)}")


def _ingest_baseline(
    postgres: PostgresSettings,
    storage: SeaweedFSSettings,
    logical_date: datetime,
    lease: SourceMutationLease,
) -> dict[str, int]:
    """초기 Watermark에서 7개 Table을 수집하고 Source별 Bronze Row Count를 반환한다."""
    row_counts: dict[str, int] = {}
    for source_table in TABLE_CONFIGS:
        assert_source_mutation_lease(postgres, lease)
        result = ingest_table(
            postgres,
            storage,
            TableIngestionRequest.for_dag_run(
                source_table=source_table,
                dag_id=REBASELINE_DAG_ID,
                logical_date=logical_date,
            ),
            local_directory=PROJECT_ROOT / "data" / "generated" / "rebaseline",
            source_lease=lease,
        )
        if result.status != "SUCCESS":
            raise RuntimeError(f"Baseline ingestion did not succeed for {source_table}: {result.status}")
        row_counts[source_table] = result.row_count
    return row_counts


def main(argv: list[str] | None = None) -> None:
    """기본은 삭제 범위를 출력하고 `--confirm`일 때만 실제 재기준화를 실행한다."""
    args = parse_args(argv)
    seeded_at = parse_seeded_at(args.seeded_at)
    input_dir = args.input_dir.resolve()
    catalog_path = args.catalog_path.resolve()
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    inventory = inspect_rebaseline(postgres, storage, catalog_path)
    if not args.confirm:
        print(json.dumps({"dry_run": True, "inventory": inventory.__dict__}, sort_keys=True))
        return
    result = run_rebaseline(
        input_dir=input_dir,
        seeded_at=seeded_at,
        catalog_path=catalog_path,
        postgres=postgres,
        storage=storage,
    )
    print(json.dumps(result, default=lambda value: value.__dict__, sort_keys=True))


if __name__ == "__main__":
    main()
