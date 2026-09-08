"""Metadata-backed Bronze File Catalog와 Schema Contract를 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime

import duckdb
import pytest

from src.ingestion import catalog
from src.ingestion.catalog import BronzeCatalogEntry, sync_bronze_catalog
from src.ingestion.schema import SourceContractError


def test_catalog_replaces_entries_with_committed_metadata_snapshot(monkeypatch, tmp_path) -> None:
    """동기화는 전달된 Commit Snapshot만 DuckDB Catalog에 남긴다."""
    entries = (
        BronzeCatalogEntry(
            "orders",
            "bronze/orders/data.parquet",
            1,
            "batch",
            datetime(2026, 9, 7, tzinfo=UTC),
            2,
            "a" * 64,
        ),
    )
    monkeypatch.setattr(catalog, "_committed_entries", lambda _: entries)
    database_path = tmp_path / "warehouse.duckdb"

    result = sync_bronze_catalog(object(), database_path)

    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        rows = connection.execute(
            "SELECT source_table, object_key, schema_version, row_count FROM control.bronze_files"
        ).fetchall()
    finally:
        connection.close()
    assert result == entries
    assert rows == [("orders", "bronze/orders/data.parquet", 1, 2)]


def test_catalog_stops_before_duckdb_write_for_unsupported_schema_version(monkeypatch, tmp_path) -> None:
    """Metadata에 미지원 Version이 있으면 SOURCE_CONTRACT_ERROR로 동기화를 차단한다."""
    monkeypatch.setattr(
        catalog,
        "_committed_entries",
        lambda _: (
            BronzeCatalogEntry(
                "orders",
                "bronze/orders/data.parquet",
                2,
                "batch",
                datetime(2026, 9, 7, tzinfo=UTC),
                2,
                "a" * 64,
            ),
        ),
    )

    with pytest.raises(SourceContractError, match="SOURCE_CONTRACT_ERROR"):
        sync_bronze_catalog(object(), tmp_path / "warehouse.duckdb")
