"""Mart 전용 Serving DuckDB 내보내기를 검증한다."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from src.serving.export import ServingPaths, export_serving_mart


def _create_published_mart(path: Path) -> None:
    """Mart 외 스키마를 함께 가진 Published Warehouse 테스트 파일을 만든다."""
    with duckdb.connect(str(path)) as connection:
        for schema in ("dimensions", "facts", "metrics", "control", "staging", "intermediate"):
            connection.execute(f"CREATE SCHEMA {schema}")
        connection.execute("CREATE TABLE dimensions.dim_date (date_key INTEGER)")
        connection.execute("INSERT INTO dimensions.dim_date VALUES (20260922)")
        connection.execute("CREATE TABLE facts.fct_order (order_id VARCHAR)")
        connection.execute("INSERT INTO facts.fct_order VALUES ('order-1'), ('order-2')")
        connection.execute("CREATE VIEW metrics.rpt_orders AS SELECT * FROM facts.fct_order")
        connection.execute("CREATE TABLE control.bronze_files (file_id VARCHAR)")
        connection.execute("CREATE VIEW staging.stg_orders AS SELECT * FROM facts.fct_order")
        connection.execute("CREATE VIEW intermediate.int_orders AS SELECT * FROM facts.fct_order")


def test_export_copies_only_mart_objects_and_records_publish_evidence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Serving 파일은 Mart와 Publish가 기록한 Hash·행 수만 보존한다."""
    published = tmp_path / "warehouse.duckdb"
    _create_published_mart(published)
    paths = ServingPaths.under(tmp_path / "serving")
    publish_run_id = uuid.uuid4()
    export_id = uuid.uuid4()

    monkeypatch.setattr("src.serving.export.uuid.uuid4", lambda: export_id)
    result = export_serving_mart(
        published,
        paths,
        publish_run_id=publish_run_id,
        mart_hashes={"dimensions.dim_date": "a" * 64, "facts.fct_order": "b" * 64},
        now=datetime(2026, 9, 22, tzinfo=UTC),
    )

    assert result.export_id == export_id
    assert result.row_counts == {
        "dimensions.dim_date": 1,
        "facts.fct_order": 2,
        "metrics.rpt_orders": 2,
    }
    assert result.serving_path == paths.serving
    assert not paths.serving.with_suffix(".duckdb.wal").exists()

    with duckdb.connect(str(paths.serving), read_only=True) as connection:
        schemas = {
            row[0]
            for row in connection.execute(
                """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name IN ('dimensions', 'facts', 'metrics', 'control', 'staging', 'intermediate')
                """
            ).fetchall()
        }
        assert schemas == {"dimensions", "facts", "metrics"}
        assert connection.execute("SELECT count(*) FROM metrics.rpt_orders").fetchone() == (2,)
        manifest = connection.execute(
            """
            SELECT export_id, publish_run_id, storage_version, mart_result_hashes, row_counts
            FROM serving_manifest
            """
        ).fetchone()
    assert manifest == (
        export_id,
        publish_run_id,
        "v1.0.0",
        '{"dimensions.dim_date":"' + "a" * 64 + '","facts.fct_order":"' + "b" * 64 + '"}',
        '{"dimensions.dim_date":1,"facts.fct_order":2,"metrics.rpt_orders":2}',
    )


def test_export_refuses_a_published_file_with_a_wal(tmp_path: Path) -> None:
    """활성 Writer를 암시하는 Published WAL이 있으면 찢어진 복사를 거부한다."""
    published = tmp_path / "warehouse.duckdb"
    _create_published_mart(published)
    wal = published.with_name(f"{published.name}.wal")
    wal.write_text("active writer", encoding="utf-8")

    with pytest.raises(ValueError, match="WAL"):
        export_serving_mart(
            published,
            ServingPaths.under(tmp_path / "serving"),
            publish_run_id=uuid.uuid4(),
            mart_hashes={},
            now=datetime(2026, 9, 22, tzinfo=UTC),
        )
