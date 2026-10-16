"""Mart 전용 Serving DuckDB 내보내기를 검증한다."""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from src.serving.export import ServingPaths, export_serving_mart
from src.serving.metabase_repoint import repoint_and_prune_serving
from src.warehouse.duckdb_files import FileIdentity, file_identity
from src.warehouse.mart_hash import mart_logical_hashes, target_for


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
        published_identity=file_identity(published),
        mart_hashes=mart_logical_hashes(
            published, (target_for("dimensions.dim_date"), target_for("facts.fct_order"))
        ),
        now=datetime(2026, 9, 22, tzinfo=UTC),
    )

    assert result.export_id == export_id
    assert result.row_counts == {
        "dimensions.dim_date": 1,
        "facts.fct_order": 2,
        "metrics.rpt_orders": 2,
    }
    assert result.serving_path == paths.serving
    versioned = paths.serving.parent / "exports" / f"{export_id}.duckdb"
    assert versioned.is_file()
    assert os.stat(versioned).st_ino == os.stat(paths.serving).st_ino
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
        json.dumps(
            mart_logical_hashes(
                published, (target_for("dimensions.dim_date"), target_for("facts.fct_order"))
            ),
            separators=(",", ":"),
            sort_keys=True,
        ),
        '{"dimensions.dim_date":1,"facts.fct_order":2,"metrics.rpt_orders":2}',
    )


def test_export_rejects_a_different_file_before_creating_files(tmp_path: Path) -> None:
    """게시 파일 식별값이 다르면 어떤 서빙 파일이나 디렉터리도 만들지 않는다."""
    published = tmp_path / "warehouse.duckdb"
    _create_published_mart(published)
    paths = ServingPaths.under(tmp_path / "serving")
    actual = file_identity(published)
    with pytest.raises(ValueError, match="identity"):
        export_serving_mart(
            published, paths, publish_run_id=uuid.uuid4(), mart_hashes={},
            published_identity=FileIdentity(actual.inode + 1, actual.mtime_ns),
            now=datetime.now(UTC),
        )
    assert not paths.serving.exists()
    assert not paths.build_dir.exists()
    assert not (paths.serving.parent / "exports").exists()


def test_export_rejects_the_same_inode_with_a_different_mtime(tmp_path: Path) -> None:
    """inode가 같아도 수정 시각이 다른 파일을 오래된 게시 실행으로 내보내지 않는다."""
    published = tmp_path / "warehouse.duckdb"
    _create_published_mart(published)
    expected = file_identity(published)
    os.utime(published, ns=(expected.mtime_ns + 1_000, expected.mtime_ns + 1_000))
    paths = ServingPaths.under(tmp_path / "serving")
    with pytest.raises(ValueError, match="identity"):
        export_serving_mart(
            published, paths, publish_run_id=uuid.uuid4(), mart_hashes={},
            published_identity=expected, now=datetime.now(UTC),
        )
    assert not paths.serving.exists()
    assert not paths.build_dir.exists()
    assert not (paths.serving.parent / "exports").exists()


def test_export_rejects_a_published_file_replaced_during_copy(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """복사 도중 게시 파일이 교체되면 서빙 교체를 거부하고 임시 파일을 정리한다."""
    from src.serving import export

    published = tmp_path / "warehouse.duckdb"
    other = tmp_path / "other.duckdb"
    _create_published_mart(published)
    _create_published_mart(other)
    paths = ServingPaths.under(tmp_path / "serving")
    original = export._copy_mart_objects

    def copy_and_replace(*args) -> dict[str, int]:
        """실제 복사를 마친 뒤 다른 게시 파일로 원자 교체한다."""
        counts = original(*args)
        os.replace(other, published)
        return counts

    monkeypatch.setattr(export, "_copy_mart_objects", copy_and_replace)
    with pytest.raises(ValueError, match="identity"):
        export_serving_mart(
            published, paths, publish_run_id=uuid.uuid4(), mart_hashes={},
            published_identity=file_identity(published), now=datetime.now(UTC),
        )
    assert not paths.serving.exists()
    assert not list(paths.build_dir.iterdir())
    assert not list((paths.serving.parent / "exports").glob("*.duckdb"))


def test_export_keeps_all_versions_until_repoint_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """재지정 실패가 반복되면 모든 버전 파일을 보존한다."""
    published = tmp_path / "warehouse.duckdb"
    _create_published_mart(published)
    paths = ServingPaths.under(tmp_path / "serving")
    exported = []
    status = "FAILED"

    def fake_repoint(*args) -> str:
        """재지정 실패와 성공을 순서대로 재현한다."""
        return status

    monkeypatch.setattr("src.serving.metabase_repoint.repoint_metabase_serving", fake_repoint)
    for _ in range(4):
        result = export_serving_mart(
            published, paths, publish_run_id=uuid.uuid4(), mart_hashes={},
            published_identity=file_identity(published), now=datetime.now(UTC)
        )
        exported.append(result.export_id)
        assert repoint_and_prune_serving(
            str(result.export_id), "url", "key", "2", paths.serving.parent / "exports"
        ) == "FAILED"
    versions = {path.stem for path in (paths.serving.parent / "exports").glob("*.duckdb")}
    assert versions == {str(export_id) for export_id in exported}

    status = "SUCCESS"
    assert repoint_and_prune_serving(
        str(exported[-1]), "url", "key", "2", paths.serving.parent / "exports"
    ) == "SUCCESS"
    versions = {path.stem for path in (paths.serving.parent / "exports").glob("*.duckdb")}
    assert versions == {str(export_id) for export_id in exported[-3:]}
