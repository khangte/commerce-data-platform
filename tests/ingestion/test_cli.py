"""Phase 3 수동 수집 CLI의 입력과 실행 경계를 검증한다."""

from __future__ import annotations

import json
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from src.ingestion import __main__ as cli


def test_parse_logical_date_normalizes_offset_and_rejects_naive_input() -> None:
    """CLI 논리 시각은 UTC로 정규화하며 Offset 없는 값은 거부한다."""
    assert cli.parse_logical_date("2026-09-08T09:00:00+09:00") == datetime(2026, 9, 8, tzinfo=UTC)
    with pytest.raises(ValueError, match="UTC offset"):
        cli.parse_logical_date("2026-09-08T00:00:00")


def test_main_runs_selected_tables_under_one_global_lease(monkeypatch, tmp_path, capsys) -> None:
    """선택 Table은 하나의 Warehouse Lease를 공유하고 결과 JSON으로 출력한다."""
    calls: list[tuple[str, object]] = []

    @contextmanager
    def _freeze(*_: object, **kwargs: object):
        """CLI가 전달한 Owner ID를 기록하고 고정 Global Lease를 제공한다."""
        calls.append(("freeze", kwargs["owner_id"]))
        yield SimpleNamespace(owner_type="WAREHOUSE")

    def _ingest(*_: object, **kwargs: object) -> SimpleNamespace:
        """선택 Table 요청과 공유 Lease를 기록한 성공 결과를 반환한다."""
        request = _[2]
        calls.append((request.source_table, kwargs["source_lease"]))
        return SimpleNamespace(
            manifest_key=f"bronze/{request.source_table}/manifest.json",
            object_key=f"bronze/{request.source_table}/data.parquet",
            row_count=3,
            rows_rejected=0,
            run=SimpleNamespace(run_id=uuid.uuid4(), source_table=request.source_table),
            status="SUCCESS",
        )

    monkeypatch.setattr(cli.PostgresSettings, "from_environment", staticmethod(lambda: object()))
    monkeypatch.setattr(cli.SeaweedFSSettings, "from_environment", staticmethod(lambda: object()))
    monkeypatch.setattr(cli, "warehouse_source_freeze", _freeze)
    monkeypatch.setattr(cli, "ingest_table", _ingest)

    cli.main(
        [
            "--dag-id",
            "manual_initial_load",
            "--logical-date",
            "2026-09-08T00:00:00Z",
            "--tables",
            "orders",
            "customers",
            "--local-directory",
            str(tmp_path),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert [item["source_table"] for item in payload["tables"]] == ["orders", "customers"]
    assert [call[0] for call in calls] == ["freeze", "orders", "customers"]
    assert calls[1][1] is calls[2][1]
