"""Serving 파일의 serving_domain_status가 부분 게시 상태를 그대로 담는지 검증한다."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from src.serving.export import ServingPaths, export_serving_mart
from src.warehouse.domain_gate import DomainHolds
from src.warehouse.duckdb_files import file_identity
from tests.serving.test_export import _create_published_mart


def _published_after_partial_publish(path: Path) -> None:
    """subscriptions를 보류해 결제가 직전 게시 값으로 남은 Published 파일을 만든다."""
    _create_published_mart(path)
    with duckdb.connect(str(path)) as connection:
        connection.execute("ALTER TABLE facts.fct_order ADD COLUMN purchase_at TIMESTAMPTZ")
        connection.execute("UPDATE facts.fct_order SET purchase_at = '2026-09-22 09:50:00+00'")
        connection.execute("CREATE TABLE facts.fct_subscription_payment (payment_at TIMESTAMPTZ)")
        connection.execute(
            "INSERT INTO facts.fct_subscription_payment VALUES ('2026-09-20 12:00:00+00')"
        )


def _status(serving: Path) -> list[tuple]:
    with duckdb.connect(str(serving), read_only=True) as connection:
        return connection.execute(
            "SELECT domain, as_of, refreshed_at, held, consecutive_holds FROM serving_domain_status"
            " ORDER BY domain"
        ).fetchall()


def test_partial_publish_status_marks_held_domain(tmp_path: Path) -> None:
    """보류 도메인은 held=true와 이전 as_of, 다른 도메인은 held=false와 최신 as_of다."""
    published = tmp_path / "warehouse.duckdb"
    _published_after_partial_publish(published)
    paths = ServingPaths.under(tmp_path / "serving")
    export_serving_mart(
        published, paths, publish_run_id=uuid.uuid4(), mart_hashes={},
        published_identity=file_identity(published), now=datetime(2026, 9, 22, 10, tzinfo=UTC),
        holds=DomainHolds(
            {"orders": 0, "subscriptions": 2},
            {"orders": datetime(2026, 9, 22, 10, tzinfo=UTC), "subscriptions": None},
        ),
    )
    rows = _status(paths.serving)
    assert [
        (domain, as_of.astimezone(UTC), refreshed and refreshed.astimezone(UTC), held, count)
        for domain, as_of, refreshed, held, count in rows
    ] == [
        ("orders", datetime(2026, 9, 22, 9, 50, tzinfo=UTC), datetime(2026, 9, 22, 10, tzinfo=UTC),
         False, 0),
        ("subscriptions", datetime(2026, 9, 20, 12, tzinfo=UTC), None, True, 2),
    ]


def test_status_without_hold_info_keeps_as_of(tmp_path: Path) -> None:
    """보류 정보를 받지 못하면 refreshed_at·held·consecutive_holds는 NULL이고 as_of는 그대로 잰다."""
    published = tmp_path / "warehouse.duckdb"
    _create_published_mart(published)
    paths = ServingPaths.under(tmp_path / "serving")
    export_serving_mart(
        published, paths, publish_run_id=uuid.uuid4(), mart_hashes={},
        published_identity=file_identity(published), now=datetime(2026, 9, 22, tzinfo=UTC),
    )
    assert _status(paths.serving) == [
        ("orders", None, None, None, None), ("subscriptions", None, None, None, None),
    ]
