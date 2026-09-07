"""실제 PostgreSQL `orders` Composite Cursor와 Keyset Pagination을 검증한다."""

from __future__ import annotations

import os

import pytest

from src.common.database import PostgresSettings
from src.ingestion.metadata import CursorPosition
from src.ingestion.orders import open_orders_snapshot

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
def test_orders_snapshot_paginates_only_the_fixed_composite_cursor_range() -> None:
    """고정 Upper Bound 안의 같은 Timestamp Page 경계에서 Key 누락·중복이 없다."""
    settings = PostgresSettings.from_environment()
    lower_bound = _lower_bound_before_five_latest_rows(settings)
    expected = _expected_order_cursors(settings, lower_bound)

    assert len(expected) == 5
    with open_orders_snapshot(settings, lower_bound, page_size=2) as snapshot:
        pages = tuple(snapshot.pages())

    actual = [record.cursor for page in pages for record in page.records]
    assert [len(page.records) for page in pages] == [2, 2, 1]
    assert actual == expected
    assert len({(cursor.timestamp, cursor.keys) for cursor in actual}) == len(actual)
    assert pages[-1].extract_upper_bound == expected[-1]


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
def test_orders_snapshot_reports_no_upper_bound_for_an_empty_range() -> None:
    """현재 최대 Cursor를 Lower Bound로 사용하면 Object 없는 Empty Range가 된다."""
    settings = PostgresSettings.from_environment()
    maximum_cursor = _maximum_cursor(settings)

    with open_orders_snapshot(settings, maximum_cursor, page_size=2) as snapshot:
        assert snapshot.extract_upper_bound is None
        assert tuple(snapshot.pages()) == ()


def _lower_bound_before_five_latest_rows(settings: PostgresSettings) -> CursorPosition:
    """Page 경계를 검증할 수 있도록 최신 다섯 Row 직전 Cursor를 반환한다."""
    with settings.source_connection() as connection:
        row = connection.execute(
            """
            SELECT updated_at, order_id
            FROM orders
            ORDER BY updated_at DESC, order_id COLLATE "C" DESC
            OFFSET 5
            LIMIT 1
            """
        ).fetchone()
    if row is None:
        raise RuntimeError("The seeded source must contain at least six orders")
    return CursorPosition(row[0], (row[1],))


def _expected_order_cursors(
    settings: PostgresSettings, lower_bound: CursorPosition
) -> list[CursorPosition]:
    """동일 범위의 예상 Cursor 목록을 Source SQL로 정렬해 반환한다."""
    with settings.source_connection() as connection:
        rows = connection.execute(
            """
            SELECT updated_at, order_id
            FROM orders
            WHERE (updated_at, order_id COLLATE "C") > (%s, %s)
            ORDER BY updated_at, order_id COLLATE "C"
            """,
            (lower_bound.timestamp, lower_bound.keys[0]),
        ).fetchall()
    return [CursorPosition(row[0], (row[1],)) for row in rows]


def _maximum_cursor(settings: PostgresSettings) -> CursorPosition:
    """Source의 현재 최대 `orders` Composite Cursor를 반환한다."""
    with settings.source_connection() as connection:
        row = connection.execute(
            """
            SELECT updated_at, order_id
            FROM orders
            ORDER BY updated_at DESC, order_id COLLATE "C" DESC
            LIMIT 1
            """
        ).fetchone()
    if row is None:
        raise RuntimeError("The seeded source must contain orders")
    return CursorPosition(row[0], (row[1],))
