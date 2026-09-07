"""Mutable Source Table의 설정 기반 고정 범위 Keyset 추출을 검증한다."""

from __future__ import annotations

import os

import pytest

from src.common.database import PostgresSettings
from src.ingestion.extract import open_table_snapshot
from src.ingestion.metadata import CursorPosition
from src.ingestion.tables import table_config

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
@pytest.mark.parametrize("source_table", ("customers", "products", "sellers"))
def test_mutable_tables_use_the_configured_fixed_range_and_complete_primary_key(
    source_table: str,
) -> None:
    """세 Mutable Table은 설정한 `(updated_at, PK)` 범위를 여러 Page로 완전하게 읽는다."""
    settings = PostgresSettings.from_environment()
    config = table_config(source_table)
    lower_bound = _lower_bound_before_five_latest_rows(settings, config.source_table)
    expected = _expected_cursors(settings, config.source_table, lower_bound)

    with open_table_snapshot(settings, config, lower_bound, page_size=2) as snapshot:
        pages = tuple(snapshot.pages())

    actual = [record.cursor for page in pages for record in page.records]
    assert len(expected) == 5
    assert [len(page.records) for page in pages] == [2, 2, 1]
    assert actual == expected
    assert all(
        tuple(record.values) == config.source_column_names
        for page in pages
        for record in page.records
    )


def _lower_bound_before_five_latest_rows(
    settings: PostgresSettings, source_table: str
) -> CursorPosition:
    """여러 Page를 만들 수 있도록 최신 다섯 Row 바로 전 Cursor를 읽는다."""
    with settings.source_connection() as connection:
        row = connection.execute(
            f"""
            SELECT updated_at, {source_table[:-1]}_id
            FROM {source_table}
            ORDER BY updated_at DESC, {source_table[:-1]}_id COLLATE "C" DESC
            OFFSET 5 LIMIT 1
            """
        ).fetchone()
    if row is None:
        raise RuntimeError(f"The seeded source must contain at least six {source_table}")
    return CursorPosition(row[0], (row[1],))


def _expected_cursors(
    settings: PostgresSettings, source_table: str, lower_bound: CursorPosition
) -> list[CursorPosition]:
    """Source SQL로 같은 Cursor 범위의 기대 Row를 정렬해 읽는다."""
    primary_key = f"{source_table[:-1]}_id"
    with settings.source_connection() as connection:
        rows = connection.execute(
            f"""
            SELECT updated_at, {primary_key}
            FROM {source_table}
            WHERE (updated_at, {primary_key} COLLATE "C") > (%s, %s)
            ORDER BY updated_at, {primary_key} COLLATE "C"
            """,
            (lower_bound.timestamp, lower_bound.keys[0]),
        ).fetchall()
    return [CursorPosition(row[0], (row[1],)) for row in rows]
