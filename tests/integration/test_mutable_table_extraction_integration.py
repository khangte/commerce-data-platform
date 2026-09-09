"""Source Table의 설정 기반 고정 범위 Keyset 추출을 검증한다."""

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
@pytest.mark.parametrize(
    "source_table", ("customers", "customer_memberships", "products", "sellers")
)
def test_source_tables_use_the_configured_fixed_range_and_complete_primary_key(
    source_table: str,
) -> None:
    """계정·멤버십·기준정보 Table은 설정한 Cursor 범위를 여러 Page로 완전하게 읽는다."""
    settings = PostgresSettings.from_environment()
    config = table_config(source_table)
    lower_bound = _lower_bound_before_five_latest_rows(settings, config)
    expected = _expected_cursors(settings, config, lower_bound)

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
    settings: PostgresSettings, config
) -> CursorPosition:
    """여러 Page를 만들 수 있도록 최신 다섯 Row 바로 전 Cursor를 읽는다."""
    with settings.source_connection() as connection:
        row = connection.execute(
            f"""
            SELECT {', '.join(config.cursor_columns)}
            FROM {config.source_table}
            ORDER BY {_cursor_order(config, descending=True)}
            OFFSET 5 LIMIT 1
            """
        ).fetchone()
    if row is None:
        raise RuntimeError(f"The seeded source must contain at least six {config.source_table}")
    return CursorPosition(row[0], (row[1],))


def _expected_cursors(
    settings: PostgresSettings, config, lower_bound: CursorPosition
) -> list[CursorPosition]:
    """Source SQL로 같은 Cursor 범위의 기대 Row를 정렬해 읽는다."""
    cursor_columns = ", ".join(config.cursor_columns)
    placeholders = ", ".join("%s" for _ in config.cursor_columns)
    with settings.source_connection() as connection:
        rows = connection.execute(
            f"""
            SELECT {cursor_columns}
            FROM {config.source_table}
            WHERE ({cursor_columns}) > ({placeholders})
            ORDER BY {_cursor_order(config)}
            """,
            (lower_bound.timestamp, *lower_bound.keys),
        ).fetchall()
    return [CursorPosition(row[0], (row[1],)) for row in rows]


def _cursor_order(config, *, descending: bool = False) -> str:
    """설정된 Cursor Key의 C Collation과 정렬 방향을 SQL로 구성한다."""
    suffix = " DESC" if descending else ""
    columns = [config.cursor_timestamp_column]
    columns.extend(f'{column} COLLATE "C"' for column in config.cursor_key_columns)
    return ", ".join(f"{column}{suffix}" for column in columns)
