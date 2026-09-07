"""Child Table의 복합 PK Cursor 설정 기반 추출을 검증한다."""

from __future__ import annotations

import os

import pyarrow as pa
import pytest

from src.common.database import PostgresSettings
from src.ingestion.extract import open_table_snapshot
from src.ingestion.metadata import CursorPosition
from src.ingestion.tables import TableConfig, table_config

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
@pytest.mark.parametrize("source_table", ("order_items", "order_payments"))
def test_child_tables_use_all_primary_key_columns_at_keyset_page_boundaries(
    source_table: str,
) -> None:
    """Append Item과 Mutable Payment는 전체 복합 PK Tie-breaker로 범위를 완전하게 읽는다."""
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
    assert all(len(cursor.keys) == len(config.primary_key_columns) for cursor in actual)


def _lower_bound_before_five_latest_rows(
    settings: PostgresSettings, config: TableConfig
) -> CursorPosition:
    """복합 Cursor Page 경계 검증을 위한 최신 다섯 Row 직전 값을 읽는다."""
    cursor_columns = ", ".join(config.cursor_columns)
    with settings.source_connection() as connection:
        row = connection.execute(
            f"""
            SELECT {cursor_columns}
            FROM {config.source_table}
            ORDER BY {_order_by(config, descending=True)}
            OFFSET 5 LIMIT 1
            """
        ).fetchone()
    if row is None:
        raise RuntimeError(f"The seeded source must contain at least six {config.source_table}")
    return CursorPosition(row[0], tuple(row[1:]))


def _expected_cursors(
    settings: PostgresSettings, config: TableConfig, lower_bound: CursorPosition
) -> list[CursorPosition]:
    """같은 Composite Cursor 조건의 기대 Row를 Source SQL로 정렬해 읽는다."""
    cursor_columns = ", ".join(config.cursor_columns)
    expression = _cursor_expression(config)
    placeholders = ", ".join("%s" for _ in config.cursor_columns)
    with settings.source_connection() as connection:
        rows = connection.execute(
            f"""
            SELECT {cursor_columns}
            FROM {config.source_table}
            WHERE {expression} > ({placeholders})
            ORDER BY {_order_by(config)}
            """,
            (lower_bound.timestamp, *lower_bound.keys),
        ).fetchall()
    return [CursorPosition(row[0], tuple(row[1:])) for row in rows]


def _cursor_expression(config: TableConfig) -> str:
    """Source SQL의 문자열 PK C Collation을 포함한 Composite Cursor 식을 만든다."""
    fields = {field.name: field for field in config.source_schema}
    columns = [config.cursor_timestamp_column]
    for column in config.cursor_key_columns:
        columns.append(
            f'{column} COLLATE "C"' if pa.types.is_string(fields[column].type) else column
        )
    return f"({', '.join(columns)})"


def _order_by(config: TableConfig, *, descending: bool = False) -> str:
    """Composite Cursor 식을 Source SQL ORDER BY 표현으로 바꾼다."""
    suffix = " DESC" if descending else ""
    return ", ".join(f"{column}{suffix}" for column in _cursor_expression(config)[1:-1].split(", "))
