"""Child Page의 Parent Reference를 같은 Source Snapshot에서 검증한다."""

from __future__ import annotations

import os

import pytest

from src.common.database import PostgresSettings
from src.ingestion.extract import open_table_snapshot
from src.ingestion.metadata import CursorPosition
from src.ingestion.references import CHILD_PARENT_REFERENCES, find_broken_parent_references
from src.ingestion.tables import TableConfig, table_config

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
@pytest.mark.parametrize("source_table", ("order_items", "order_payments"))
def test_child_pages_resolve_all_parent_keys_from_the_same_snapshot(source_table: str) -> None:
    """정상 Seed Child Page는 Snapshot 내 Orders·Products·Sellers 참조가 모두 존재한다."""
    settings = PostgresSettings.from_environment()
    config = table_config(source_table)
    lower_bound = _lower_bound_before_five_latest_rows(settings, config)

    with open_table_snapshot(settings, config, lower_bound, page_size=2) as snapshot:
        pages = tuple(snapshot.pages())
        broken = [
            reference
            for page in pages
            for reference in find_broken_parent_references(snapshot, page)
        ]

    assert len(pages) == 3
    assert broken == []
    assert CHILD_PARENT_REFERENCES[source_table]


def _lower_bound_before_five_latest_rows(
    settings: PostgresSettings, config: TableConfig
) -> CursorPosition:
    """여러 Child Page를 만들 수 있는 최신 다섯 Row 직전 Composite Cursor를 읽는다."""
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


def _order_by(config: TableConfig, *, descending: bool = False) -> str:
    """문자열 Key의 C Collation을 보존한 Cursor SQL ORDER BY를 만든다."""
    suffix = " DESC" if descending else ""
    key_columns = [
        f'{column} COLLATE "C"' if column == "order_id" else column
        for column in config.cursor_columns
    ]
    return ", ".join(f"{column}{suffix}" for column in key_columns)
