"""Published Warehouse의 Mart 소비 경계가 외부 의존 없이 조회되는지 검증한다."""

from __future__ import annotations

import os

import duckdb
import pytest

from src.warehouse.publish import DEFAULT_WAREHOUSE_ROOT, PUBLISHED_FILE_NAME

MART_SCHEMAS = ("dimensions", "facts", "metrics")

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_PUBLISHED_MART_CHECK") != "1",
    reason=(
        "Run after full publish: RUN_PUBLISHED_MART_CHECK=1 "
        "uv run pytest tests/test_published_mart_queryable.py -q"
    ),
)


def test_every_published_mart_object_is_queryable_without_external_access() -> None:
    """Published Mart는 S3·생성 당시 Catalog 없이도 행 수 조회에 응답해야 한다."""
    published = DEFAULT_WAREHOUSE_ROOT / PUBLISHED_FILE_NAME
    assert published.is_file(), f"Published Warehouse가 없습니다: {published}"

    with duckdb.connect(
        str(published), read_only=True, config={"enable_external_access": "false"}
    ) as connection:
        relations = connection.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema IN ('dimensions', 'facts', 'metrics')
            ORDER BY table_schema, table_name
            """
        ).fetchall()
        assert relations
        assert {schema for schema, _ in relations} == set(MART_SCHEMAS)
        for schema, name in relations:
            connection.execute(f"SELECT count(*) FROM {_relation(schema, name)}").fetchone()


def _relation(schema: str, name: str) -> str:
    """검증 대상 Mart Relation을 DuckDB 식별자로 만든다."""
    return f'"{_quote_identifier(schema)}"."{_quote_identifier(name)}"'


def _quote_identifier(value: str) -> str:
    """식별자 안의 큰따옴표를 DuckDB 규칙으로 이스케이프한다."""
    return value.replace('"', '""')
