"""설정 기반 Source Table Snapshot과 Composite Cursor Keyset 추출을 제공한다."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType

import psycopg
import pyarrow as pa

from src.common.database import PostgresSettings
from src.ingestion.config import ingestion_page_size
from src.ingestion.metadata import CursorPosition
from src.ingestion.tables import TableConfig, table_config


@dataclass(frozen=True)
class SourceRecord:
    """Table Config 순서대로 읽은 Raw-compatible Source Row와 Cursor를 보관한다."""

    config: TableConfig
    values: Mapping[str, object]
    cursor_override: CursorPosition | None = None

    def __post_init__(self) -> None:
        """읽은 값이 Config의 Source Column을 정확히 포함하고 Cursor Override가 맞는지 검증한다."""
        if tuple(self.values) != self.config.source_column_names:
            raise ValueError("Source record columns must match the configured source column order")
        if self.cursor_override is not None:
            _assert_cursor(self.config, self.cursor_override)

    @property
    def cursor(self) -> CursorPosition:
        """Config의 Timestamp와 전체 PK Tie-breaker 또는 보존된 Cursor를 반환한다."""
        if self.cursor_override is not None:
            return self.cursor_override
        timestamp = self.values[self.config.cursor_timestamp_column]
        if not isinstance(timestamp, datetime):
            raise TypeError("Source cursor timestamp must be a datetime")
        return CursorPosition(
            timestamp=timestamp,
            keys=tuple(self.values[column] for column in self.config.cursor_key_columns),
        )


@dataclass(frozen=True)
class SourcePage:
    """고정 범위에서 비어 있지 않게 읽은 한 Table의 Keyset Page다."""

    records: tuple[SourceRecord, ...]
    lower_bound: CursorPosition
    extract_upper_bound: CursorPosition

    def __post_init__(self) -> None:
        """Page가 최소 한 Row를 가지고 동일 Config·범위를 보존하는지 확인한다."""
        if not self.records:
            raise ValueError("Source pages must contain at least one record")
        config = self.records[0].config
        if any(record.config != config for record in self.records):
            raise ValueError("All records in a source page must use the same table config")

    @property
    def last_cursor(self) -> CursorPosition:
        """다음 Keyset Page의 Lower Bound가 될 마지막 Record Cursor를 반환한다."""
        return self.records[-1].cursor


@dataclass(frozen=True)
class TableSnapshot:
    """한 Read-only Snapshot의 Table Config, 고정 Upper Bound, Page Reader다."""

    connection: psycopg.Connection
    config: TableConfig
    watermark_before: CursorPosition
    extract_upper_bound: CursorPosition | None
    page_size: int

    def pages(self) -> Iterator[SourcePage]:
        """고정 Upper Bound까지 중복·누락 없이 Config 기반 Keyset Page를 생성한다."""
        if self.extract_upper_bound is None:
            return
        lower_bound = self.watermark_before
        while True:
            records = _fetch_page(
                self.connection,
                self.config,
                lower_bound=lower_bound,
                extract_upper_bound=self.extract_upper_bound,
                page_size=self.page_size,
            )
            if not records:
                return
            page = SourcePage(
                records=records,
                lower_bound=lower_bound,
                extract_upper_bound=self.extract_upper_bound,
            )
            yield page
            if len(records) < self.page_size:
                return
            lower_bound = page.last_cursor


@contextmanager
def open_table_snapshot(
    settings: PostgresSettings,
    config: TableConfig,
    watermark_before: CursorPosition,
    *,
    page_size: int | None = None,
) -> Iterator[TableSnapshot]:
    """지원 Table의 동일 Read-only Snapshot과 고정 Upper Bound를 연다."""
    _assert_supported_config(config)
    _assert_cursor(config, watermark_before)
    resolved_page_size = page_size if page_size is not None else ingestion_page_size()
    if resolved_page_size <= 0:
        raise ValueError("page_size must be greater than zero")
    with settings.source_connection() as connection, connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        upper_bound = _fetch_upper_bound(connection, config, watermark_before)
        yield TableSnapshot(
            connection=connection,
            config=config,
            watermark_before=watermark_before,
            extract_upper_bound=upper_bound,
            page_size=resolved_page_size,
        )


def _fetch_upper_bound(
    connection: psycopg.Connection, config: TableConfig, watermark_before: CursorPosition
) -> CursorPosition | None:
    """고정 Snapshot에서 Lower Bound 이후의 마지막 Composite Cursor를 읽는다."""
    where_clause, parameters = _lower_bound_clause(config, watermark_before)
    row = connection.execute(
        f"""
        SELECT {config.cursor_timestamp_column}, {", ".join(config.cursor_key_columns)}
        FROM {config.source_table}
        {where_clause}
        ORDER BY {_order_by(config, descending=True)}
        LIMIT 1
        """,
        parameters,
    ).fetchone()
    if row is None:
        return None
    return CursorPosition(row[0], tuple(row[1:]))


def _fetch_page(
    connection: psycopg.Connection,
    config: TableConfig,
    *,
    lower_bound: CursorPosition,
    extract_upper_bound: CursorPosition,
    page_size: int,
) -> tuple[SourceRecord, ...]:
    """Lower 초과·고정 Upper 이하의 Raw-compatible Source Row를 한 Page 읽는다."""
    lower_clause, lower_parameters = _lower_bound_clause(config, lower_bound)
    upper_expression = _cursor_expression(config)
    upper_placeholders = _cursor_placeholders(config)
    rows = connection.execute(
        f"""
        SELECT {", ".join(config.source_column_names)}
        FROM {config.source_table}
        {lower_clause}
          AND {upper_expression} <= {upper_placeholders}
        ORDER BY {_order_by(config)}
        LIMIT %s
        """,
        (*lower_parameters, extract_upper_bound.timestamp, *extract_upper_bound.keys, page_size),
    ).fetchall()
    return tuple(
        SourceRecord(
            config=config, values=MappingProxyType(dict(zip(config.source_column_names, row)))
        )
        for row in rows
    )


def _lower_bound_clause(
    config: TableConfig, cursor: CursorPosition
) -> tuple[str, tuple[object, ...]]:
    """초기 또는 직전 Page Cursor에 맞는 SQL Lower Bound와 Parameter를 만든다."""
    if cursor.timestamp is None:
        return "WHERE TRUE", ()
    return (
        f"WHERE {_cursor_expression(config)} > {_cursor_placeholders(config)}",
        (cursor.timestamp, *cursor.keys),
    )


def _cursor_expression(config: TableConfig) -> str:
    """문자열 PK는 C Collation을 명시한 SQL Composite Cursor 식을 반환한다."""
    expressions = [config.cursor_timestamp_column]
    fields = {field.name: field for field in config.source_schema}
    for column in config.cursor_key_columns:
        expression = column
        if pa.types.is_string(fields[column].type):
            expression = f'{column} COLLATE "C"'
        expressions.append(expression)
    return f"({', '.join(expressions)})"


def _cursor_placeholders(config: TableConfig) -> str:
    """Composite Cursor 길이와 같은 psycopg Placeholder Tuple을 반환한다."""
    return f"({', '.join('%s' for _ in config.cursor_columns)})"


def _order_by(config: TableConfig, *, descending: bool = False) -> str:
    """Cursor 식별 순서의 SQL ORDER BY 항목을 반환한다."""
    suffix = " DESC" if descending else ""
    return ", ".join(
        f"{expression}{suffix}" for expression in _cursor_expression(config)[1:-1].split(", ")
    )


def _assert_supported_config(config: TableConfig) -> None:
    """임의 SQL 식별자를 막기 위해 정적으로 등록된 Config만 허용한다."""
    if table_config(config.source_table) is not config:
        raise ValueError("Table extraction requires a registered table config")


def _assert_cursor(config: TableConfig, cursor: CursorPosition) -> None:
    """초기 Cursor 또는 Config PK 길이·타입과 맞는 Cursor인지 검증한다."""
    if cursor.timestamp is None:
        return
    if len(cursor.keys) != len(config.cursor_key_columns):
        raise ValueError("Cursor key count differs from the table primary key")
