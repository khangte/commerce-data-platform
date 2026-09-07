"""`orders` Table의 고정 Composite Cursor 범위와 Keyset Pagination을 제공한다."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime

import psycopg

from src.common.database import PostgresSettings
from src.ingestion.config import ingestion_page_size
from src.ingestion.metadata import CursorPosition


@dataclass(frozen=True)
class SourceOrderRecord:
    """Source `orders` Row와 증분 Cursor에 필요한 Raw-compatible 값이다."""

    order_id: str
    customer_id: str
    order_status: str
    order_purchase_timestamp: datetime
    order_approved_at: datetime | None
    order_delivered_carrier_date: datetime | None
    order_delivered_customer_date: datetime | None
    order_estimated_delivery_date: datetime | None
    created_at: datetime
    updated_at: datetime

    @property
    def cursor(self) -> CursorPosition:
        """`orders` 증분 정렬에 사용할 `(updated_at, order_id)` Cursor를 반환한다."""
        return CursorPosition(self.updated_at, (self.order_id,))


@dataclass(frozen=True)
class OrdersPage:
    """고정 추출 범위 안에서 읽은 비어 있지 않은 `orders` Keyset Page다."""

    records: tuple[SourceOrderRecord, ...]
    lower_bound: CursorPosition
    extract_upper_bound: CursorPosition

    def __post_init__(self) -> None:
        """Page가 최소 한 Row를 가지며 범위와 마지막 Cursor를 보존하는지 확인한다."""
        if not self.records:
            raise ValueError("Orders pages must contain at least one record")
        _assert_orders_cursor(self.lower_bound)
        _assert_orders_cursor(self.extract_upper_bound)

    @property
    def last_cursor(self) -> CursorPosition:
        """다음 Keyset Page Lower Bound가 될 마지막 Row Cursor를 반환한다."""
        return self.records[-1].cursor


@dataclass(frozen=True)
class OrdersSnapshot:
    """한 Read-only Snapshot에서 고정한 `orders` 추출 상한과 Page Reader다."""

    connection: psycopg.Connection
    watermark_before: CursorPosition
    extract_upper_bound: CursorPosition | None
    page_size: int

    def pages(self) -> Iterator[OrdersPage]:
        """고정 Upper Bound까지 중복·누락 없이 순서대로 Keyset Page를 생성한다."""
        if self.extract_upper_bound is None:
            return
        lower_bound = self.watermark_before
        while True:
            records = _fetch_orders_page(
                self.connection,
                lower_bound=lower_bound,
                extract_upper_bound=self.extract_upper_bound,
                page_size=self.page_size,
            )
            if not records:
                return
            page = OrdersPage(
                records=records,
                lower_bound=lower_bound,
                extract_upper_bound=self.extract_upper_bound,
            )
            yield page
            if len(records) < self.page_size:
                return
            lower_bound = page.last_cursor


@contextmanager
def open_orders_snapshot(
    settings: PostgresSettings,
    watermark_before: CursorPosition,
    *,
    page_size: int | None = None,
) -> Iterator[OrdersSnapshot]:
    """고정 Upper Bound와 모든 Page가 같은 Read-only Snapshot을 사용하도록 연다."""
    _assert_orders_cursor(watermark_before)
    resolved_page_size = page_size if page_size is not None else ingestion_page_size()
    if resolved_page_size <= 0:
        raise ValueError("page_size must be greater than zero")

    with settings.source_connection() as connection, connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        extract_upper_bound = _fetch_orders_upper_bound(connection, watermark_before)
        yield OrdersSnapshot(
            connection=connection,
            watermark_before=watermark_before,
            extract_upper_bound=extract_upper_bound,
            page_size=resolved_page_size,
        )


def _fetch_orders_upper_bound(
    connection: psycopg.Connection, watermark_before: CursorPosition
) -> CursorPosition | None:
    """현재 Snapshot의 Lower Bound 이후 마지막 `orders` Composite Cursor를 고정한다."""
    where_clause, parameters = _lower_bound_clause(watermark_before)
    row = connection.execute(
        f"""
        SELECT updated_at, order_id
        FROM orders
        {where_clause}
        ORDER BY updated_at DESC, order_id COLLATE "C" DESC
        LIMIT 1
        """,
        parameters,
    ).fetchone()
    if row is None:
        return None
    return CursorPosition(row[0], (row[1],))


def _fetch_orders_page(
    connection: psycopg.Connection,
    *,
    lower_bound: CursorPosition,
    extract_upper_bound: CursorPosition,
    page_size: int,
) -> tuple[SourceOrderRecord, ...]:
    """Lower 초과·고정 Upper 이하의 `orders` Row를 한 Page만 읽는다."""
    lower_clause, lower_parameters = _lower_bound_clause(lower_bound)
    rows = connection.execute(
        f"""
        SELECT order_id, customer_id, order_status, order_purchase_timestamp,
               order_approved_at, order_delivered_carrier_date, order_delivered_customer_date,
               order_estimated_delivery_date, created_at, updated_at
        FROM orders
        {lower_clause}
          AND (updated_at, order_id COLLATE "C") <= (%s, %s)
        ORDER BY updated_at, order_id COLLATE "C"
        LIMIT %s
        """,
        (*lower_parameters, extract_upper_bound.timestamp, extract_upper_bound.keys[0], page_size),
    ).fetchall()
    return tuple(SourceOrderRecord(*row) for row in rows)


def _lower_bound_clause(cursor: CursorPosition) -> tuple[str, tuple[datetime | str, ...]]:
    """초기 또는 직전 Page Cursor에 맞는 SQL Lower Bound와 Parameter를 반환한다."""
    if cursor.timestamp is None:
        return "WHERE TRUE", ()
    return (
        'WHERE (updated_at, order_id COLLATE "C") > (%s, %s)',
        (cursor.timestamp, cursor.keys[0]),
    )


def _assert_orders_cursor(cursor: CursorPosition) -> None:
    """`orders` Cursor가 초기 상태 또는 한 개 문자열 PK Key인지 확인한다."""
    if cursor.timestamp is None:
        return
    if len(cursor.keys) != 1 or not isinstance(cursor.keys[0], str):
        raise ValueError("orders cursor must use exactly one string order_id key")
