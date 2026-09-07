"""- 결정적 Pipeline Corruption의 원본 보존·오류 Count 분리를 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import MappingProxyType

from src.ingestion.corruption import (
    BROKEN_REFERENCE,
    INVALID_STATUS,
    NEGATIVE_NUMERIC,
    NULL_PRIMARY_KEY,
    TYPE_MISMATCH,
    CorruptionPlan,
)
from src.ingestion.extract import SourcePage, SourceRecord
from src.ingestion.metadata import CursorPosition
from src.ingestion.tables import ORDER_ITEMS_TABLE, ORDERS_TABLE
from src.ingestion.validation import ValidationPipeline


def test_corruption_plan_keeps_source_unchanged_and_separates_input_corrupted_valid_rejected_counts() -> None:
    """- 다섯 종류 복제본 오류가 Source를 바꾸지 않고 각 Error Code로 분리된다."""
    base = datetime(2026, 9, 7, tzinfo=UTC)
    orders = tuple(_order(base + timedelta(seconds=index), index) for index in range(4))
    order_plan = CorruptionPlan(
        {0: NULL_PRIMARY_KEY, 1: INVALID_STATUS, 2: TYPE_MISMATCH}
    )
    corrupted_orders = tuple(order_plan.apply(record, index) for index, record in enumerate(orders))
    page = SourcePage(
        records=corrupted_orders,
        lower_bound=CursorPosition(base - timedelta(seconds=1), ("order-0000",)),
        extract_upper_bound=orders[-1].cursor,
    )
    result = ValidationPipeline(
        ORDERS_TABLE, page.lower_bound, page.extract_upper_bound
    ).validate_page(page)

    assert orders[0].values["order_id"] == "order-0000"
    assert len(orders) == 4
    assert len(corrupted_orders) == 4
    assert len(result.valid_records) == 1
    assert len(result.rejected_records) == 3
    assert [rejected.error_codes for rejected in result.rejected_records] == [
        ("REQUIRED_NULL", "KEY_NULL"),
        ("STATUS_DOMAIN_INVALID",),
        ("TYPE_MISMATCH",),
    ]

    first_item = _item(base + timedelta(seconds=5), 1)
    second_item = _item(base + timedelta(seconds=6), 2)
    item_page = SourcePage(
        records=(
            CorruptionPlan({0: NEGATIVE_NUMERIC}).apply(first_item, 0),
            CorruptionPlan({1: BROKEN_REFERENCE}).apply(second_item, 1),
        ),
        lower_bound=CursorPosition(base, ("order-0000", 0)),
        extract_upper_bound=CursorPosition(base + timedelta(seconds=10), ("order-9999", 99)),
    )
    item_result = ValidationPipeline(
        ORDER_ITEMS_TABLE, item_page.lower_bound, item_page.extract_upper_bound
    ).validate_page(item_page, broken_reference_cursors=frozenset({item_page.records[1].cursor.keys}))

    assert [rejected.error_codes for rejected in item_result.rejected_records] == [
        ("NUMERIC_RANGE_INVALID",),
        ("BROKEN_REFERENCE",),
    ]


def _order(timestamp: datetime, ordinal: int) -> SourceRecord:
    """- Orders Corruption 검증에 쓸 정상 Source Record를 만든다."""
    values = {
        "order_id": f"order-{ordinal:04d}",
        "customer_id": "customer-0001",
        "order_status": "created",
        "order_purchase_timestamp": timestamp,
        "order_approved_at": None,
        "order_delivered_carrier_date": None,
        "order_delivered_customer_date": None,
        "order_estimated_delivery_date": timestamp + timedelta(days=7),
        "created_at": timestamp,
        "updated_at": timestamp,
    }
    return SourceRecord(ORDERS_TABLE, MappingProxyType(values))


def _item(timestamp: datetime, item_id: int) -> SourceRecord:
    """- Child Corruption 검증에 쓸 정상 Item Source Record를 만든다."""
    values = {
        "order_id": "order-0001",
        "order_item_id": item_id,
        "product_id": "product-0001",
        "seller_id": "seller-0001",
        "price": Decimal(10),
        "freight_value": Decimal(1),
        "created_at": timestamp,
    }
    return SourceRecord(ORDER_ITEMS_TABLE, MappingProxyType(values))
