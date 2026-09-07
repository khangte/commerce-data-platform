"""Source 검증 Pipeline의 Row 오류 분리와 순서 계약을 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import MappingProxyType

import pytest

from src.ingestion.extract import SourcePage, SourceRecord
from src.ingestion.metadata import CursorPosition
from src.ingestion.tables import ORDER_ITEMS_TABLE, ORDERS_TABLE
from src.ingestion.validation import (
    SourceContractError,
    ValidationPipeline,
    _schema_and_type_errors,
)


def test_validation_pipeline_keeps_valid_records_and_collects_domain_numeric_duplicate_errors() -> (
    None
):
    """검증기는 Valid Row를 보존하고 Status·수치·Batch Duplicate 오류를 분리한다."""
    base = datetime(2026, 9, 7, tzinfo=UTC)
    pipeline = ValidationPipeline(
        ORDERS_TABLE,
        CursorPosition(base, ("order-0000",)),
        CursorPosition(base + timedelta(seconds=10), ("order-9999",)),
    )
    page = SourcePage(
        records=(
            _order(base, "order-0001", "created"),
            _order(base + timedelta(seconds=1), "order-0001", "invalid"),
            _order(base + timedelta(seconds=2), "order-0002", "created"),
        ),
        lower_bound=pipeline.watermark_before,
        extract_upper_bound=pipeline.extract_upper_bound,
    )

    result = pipeline.validate_page(page)

    assert [record.values["order_id"] for record in result.valid_records] == [
        "order-0001",
        "order-0002",
    ]
    assert result.rejected_records[0].ordinal == 1
    assert result.rejected_records[0].error_codes == ("BATCH_DUPLICATE", "STATUS_DOMAIN_INVALID")


def test_validation_pipeline_rejects_type_numeric_and_stops_on_cursor_contract_error() -> None:
    """Type 오류는 수치 비교를 건너뛰고, Cursor 위반은 Batch 오류로 중단한다."""
    base = datetime(2026, 9, 7, tzinfo=UTC)
    pipeline = ValidationPipeline(
        ORDER_ITEMS_TABLE,
        CursorPosition(base, ("order-0000", 0)),
        CursorPosition(base + timedelta(seconds=10), ("order-9999", 99)),
    )
    type_error = _item(base + timedelta(seconds=1), "order-0001", "not-an-int", Decimal(1))
    numeric_error = _item(base + timedelta(seconds=2), "order-0002", 2, Decimal(-1))
    cursor_error = _item(base + timedelta(seconds=11), "order-0003", 3, Decimal(1))
    page = SourcePage(
        records=(type_error, numeric_error, cursor_error),
        lower_bound=pipeline.watermark_before,
        extract_upper_bound=pipeline.extract_upper_bound,
    )

    with pytest.raises(SourceContractError, match="fixed extraction range"):
        pipeline.validate_page(page)

    valid_range_page = SourcePage(
        records=(type_error, numeric_error),
        lower_bound=pipeline.watermark_before,
        extract_upper_bound=pipeline.extract_upper_bound,
    )
    result = ValidationPipeline(
        ORDER_ITEMS_TABLE, pipeline.watermark_before, pipeline.extract_upper_bound
    ).validate_page(valid_range_page)
    assert [rejected.error_codes for rejected in result.rejected_records] == [
        ("TYPE_MISMATCH",),
        ("NUMERIC_RANGE_INVALID",),
    ]


def test_schema_validation_rejects_missing_or_extra_source_columns() -> None:
    """허용된 Raw-compatible Source Column 집합과 순서가 다르면 Schema 오류가 된다."""
    values = {column: None for column in ORDERS_TABLE.source_column_names}
    values.pop("updated_at")

    assert _schema_and_type_errors(ORDERS_TABLE, values) == ["SCHEMA_MISMATCH"]


def _order(timestamp: datetime, order_id: str, status: str) -> SourceRecord:
    """주문 검증 계약 테스트에 쓸 Raw-compatible Source Record를 만든다."""
    values = {
        "order_id": order_id,
        "customer_id": "customer-0001",
        "order_status": status,
        "order_purchase_timestamp": timestamp,
        "order_approved_at": None,
        "order_delivered_carrier_date": None,
        "order_delivered_customer_date": None,
        "order_estimated_delivery_date": timestamp + timedelta(days=7),
        "created_at": timestamp,
        "updated_at": timestamp,
    }
    return SourceRecord(ORDERS_TABLE, MappingProxyType(values))


def _item(timestamp: datetime, order_id: str, item_id: object, price: Decimal) -> SourceRecord:
    """Item 검증 계약 테스트에 쓸 Raw-compatible Source Record를 만든다."""
    values = {
        "order_id": order_id,
        "order_item_id": item_id,
        "product_id": "product-0001",
        "seller_id": "seller-0001",
        "price": price,
        "freight_value": Decimal(1),
        "created_at": timestamp,
    }
    return SourceRecord(ORDER_ITEMS_TABLE, MappingProxyType(values))
