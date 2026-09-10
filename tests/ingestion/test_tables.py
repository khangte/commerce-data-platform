"""9개 Source Table 증분·Bronze Schema 설정 계약을 검증한다."""

from __future__ import annotations

import pyarrow as pa
import pytest

from src.ingestion.bronze import ORDERS_BRONZE_SCHEMA
from src.ingestion.tables import (
    BRONZE_SCHEMA_VERSION,
    ORDER_ITEMS_TABLE,
    ORDERS_TABLE,
    TABLE_CONFIGS,
    TableConfig,
    table_config,
)


def test_table_configs_cover_all_source_tables_with_complete_composite_cursors() -> None:
    """모든 Source Table은 SQL Index와 같은 Timestamp·완전 PK Cursor를 가진다."""
    assert tuple(TABLE_CONFIGS) == (
        "customers",
        "customer_subscriptions",
        "customer_membership_tiers",
        "subscription_payments",
        "products",
        "sellers",
        "orders",
        "order_items",
        "order_payments",
    )
    assert {name: config.cursor_columns for name, config in TABLE_CONFIGS.items()} == {
        "customers": ("created_at", "customer_id"),
        "customer_subscriptions": ("updated_at", "customer_unique_id"),
        "customer_membership_tiers": ("updated_at", "customer_unique_id"),
        "subscription_payments": ("updated_at", "customer_unique_id", "billing_sequence"),
        "products": ("updated_at", "product_id"),
        "sellers": ("updated_at", "seller_id"),
        "orders": ("updated_at", "order_id"),
        "order_items": ("created_at", "order_id", "order_item_id"),
        "order_payments": ("updated_at", "order_id", "payment_sequential"),
    }
    assert ORDER_ITEMS_TABLE.append_only is True
    assert TABLE_CONFIGS["subscription_payments"].append_only is True
    assert all(
        config.primary_key_columns == config.cursor_key_columns for config in TABLE_CONFIGS.values()
    )


def test_table_configs_define_raw_compatible_schemas_and_decimal_money_columns() -> None:
    """Table Schema는 Source 순서·UTC Timestamp와 금액 Decimal 정밀도를 고정한다."""
    assert ORDERS_TABLE.bronze_schema == ORDERS_BRONZE_SCHEMA
    assert (
        TABLE_CONFIGS["customer_subscriptions"]
        .source_schema.field("subscription_status")
        .nullable
        is False
    )
    assert (
        TABLE_CONFIGS["customer_membership_tiers"].source_schema.field("membership_tier").nullable
        is False
    )
    assert TABLE_CONFIGS["subscription_payments"].source_schema.field(
        "payment_value"
    ).type == pa.decimal128(14, 2)
    assert TABLE_CONFIGS["order_items"].source_schema.field("price").type == pa.decimal128(14, 2)
    assert TABLE_CONFIGS["order_payments"].source_schema.field(
        "payment_value"
    ).type == pa.decimal128(14, 2)
    for config in TABLE_CONFIGS.values():
        assert config.bronze_schema.field("_schema_version").type == pa.int32()
        assert config.bronze_schema.field("_schema_version").nullable is False
        assert config.schema_version == BRONZE_SCHEMA_VERSION


def test_table_config_rejects_unknown_table_and_incomplete_primary_key_tie_breaker() -> None:
    """지원하지 않는 Table과 PK 일부만 가진 Cursor 설정은 즉시 차단한다."""
    with pytest.raises(ValueError, match="Unsupported source table"):
        table_config("unknown")
    with pytest.raises(ValueError, match="complete primary key"):
        TableConfig(
            source_table="invalid",
            primary_key_columns=("first_id", "second_id"),
            cursor_timestamp_column="updated_at",
            cursor_key_columns=("first_id",),
            source_columns=(),
        )
