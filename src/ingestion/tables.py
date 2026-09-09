"""7개 Source Table의 Cursor·PK·Raw-compatible Bronze Schema 계약을 정의한다."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from types import MappingProxyType

import pyarrow as pa

from src.ingestion.schema import assert_supported_schema_version

BRONZE_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class SourceColumn:
    """Source Column의 이름, Arrow Type, Null 허용 여부를 나타낸다."""

    name: str
    type: pa.DataType
    nullable: bool = True

    @property
    def field(self) -> pa.Field:
        """명시적 Bronze Schema를 구성할 Arrow Field를 반환한다."""
        return pa.field(self.name, self.type, nullable=self.nullable)


@dataclass(frozen=True)
class TableConfig:
    """하나의 Source Table 증분 추출·Bronze 기록의 불변 계약이다."""

    source_table: str
    primary_key_columns: tuple[str, ...]
    cursor_timestamp_column: str
    cursor_key_columns: tuple[str, ...]
    source_columns: tuple[SourceColumn, ...]
    status_domains: Mapping[str, frozenset[str]] = MappingProxyType({})
    numeric_minimums: Mapping[str, Decimal | int] = MappingProxyType({})
    append_only: bool = False
    schema_version: int = BRONZE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        """PK Tie-breaker, Source Column, Schema Version의 일관성을 검증한다."""
        if not self.source_table.strip():
            raise ValueError("source_table must not be empty")
        if not self.primary_key_columns or not self.cursor_key_columns:
            raise ValueError("primary_key_columns and cursor_key_columns must not be empty")
        if self.primary_key_columns != self.cursor_key_columns:
            raise ValueError("Cursor key columns must contain the complete primary key in order")
        assert_supported_schema_version(self.schema_version)
        names = self.source_column_names
        if len(names) != len(set(names)):
            raise ValueError("Source column names must be unique")
        required_columns = (self.cursor_timestamp_column, *self.primary_key_columns)
        if any(column not in names for column in required_columns):
            raise ValueError("Cursor and primary key columns must exist in source_columns")
        if any(column not in names for column in (*self.status_domains, *self.numeric_minimums)):
            raise ValueError("Validation columns must exist in source_columns")

    @property
    def source_column_names(self) -> tuple[str, ...]:
        """Source SELECT와 Hash에 쓸 Raw-compatible Column 이름 순서를 반환한다."""
        return tuple(column.name for column in self.source_columns)

    @property
    def cursor_columns(self) -> tuple[str, ...]:
        """고정 Upper Bound와 Keyset Query에 쓸 Composite Cursor 순서를 반환한다."""
        return (self.cursor_timestamp_column, *self.cursor_key_columns)

    @property
    def source_schema(self) -> pa.Schema:
        """기술 컬럼을 제외한 Source-compatible 명시적 Arrow Schema를 반환한다."""
        return pa.schema([column.field for column in self.source_columns])

    @property
    def bronze_schema(self) -> pa.Schema:
        """Source Schema 뒤에 모든 Table 공통 Bronze 기술 컬럼을 추가한다."""
        return pa.schema([*self.source_schema, *_technical_fields()])


def table_config(source_table: str) -> TableConfig:
    """지원하는 Source Table 이름의 증분·Schema 계약을 반환한다."""
    try:
        return TABLE_CONFIGS[source_table]
    except KeyError as error:
        raise ValueError(f"Unsupported source table: {source_table}") from error


def _text(name: str, *, nullable: bool = True) -> SourceColumn:
    """UTF-8 문자열 Source Column 정의를 짧게 만든다."""
    return SourceColumn(name, pa.string(), nullable)


def _integer(name: str, *, nullable: bool = True) -> SourceColumn:
    """32-bit 정수 Source Column 정의를 짧게 만든다."""
    return SourceColumn(name, pa.int32(), nullable)


def _timestamp(name: str, *, nullable: bool = True) -> SourceColumn:
    """UTC microsecond Timestamp Source Column 정의를 짧게 만든다."""
    return SourceColumn(name, pa.timestamp("us", tz="UTC"), nullable)


def _decimal(name: str, *, nullable: bool = True) -> SourceColumn:
    """금액용 고정 `decimal128(14,2)` Source Column 정의를 짧게 만든다."""
    return SourceColumn(name, pa.decimal128(14, 2), nullable)


def _technical_fields() -> tuple[pa.Field, ...]:
    """모든 Bronze Table에 공통으로 붙는 기술 Column Arrow Field를 반환한다."""
    return (
        pa.field("_batch_id", pa.string(), nullable=False),
        pa.field("_run_id", pa.string(), nullable=False),
        pa.field("_ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("_source_table", pa.string(), nullable=False),
        pa.field("_schema_version", pa.int32(), nullable=False),
    )


CUSTOMERS_TABLE = TableConfig(
    source_table="customers",
    primary_key_columns=("customer_id",),
    cursor_timestamp_column="created_at",
    cursor_key_columns=("customer_id",),
    source_columns=(
        _text("customer_id", nullable=False),
        _text("customer_unique_id", nullable=False),
        _text("customer_city"),
        _text("customer_state"),
        _timestamp("created_at", nullable=False),
    ),
)

CUSTOMER_MEMBERSHIPS_TABLE = TableConfig(
    source_table="customer_memberships",
    primary_key_columns=("customer_unique_id",),
    cursor_timestamp_column="updated_at",
    cursor_key_columns=("customer_unique_id",),
    source_columns=(
        _text("customer_unique_id", nullable=False),
        _text("membership_level", nullable=False),
        _timestamp("created_at", nullable=False),
        _timestamp("updated_at", nullable=False),
    ),
    status_domains={"membership_level": frozenset({"bronze", "silver", "gold"})},
)

PRODUCTS_TABLE = TableConfig(
    source_table="products",
    primary_key_columns=("product_id",),
    cursor_timestamp_column="updated_at",
    cursor_key_columns=("product_id",),
    source_columns=(
        _text("product_id", nullable=False),
        _text("product_category_name"),
        _integer("product_weight_g"),
        _integer("product_length_cm"),
        _integer("product_height_cm"),
        _integer("product_width_cm"),
        _timestamp("created_at", nullable=False),
        _timestamp("updated_at", nullable=False),
    ),
)

SELLERS_TABLE = TableConfig(
    source_table="sellers",
    primary_key_columns=("seller_id",),
    cursor_timestamp_column="updated_at",
    cursor_key_columns=("seller_id",),
    source_columns=(
        _text("seller_id", nullable=False),
        _text("seller_city"),
        _text("seller_state"),
        _timestamp("created_at", nullable=False),
        _timestamp("updated_at", nullable=False),
    ),
)

ORDERS_TABLE = TableConfig(
    source_table="orders",
    primary_key_columns=("order_id",),
    cursor_timestamp_column="updated_at",
    cursor_key_columns=("order_id",),
    source_columns=(
        _text("order_id", nullable=False),
        _text("customer_id", nullable=False),
        _text("order_status", nullable=False),
        _timestamp("order_purchase_timestamp", nullable=False),
        _timestamp("order_approved_at"),
        _timestamp("order_delivered_carrier_date"),
        _timestamp("order_delivered_customer_date"),
        _timestamp("order_estimated_delivery_date"),
        _timestamp("created_at", nullable=False),
        _timestamp("updated_at", nullable=False),
    ),
    status_domains={
        "order_status": frozenset(
            {
                "created",
                "approved",
                "processing",
                "invoiced",
                "shipped",
                "delivered",
                "canceled",
                "unavailable",
            }
        )
    },
)

ORDER_ITEMS_TABLE = TableConfig(
    source_table="order_items",
    primary_key_columns=("order_id", "order_item_id"),
    cursor_timestamp_column="created_at",
    cursor_key_columns=("order_id", "order_item_id"),
    source_columns=(
        _text("order_id", nullable=False),
        _integer("order_item_id", nullable=False),
        _text("product_id", nullable=False),
        _text("seller_id", nullable=False),
        _decimal("price", nullable=False),
        _decimal("freight_value", nullable=False),
        _timestamp("created_at", nullable=False),
    ),
    numeric_minimums={"order_item_id": 1, "price": Decimal(0), "freight_value": Decimal(0)},
    append_only=True,
)

ORDER_PAYMENTS_TABLE = TableConfig(
    source_table="order_payments",
    primary_key_columns=("order_id", "payment_sequential"),
    cursor_timestamp_column="updated_at",
    cursor_key_columns=("order_id", "payment_sequential"),
    source_columns=(
        _text("order_id", nullable=False),
        _integer("payment_sequential", nullable=False),
        _text("payment_type", nullable=False),
        _integer("payment_installments"),
        _decimal("payment_value", nullable=False),
        _text("payment_status", nullable=False),
        _timestamp("created_at", nullable=False),
        _timestamp("updated_at", nullable=False),
    ),
    status_domains={"payment_status": frozenset({"pending", "completed", "failed", "refunded"})},
    numeric_minimums={
        "payment_sequential": 1,
        "payment_installments": 0,
        "payment_value": Decimal(0),
    },
)

TABLE_CONFIGS: Mapping[str, TableConfig] = MappingProxyType(
    {
        table.source_table: table
        for table in (
            CUSTOMERS_TABLE,
            CUSTOMER_MEMBERSHIPS_TABLE,
            PRODUCTS_TABLE,
            SELLERS_TABLE,
            ORDERS_TABLE,
            ORDER_ITEMS_TABLE,
            ORDER_PAYMENTS_TABLE,
        )
    }
)
