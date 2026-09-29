"""Transactional, raw-compatible Olist seed loading."""

from __future__ import annotations

import csv
import hashlib
import json
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from src.common.database import PostgresSettings, apply_sql_file
from src.seed.contracts import CONTRACT_BY_TABLE, combined_checksum, validate_input_directory

LOAD_ORDER = (
    "customers",
    "customer_subscriptions",
    "customer_membership_tiers",
    "products",
    "sellers",
    "orders",
    "order_items",
    "order_payments",
)
ORDER_TIMESTAMP_COLUMNS = (
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
)
ORDER_COLUMNS = (
    "order_id",
    "customer_id",
    "order_status",
    *ORDER_TIMESTAMP_COLUMNS,
    "created_at",
    "updated_at",
)
TARGET_COLUMNS = {
    "customers": (
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state",
        "created_at",
    ),
    "customer_subscriptions": (
        "subscription_id",
        "customer_unique_id",
        "subscription_status",
        "auto_renew_enabled",
        "subscription_started_at",
        "current_period_started_at",
        "current_period_ends_at",
        "billing_due_at",
        "next_payment_attempt_at",
        "payment_failed_at",
        "cancel_requested_at",
        "ended_at",
        "status_changed_at",
        "created_at",
        "updated_at",
    ),
    "customer_membership_tiers": (
        "customer_unique_id",
        "membership_tier",
        "created_at",
        "updated_at",
    ),
    "products": (
        "product_id",
        "product_category_name",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "created_at",
        "updated_at",
    ),
    "sellers": ("seller_id", "seller_city", "seller_state", "created_at", "updated_at"),
    "orders": ORDER_COLUMNS,
    "order_items": (
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
        "created_at",
    ),
    "order_payments": (
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
        "payment_status",
        "payment_initiated_at",
        "payment_completed_at",
        "payment_failed_at",
        "payment_refunded_at",
        "created_at",
        "updated_at",
    ),
}
PRIMARY_KEYS = {
    **{table_name: contract.primary_key for table_name, contract in CONTRACT_BY_TABLE.items()},
    "customer_subscriptions": ("subscription_id",),
    "customer_membership_tiers": ("customer_unique_id",),
}


@dataclass(frozen=True)
class SeedDataset:
    rows: dict[str, list[tuple[Any, ...]]]
    maximum_event_time: datetime
    raw_checksum: str


@dataclass(frozen=True)
class SeedResult:
    seed_run_id: uuid.UUID
    raw_checksum: str
    seeded_at: datetime
    table_row_counts: dict[str, int]
    table_content_hashes: dict[str, str]


def parse_seeded_at(value: str) -> datetime:
    """Parse a required, timezone-aware CLI timestamp into UTC."""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError("--seeded-at must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None:
        raise ValueError("--seeded-at must include a UTC offset, for example 2026-09-03T00:00:00Z")
    return parsed.astimezone(UTC)


Row = dict[str, Any]


def _read_raw_rows(input_dir: Path, table_name: str) -> list[Row]:
    """원본 CSV를 선택 컬럼만 문자열로 읽고 빈 값은 None으로 바꾼다."""
    contract = CONTRACT_BY_TABLE[table_name]
    with (input_dir / contract.file_name).open(newline="", encoding="utf-8") as file:
        return [
            {column: record[column] or None for column in contract.selected_columns}
            for record in csv.DictReader(file)
        ]


def _timestamp(value: str | None, column: str, *, required: bool) -> datetime | None:
    """문자열을 UTC datetime으로 변환한다. Offset이 없으면 UTC로 간주한다."""
    if value is None:
        if required:
            raise ValueError(f"{column} must not contain an empty value")
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"Invalid timestamp in {column}") from error
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _optional_integer(value: object, column: str) -> int | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"Invalid integer in {column}: {value}") from error
    if parsed != parsed.to_integral_value():
        raise ValueError(f"Invalid non-integral value in {column}: {value}")
    return int(parsed)


def _required_decimal(value: object, column: str) -> Decimal:
    if value is None:
        raise ValueError(f"{column} must not contain an empty value")
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"Invalid decimal in {column}: {value}") from error


def _assert_primary_keys(rows: list[Row], table_name: str) -> None:
    """Primary Key의 빈 값과 중복을 거부한다."""
    keys = [tuple(row[column] for column in PRIMARY_KEYS[table_name]) for row in rows]
    if any(value is None for key in keys for value in key):
        raise ValueError(f"{table_name} contains an empty primary key")
    if len(set(keys)) != len(keys):
        raise ValueError(f"{table_name} contains a duplicate primary key")


def _assert_references(
    child: list[Row], child_column: str, parent: list[Row], parent_column: str, name: str
) -> None:
    """자식 행의 참조 값이 모두 부모 테이블에 있는지 검증한다."""
    missing = {row[child_column] for row in child} - {row[parent_column] for row in parent}
    if missing:
        raise ValueError(f"{name} contains a reference absent from its parent table")


def _membership_tier(delivered_orders: int) -> str:
    """완료 주문 수를 거래 실적 멤버십 등급으로 계산한다."""
    if delivered_orders >= 15:
        return "GOLD"
    if delivered_orders >= 5:
        return "SILVER"
    return "BRONZE"


def _table_rows(rows: list[Row], columns: tuple[str, ...]) -> list[tuple[Any, ...]]:
    """행 dict를 COPY 컬럼 순서의 튜플로 변환한다."""
    return [tuple(row[column] for column in columns) for row in rows]


def build_seed_dataset(input_dir: Path, seeded_at: datetime) -> SeedDataset:
    """Read, type-convert, and validate all raw files before any source mutation."""
    raw_checksum = combined_checksum(validate_input_directory(input_dir))
    customers = _read_raw_rows(input_dir, "customers")
    products = _read_raw_rows(input_dir, "products")
    sellers = _read_raw_rows(input_dir, "sellers")
    orders = _read_raw_rows(input_dir, "orders")
    order_items = _read_raw_rows(input_dir, "order_items")
    order_payments = _read_raw_rows(input_dir, "order_payments")

    for table_name, rows in (
        ("customers", customers),
        ("products", products),
        ("sellers", sellers),
        ("orders", orders),
        ("order_items", order_items),
        ("order_payments", order_payments),
    ):
        _assert_primary_keys(rows, table_name)

    for order in orders:
        for column in ORDER_TIMESTAMP_COLUMNS:
            order[column] = _timestamp(
                order[column], column, required=column == "order_purchase_timestamp"
            )
    maximum_event_time = max(
        order[column]
        for order in orders
        for column in ORDER_TIMESTAMP_COLUMNS
        if order[column] is not None
    )
    if seeded_at < maximum_event_time:
        raise ValueError("--seeded-at must be greater than or equal to every raw event timestamp")

    for order in orders:
        order["created_at"] = order["order_purchase_timestamp"]
        order["updated_at"] = seeded_at

    _assert_references(orders, "customer_id", customers, "customer_id", "orders.customer_id")
    _assert_references(order_items, "order_id", orders, "order_id", "order_items.order_id")
    _assert_references(order_items, "product_id", products, "product_id", "order_items.product_id")
    _assert_references(order_items, "seller_id", sellers, "seller_id", "order_items.seller_id")
    _assert_references(order_payments, "order_id", orders, "order_id", "order_payments.order_id")

    customer_first_order: dict[str, datetime] = {}
    for order in orders:
        first = customer_first_order.get(order["customer_id"])
        if first is None or order["created_at"] < first:
            customer_first_order[order["customer_id"]] = order["created_at"]
    for customer in customers:
        customer["created_at"] = customer_first_order.get(customer["customer_id"])
        if customer["created_at"] is None:
            raise ValueError("customers contains a record without a linked order")
        customer["updated_at"] = seeded_at

    customer_identity = {row["customer_id"]: row["customer_unique_id"] for row in customers}
    delivered_counts = Counter(
        customer_identity[order["customer_id"]]
        for order in orders
        if order["order_status"] == "delivered"
    )
    first_created_at: dict[str, datetime] = {}
    for customer in customers:
        unique_id = customer["customer_unique_id"]
        if (
            unique_id not in first_created_at
            or customer["created_at"] < first_created_at[unique_id]
        ):
            first_created_at[unique_id] = customer["created_at"]
    customer_membership_tiers = [
        {
            "customer_unique_id": unique_id,
            "created_at": created_at,
            "membership_tier": _membership_tier(delivered_counts[unique_id]),
            "updated_at": seeded_at,
        }
        for unique_id, created_at in sorted(first_created_at.items())
    ]

    for product in products:
        for column in (
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ):
            product[column] = _optional_integer(product[column], column)
        product["created_at"] = seeded_at
        product["updated_at"] = seeded_at
    for seller in sellers:
        seller["created_at"] = seeded_at
        seller["updated_at"] = seeded_at

    order_by_id = {order["order_id"]: order for order in orders}
    for item in order_items:
        item["order_item_id"] = _optional_integer(item["order_item_id"], "order_item_id")
        item["price"] = _required_decimal(item["price"], "price")
        item["freight_value"] = _required_decimal(item["freight_value"], "freight_value")
        item["shipping_limit_date"] = _timestamp(
            item["shipping_limit_date"], "shipping_limit_date", required=True
        )
        item["created_at"] = order_by_id[item["order_id"]]["order_purchase_timestamp"]

    for payment in order_payments:
        order = order_by_id[payment["order_id"]]
        payment["payment_sequential"] = _optional_integer(
            payment["payment_sequential"], "payment_sequential"
        )
        payment["payment_installments"] = _optional_integer(
            payment["payment_installments"], "payment_installments"
        )
        payment["payment_value"] = _required_decimal(payment["payment_value"], "payment_value")
        payment["payment_status"] = (
            "failed" if order["order_status"] in {"canceled", "unavailable"} else "completed"
        )
        payment["payment_initiated_at"] = None
        payment["payment_completed_at"] = None
        payment["payment_failed_at"] = None
        payment["payment_refunded_at"] = None
        payment["created_at"] = order["order_purchase_timestamp"]
        payment["updated_at"] = seeded_at

    rows = {
        "customers": _table_rows(customers, TARGET_COLUMNS["customers"]),
        "customer_subscriptions": [],
        "customer_membership_tiers": _table_rows(
            customer_membership_tiers, TARGET_COLUMNS["customer_membership_tiers"]
        ),
        "products": _table_rows(products, TARGET_COLUMNS["products"]),
        "sellers": _table_rows(sellers, TARGET_COLUMNS["sellers"]),
        "orders": _table_rows(orders, TARGET_COLUMNS["orders"]),
        "order_items": _table_rows(order_items, TARGET_COLUMNS["order_items"]),
        "order_payments": _table_rows(order_payments, TARGET_COLUMNS["order_payments"]),
    }
    return SeedDataset(rows=rows, maximum_event_time=maximum_event_time, raw_checksum=raw_checksum)


def _upsert_stage(
    connection: psycopg.Connection, table_name: str, rows: list[tuple[Any, ...]]
) -> None:
    columns = TARGET_COLUMNS[table_name]
    stage_name = f"seed_stage_{table_name}"
    column_sql = ", ".join(columns)
    primary_key = PRIMARY_KEYS[table_name]
    update_columns = [column for column in columns if column not in primary_key]
    update_sql = ", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns)
    conflict_sql = ", ".join(primary_key)

    connection.execute(
        f"CREATE TEMPORARY TABLE {stage_name} (LIKE {table_name} INCLUDING DEFAULTS) ON COMMIT DROP"
    )
    with (
        connection.cursor() as cursor,
        cursor.copy(f"COPY {stage_name} ({column_sql}) FROM STDIN") as copy,
    ):
        for row in rows:
            copy.write_row(row)
    if connection.execute(f"SELECT count(*) FROM {stage_name}").fetchone()[0] != len(rows):
        raise RuntimeError(f"Staging row count changed for {table_name}")
    connection.execute(
        f"INSERT INTO {table_name} ({column_sql}) SELECT {column_sql} FROM {stage_name} "
        f"ON CONFLICT ({conflict_sql}) DO UPDATE SET {update_sql}"
    )


def _table_content_hash(connection: psycopg.Connection, table_name: str) -> str:
    columns = TARGET_COLUMNS[table_name]
    order_by = ", ".join(PRIMARY_KEYS[table_name])
    digest = hashlib.sha256()
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT {', '.join(columns)} FROM {table_name} ORDER BY {order_by}")
        for row in cursor:
            digest.update(
                json.dumps(row, default=str, ensure_ascii=True, separators=(",", ":")).encode(
                    "utf-8"
                )
            )
            digest.update(b"\n")
    return digest.hexdigest()


def _ensure_schema(settings: PostgresSettings) -> None:
    with settings.source_connection() as source_connection:
        apply_sql_file(source_connection, "sql/source/003_restructure_subscription_tables.sql")
        apply_sql_file(source_connection, "sql/source/001_create_source_tables.sql")
        apply_sql_file(source_connection, "sql/source/002_reorder_order_payments_columns.sql")
    with settings.pipeline_connection() as pipeline_connection:
        apply_sql_file(pipeline_connection, "sql/metadata/001_create_seed_metadata.sql")


def _assert_seed_guard(
    connection: psycopg.Connection, raw_checksum: str, seeded_at: datetime
) -> None:
    generator_table = connection.execute("SELECT to_regclass('public.generator_runs')").fetchone()[
        0
    ]
    if (
        generator_table
        and connection.execute(
            "SELECT EXISTS (SELECT 1 FROM generator_runs WHERE status = 'SUCCESS')"
        ).fetchone()[0]
    ):
        raise ValueError("Seed is blocked because a successful generator run already exists")

    prior_run = connection.execute(
        "SELECT raw_checksum, seeded_at FROM seed_runs WHERE status = 'SUCCESS' LIMIT 1"
    ).fetchone()
    if prior_run and (prior_run[0] != raw_checksum or prior_run[1] != seeded_at):
        raise ValueError(
            "Seed is blocked because the baseline input differs from the successful seed"
        )


def _record_started_run(
    connection: psycopg.Connection, seed_run_id: uuid.UUID, raw_checksum: str, seeded_at: datetime
) -> None:
    connection.execute(
        """
        INSERT INTO seed_runs (seed_run_id, raw_checksum, seeded_at, started_at, status)
        VALUES (%s, %s, %s, %s, 'RUNNING')
        """,
        (seed_run_id, raw_checksum, seeded_at, datetime.now(UTC)),
    )
    connection.commit()


def _record_finished_run(
    settings: PostgresSettings,
    seed_run_id: uuid.UUID,
    *,
    status: str,
    table_row_counts: dict[str, int] | None = None,
    table_content_hashes: dict[str, str] | None = None,
    error_message: str | None = None,
) -> None:
    with settings.pipeline_connection() as connection:
        connection.execute(
            """
            UPDATE seed_runs
            SET finished_at = %s,
                table_row_counts = %s,
                table_content_hashes = %s,
                status = %s,
                error_message = %s
            WHERE seed_run_id = %s
            """,
            (
                datetime.now(UTC),
                Jsonb(table_row_counts) if table_row_counts is not None else None,
                Jsonb(table_content_hashes) if table_content_hashes is not None else None,
                status,
                error_message,
                seed_run_id,
            ),
        )
        connection.commit()


def run_seed(input_dir: Path, seeded_at: datetime, settings: PostgresSettings) -> SeedResult:
    """Apply one seed run and preserve deterministic evidence in pipeline metadata."""
    dataset = build_seed_dataset(input_dir, seeded_at)
    _ensure_schema(settings)
    seed_run_id = uuid.uuid4()

    with settings.pipeline_connection() as metadata_connection:
        _assert_seed_guard(metadata_connection, dataset.raw_checksum, seeded_at)
        _record_started_run(metadata_connection, seed_run_id, dataset.raw_checksum, seeded_at)

    try:
        with settings.source_connection() as source_connection, source_connection.transaction():
            for table_name in LOAD_ORDER:
                _upsert_stage(source_connection, table_name, dataset.rows[table_name])
            table_row_counts = {
                table_name: source_connection.execute(
                    f"SELECT count(*) FROM {table_name}"
                ).fetchone()[0]
                for table_name in LOAD_ORDER
            }
            table_content_hashes = {
                table_name: _table_content_hash(source_connection, table_name)
                for table_name in LOAD_ORDER
            }
    except Exception as error:
        _record_finished_run(settings, seed_run_id, status="FAILED", error_message=str(error))
        raise

    _record_finished_run(
        settings,
        seed_run_id,
        status="SUCCESS",
        table_row_counts=table_row_counts,
        table_content_hashes=table_content_hashes,
    )
    return SeedResult(
        seed_run_id=seed_run_id,
        raw_checksum=dataset.raw_checksum,
        seeded_at=seeded_at,
        table_row_counts=table_row_counts,
        table_content_hashes=table_content_hashes,
    )
