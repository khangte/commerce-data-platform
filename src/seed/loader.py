"""Transactional, raw-compatible Olist seed loading."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg
from psycopg.types.json import Jsonb

from src.common.database import PostgresSettings, apply_sql_file
from src.seed.contracts import CONTRACT_BY_TABLE, combined_checksum, validate_input_directory

LOAD_ORDER = (
    "customers",
    "customer_memberships",
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
    "customer_memberships": (
        "customer_unique_id",
        "membership_level",
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
        "created_at",
        "updated_at",
    ),
}
PRIMARY_KEYS = {
    **{table_name: contract.primary_key for table_name, contract in CONTRACT_BY_TABLE.items()},
    "customer_memberships": ("customer_unique_id",),
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


def _read_raw_frame(input_dir: Path, table_name: str) -> pd.DataFrame:
    contract = CONTRACT_BY_TABLE[table_name]
    frame = pd.read_csv(
        input_dir / contract.file_name,
        usecols=list(contract.selected_columns),
        dtype=str,
        keep_default_na=False,
    )
    return frame.replace("", None).astype(object).where(lambda values: pd.notna(values), None)


def _timestamps(frame: pd.DataFrame, column: str, *, required: bool) -> list[datetime | None]:
    try:
        parsed = pd.to_datetime(frame[column], utc=True, format="mixed", errors="raise")
    except (TypeError, ValueError) as error:
        raise ValueError(f"Invalid timestamp in {column}") from error
    result = [None if pd.isna(value) else value.to_pydatetime() for value in parsed]
    if required and any(value is None for value in result):
        raise ValueError(f"{column} must not contain an empty value")
    return result


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


def _parse_integer_column(frame: pd.DataFrame, column: str) -> None:
    """Preserve Python ``int`` and ``None`` values instead of pandas float coercion."""
    frame[column] = pd.Series(
        [_optional_integer(value, column) for value in frame[column]],
        index=frame.index,
        dtype=object,
    )


def _assert_primary_keys(frame: pd.DataFrame, table_name: str) -> None:
    primary_key = list(PRIMARY_KEYS[table_name])
    if frame[primary_key].isna().any().any():
        raise ValueError(f"{table_name} contains an empty primary key")
    if frame.duplicated(primary_key).any():
        raise ValueError(f"{table_name} contains a duplicate primary key")


def _assert_references(
    child: pd.DataFrame, child_column: str, parent: pd.DataFrame, parent_column: str, name: str
) -> None:
    missing = set(child[child_column]) - set(parent[parent_column])
    if missing:
        raise ValueError(f"{name} contains a reference absent from its parent table")


def _membership_level(delivered_orders: int) -> str:
    if delivered_orders >= 15:
        return "gold"
    if delivered_orders >= 5:
        return "silver"
    return "bronze"


def _table_rows(frame: pd.DataFrame, columns: tuple[str, ...]) -> list[tuple[Any, ...]]:
    return [
        tuple(_python_value(value) for value in row)
        for row in frame.loc[:, columns].itertuples(index=False, name=None)
    ]


def _python_value(value: object) -> object:
    """Convert pandas scalar values to COPY-compatible Python values."""
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if hasattr(value, "item") and not isinstance(value, (str, bytes, Decimal, datetime)):
        return value.item()
    return value


def build_seed_dataset(input_dir: Path, seeded_at: datetime) -> SeedDataset:
    """Read, type-convert, and validate all raw files before any source mutation."""
    raw_checksum = combined_checksum(validate_input_directory(input_dir))
    customers = _read_raw_frame(input_dir, "customers")
    products = _read_raw_frame(input_dir, "products")
    sellers = _read_raw_frame(input_dir, "sellers")
    orders = _read_raw_frame(input_dir, "orders")
    order_items = _read_raw_frame(input_dir, "order_items")
    order_payments = _read_raw_frame(input_dir, "order_payments")

    for table_name, frame in (
        ("customers", customers),
        ("products", products),
        ("sellers", sellers),
        ("orders", orders),
        ("order_items", order_items),
        ("order_payments", order_payments),
    ):
        _assert_primary_keys(frame, table_name)

    parsed_order_timestamps = {
        column: _timestamps(orders, column, required=column == "order_purchase_timestamp")
        for column in ORDER_TIMESTAMP_COLUMNS
    }
    event_times = [
        value
        for values in parsed_order_timestamps.values()
        for value in values
        if value is not None
    ]
    maximum_event_time = max(event_times)
    if seeded_at < maximum_event_time:
        raise ValueError("--seeded-at must be greater than or equal to every raw event timestamp")

    for column, values in parsed_order_timestamps.items():
        orders[column] = pd.Series(values, index=orders.index, dtype=object)
    orders["created_at"] = orders["order_purchase_timestamp"]
    orders["updated_at"] = seeded_at

    _assert_references(orders, "customer_id", customers, "customer_id", "orders.customer_id")
    _assert_references(order_items, "order_id", orders, "order_id", "order_items.order_id")
    _assert_references(order_items, "product_id", products, "product_id", "order_items.product_id")
    _assert_references(order_items, "seller_id", sellers, "seller_id", "order_items.seller_id")
    _assert_references(order_payments, "order_id", orders, "order_id", "order_payments.order_id")

    customer_first_order = orders.groupby("customer_id")["order_purchase_timestamp"].min()
    customers["created_at"] = customers["customer_id"].map(customer_first_order)
    if customers["created_at"].isna().any():
        raise ValueError("customers contains a record without a linked order")
    customers["updated_at"] = seeded_at

    customer_identity = customers.set_index("customer_id")["customer_unique_id"]
    delivered_counts = (
        orders.loc[orders["order_status"] == "delivered", "customer_id"]
        .map(customer_identity)
        .value_counts()
    )
    customer_memberships = (
        customers.loc[:, ["customer_unique_id", "created_at"]]
        .groupby("customer_unique_id", as_index=False)["created_at"]
        .min()
    )
    customer_memberships["membership_level"] = customer_memberships["customer_unique_id"].map(
        lambda customer_unique_id: _membership_level(
            int(delivered_counts.get(customer_unique_id, 0))
        )
    )
    customer_memberships["updated_at"] = seeded_at

    for column in (
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ):
        _parse_integer_column(products, column)
    products["created_at"] = seeded_at
    products["updated_at"] = seeded_at
    sellers["created_at"] = seeded_at
    sellers["updated_at"] = seeded_at

    order_purchase_timestamp = orders.set_index("order_id")["order_purchase_timestamp"]
    _parse_integer_column(order_items, "order_item_id")
    order_items["price"] = order_items["price"].map(lambda value: _required_decimal(value, "price"))
    order_items["freight_value"] = order_items["freight_value"].map(
        lambda value: _required_decimal(value, "freight_value")
    )
    order_items["created_at"] = order_items["order_id"].map(order_purchase_timestamp)

    _parse_integer_column(order_payments, "payment_sequential")
    _parse_integer_column(order_payments, "payment_installments")
    order_payments["payment_value"] = order_payments["payment_value"].map(
        lambda value: _required_decimal(value, "payment_value")
    )
    order_status = orders.set_index("order_id")["order_status"]
    order_payments["payment_status"] = order_payments["order_id"].map(
        lambda order_id: (
            "failed" if order_status[order_id] in {"canceled", "unavailable"} else "completed"
        )
    )
    order_payments["created_at"] = order_payments["order_id"].map(order_purchase_timestamp)
    order_payments["updated_at"] = seeded_at

    rows = {
        "customers": _table_rows(customers, TARGET_COLUMNS["customers"]),
        "customer_memberships": _table_rows(
            customer_memberships, TARGET_COLUMNS["customer_memberships"]
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
        apply_sql_file(source_connection, "sql/source/001_create_source_tables.sql")
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
