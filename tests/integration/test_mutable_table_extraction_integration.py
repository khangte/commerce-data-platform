"""Source Table의 설정 기반 고정 범위 Keyset 추출을 검증한다."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from src.common.database import PostgresSettings
from src.generator.config import GENERATOR_VERSION, GeneratorConfig
from src.generator.customers import (
    ensure_membership_tier_records,
    ensure_subscription_records,
    new_customer_record,
    new_membership_tier_record,
    new_subscription_record,
    persist_customer_records,
)
from src.generator.subscription_payments import (
    persist_subscription_payments,
    plan_subscription_payment,
)
from src.ingestion.extract import open_table_snapshot
from src.ingestion.metadata import CursorPosition
from src.ingestion.tables import table_config

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
@pytest.mark.parametrize(
    "source_table",
    (
        "customers",
        "customer_subscriptions",
        "customer_membership_tiers",
        "subscription_payments",
        "products",
        "sellers",
    ),
)
def test_source_tables_use_the_configured_fixed_range_and_complete_primary_key(
    source_table: str,
) -> None:
    """계정·멤버십·기준정보 Table은 설정한 Cursor 범위를 여러 Page로 완전하게 읽는다."""
    settings = PostgresSettings.from_environment()
    config = table_config(source_table)
    fixture = (
        _create_subscription_fixture(settings) if source_table in _SUBSCRIPTION_TABLES else None
    )
    try:
        lower_bound = _lower_bound_before_five_latest_rows(settings, config)
        expected = _expected_cursors(settings, config, lower_bound)

        with open_table_snapshot(settings, config, lower_bound, page_size=2) as snapshot:
            pages = tuple(snapshot.pages())

        actual = [record.cursor for page in pages for record in page.records]
        assert len(expected) == 5
        assert [len(page.records) for page in pages] == [2, 2, 1]
        assert actual == expected
        assert all(
            tuple(record.values) == config.source_column_names
            for page in pages
            for record in page.records
        )
    finally:
        if fixture is not None:
            _cleanup_subscription_fixture(settings, fixture)


def _lower_bound_before_five_latest_rows(settings: PostgresSettings, config) -> CursorPosition:
    """여러 Page를 만들 수 있도록 최신 다섯 Row 바로 전 Cursor를 읽는다."""
    with settings.source_connection() as connection:
        row = connection.execute(
            f"""
            SELECT {", ".join(config.cursor_columns)}
            FROM {config.source_table}
            ORDER BY {_cursor_order(config, descending=True)}
            OFFSET 5 LIMIT 1
            """
        ).fetchone()
    if row is None:
        raise RuntimeError(f"The seeded source must contain at least six {config.source_table}")
    return CursorPosition(row[0], _cursor_keys(config, row[1:]))


def _expected_cursors(
    settings: PostgresSettings, config, lower_bound: CursorPosition
) -> list[CursorPosition]:
    """Source SQL로 같은 Cursor 범위의 기대 Row를 정렬해 읽는다."""
    cursor_columns = ", ".join(config.cursor_columns)
    expression = _cursor_expression(config)
    placeholders = ", ".join("%s" for _ in config.cursor_columns)
    with settings.source_connection() as connection:
        rows = connection.execute(
            f"""
            SELECT {cursor_columns}
            FROM {config.source_table}
            WHERE {expression} > ({placeholders})
            ORDER BY {_cursor_order(config)}
            """,
            (lower_bound.timestamp, *lower_bound.keys),
        ).fetchall()
    return [CursorPosition(row[0], _cursor_keys(config, row[1:])) for row in rows]


def _cursor_order(config, *, descending: bool = False) -> str:
    """설정된 Cursor Key의 Type에 맞는 정렬 방향을 SQL로 구성한다."""
    suffix = " DESC" if descending else ""
    return ", ".join(f"{column}{suffix}" for column in _cursor_expression(config)[1:-1].split(", "))


def _cursor_expression(config) -> str:
    """문자열 PK에만 C Collation을 적용한 Composite Cursor 식을 만든다."""
    columns_by_name = {column.name: column for column in config.source_columns}
    columns = [config.cursor_timestamp_column]
    for column_name in config.cursor_key_columns:
        column = columns_by_name[column_name]
        expression = column_name if column.postgres_uuid else f'{column_name} COLLATE "C"'
        columns.append(expression)
    return f"({', '.join(columns)})"


def _cursor_keys(config, values: tuple[object, ...]) -> tuple[str | int, ...]:
    """UUID Cursor Key를 Metadata와 같은 문자열 표현으로 정규화한다."""
    columns_by_name = {column.name: column for column in config.source_columns}
    normalized: list[str | int] = []
    for column_name, value in zip(config.cursor_key_columns, values, strict=True):
        if columns_by_name[column_name].postgres_uuid:
            value = str(value)
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            raise TypeError("Source cursor keys must be strings or integers")
        normalized.append(value)
    return tuple(normalized)


def _create_subscription_fixture(settings: PostgresSettings) -> tuple[object, ...]:
    """구독·결제 추출 Test가 필요한 6개 독립 계약과 결제를 저장한다."""
    logical_date = datetime(2100, 1, 1, tzinfo=UTC)
    config = GeneratorConfig(
        source_snapshot_id=f"test:mutable-extraction:{uuid.uuid4()}",
        random_seed=20260918,
        logical_date=logical_date,
        order_count=1,
        anomaly_profile="default",
        generator_version=GENERATOR_VERSION,
    )
    customers = tuple(new_customer_record(config, ordinal) for ordinal in range(1, 7))
    subscriptions = tuple(new_subscription_record(customer) for customer in customers)
    tiers = tuple(new_membership_tier_record(customer) for customer in customers)
    payments = tuple(
        plan_subscription_payment(
            GeneratorConfig(
                source_snapshot_id=config.source_snapshot_id,
                random_seed=config.random_seed,
                logical_date=logical_date + timedelta(days=ordinal),
                order_count=config.order_count,
                anomaly_profile=config.anomaly_profile,
                generator_version=config.generator_version,
            ),
            subscription.subscription_id,
            billing_cycle_sequence=1,
            attempt_sequence=1,
            billing_period_start_at=logical_date,
        )
        for ordinal, subscription in enumerate(subscriptions, start=1)
    )
    with settings.source_connection() as connection, connection.transaction():
        assert persist_customer_records(connection, customers).inserted == len(customers)
        assert ensure_subscription_records(connection, subscriptions).inserted == len(subscriptions)
        assert ensure_membership_tier_records(connection, tiers).inserted == len(tiers)
        assert persist_subscription_payments(connection, payments) == len(payments)
    return customers, subscriptions


def _cleanup_subscription_fixture(settings: PostgresSettings, fixture: tuple[object, ...]) -> None:
    """독립 추출 Fixture가 만든 결제·계약·등급·고객을 FK 역순으로 제거한다."""
    customers, subscriptions = fixture
    with settings.source_connection() as connection, connection.transaction():
        for subscription in subscriptions:
            connection.execute(
                "DELETE FROM subscription_payments WHERE subscription_id = %s",
                (subscription.subscription_id,),
            )
            connection.execute(
                "DELETE FROM customer_subscriptions WHERE subscription_id = %s",
                (subscription.subscription_id,),
            )
        for customer in customers:
            connection.execute(
                "DELETE FROM customer_membership_tiers WHERE customer_unique_id = %s",
                (customer.customer_unique_id,),
            )
            connection.execute(
                "DELETE FROM customers WHERE customer_id = %s", (customer.customer_id,)
            )


_SUBSCRIPTION_TABLES = frozenset({"customer_subscriptions", "subscription_payments"})
