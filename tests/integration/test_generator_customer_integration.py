"""실제 PostgreSQL Customer 시나리오 저장 계약을 검증한다."""

from __future__ import annotations

import os
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from src.common.database import PostgresSettings
from src.generator.config import GENERATOR_VERSION, GeneratorConfig
from src.generator.customers import (
    membership_change_records,
    new_customer_record,
    persist_customer_records,
)

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
def test_customer_record_is_idempotent_and_membership_changes_monotonically() -> None:
    """신규 Customer 저장은 멱등적이고 Membership 변경은 새 Mutation Time을 사용한다."""
    settings = PostgresSettings.from_environment()
    source_snapshot_id = f"test:{uuid.uuid4()}"
    config = GeneratorConfig(
        source_snapshot_id=source_snapshot_id,
        random_seed=42,
        logical_date=datetime(2026, 9, 4, tzinfo=UTC),
        order_count=1,
        anomaly_profile="default",
        generator_version=GENERATOR_VERSION,
    )
    record = new_customer_record(config, 1)
    next_config = replace(config, logical_date=config.logical_date + timedelta(days=1))

    try:
        with settings.source_connection() as connection, connection.transaction():
            assert persist_customer_records(connection, (record,)).inserted == 1
            assert persist_customer_records(connection, (record,)).skipped == 1

        changed = membership_change_records(next_config, (record,), delivered_order_count=5)
        with settings.source_connection() as connection, connection.transaction():
            assert persist_customer_records(connection, changed).updated == 1
            actual = connection.execute(
                "SELECT membership_level, updated_at FROM customers WHERE customer_id = %s",
                (record.customer_id,),
            ).fetchone()

        assert actual == ("silver", next_config.logical_date)
    finally:
        with settings.source_connection() as connection:
            connection.execute("DELETE FROM customers WHERE customer_id = %s", (record.customer_id,))
            connection.commit()
