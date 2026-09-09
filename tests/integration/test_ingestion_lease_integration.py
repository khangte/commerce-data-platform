"""Warehouse Table Lease와 Global Source Freeze의 PostgreSQL 계약을 검증한다."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from src.common.database import PostgresSettings
from src.ingestion.lease import (
    TableLeaseUnavailableError,
    acquire_table_lease,
    release_table_lease,
    renew_table_lease,
    warehouse_source_freeze,
)
from src.ingestion.metadata import get_or_create_watermark

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
def test_table_lease_blocks_other_owner_then_renews_and_releases() -> None:
    """활성 Lease는 다른 Owner를 차단하고 갱신·해제 후 다음 Owner를 허용한다."""
    settings = PostgresSettings.from_environment()
    now = datetime(2026, 9, 7, tzinfo=UTC)
    pipeline_name = f"test_table_lease_{uuid.uuid4().hex}"
    first_owner = uuid.uuid4()
    second_owner = uuid.uuid4()
    get_or_create_watermark(settings, pipeline_name, "orders", now=now)
    first = acquire_table_lease(
        settings, pipeline_name=pipeline_name, source_table="orders", owner_id=first_owner, now=now
    )
    try:
        with pytest.raises(TableLeaseUnavailableError):
            acquire_table_lease(
                settings,
                pipeline_name=pipeline_name,
                source_table="orders",
                owner_id=second_owner,
                now=now + timedelta(minutes=1),
            )
        renewed = renew_table_lease(settings, first, now=now + timedelta(minutes=2))
        release_table_lease(settings, renewed, now=now + timedelta(minutes=3))
        second = acquire_table_lease(
            settings,
            pipeline_name=pipeline_name,
            source_table="orders",
            owner_id=second_owner,
            now=now + timedelta(minutes=3),
        )
        release_table_lease(settings, second, now=now + timedelta(minutes=4))
    finally:
        with settings.pipeline_connection() as connection:
            connection.execute(
                "DELETE FROM watermarks WHERE pipeline_name = %s AND source_table = 'orders'",
                (pipeline_name,),
            )
            connection.commit()


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
def test_warehouse_source_freeze_releases_global_lease_after_all_table_work() -> None:
    """Warehouse Context 종료 후 같은 Global Source Lease를 새 Owner가 다시 획득할 수 있다."""
    settings = PostgresSettings.from_environment()
    now = datetime(2026, 9, 7, tzinfo=UTC)
    with warehouse_source_freeze(settings, owner_id=uuid.uuid4(), now=now) as lease:
        assert lease.owner_type == "WAREHOUSE"
    with warehouse_source_freeze(
        settings, owner_id=uuid.uuid4(), now=now + timedelta(minutes=1)
    ) as lease:
        assert lease.owner_type == "WAREHOUSE"
