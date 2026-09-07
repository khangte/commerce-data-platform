"""실제 PostgreSQL Global Source Mutation Lease 계약을 검증한다."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from src.common.database import PostgresSettings
from src.generator.lease import (
    GENERATOR_OWNER_TYPE,
    WAREHOUSE_OWNER_TYPE,
    LeaseOwnershipLostError,
    LeaseUnavailableError,
    acquire_source_mutation_lease,
    assert_source_mutation_lease,
    ensure_source_mutation_lease_metadata,
    release_source_mutation_lease,
    renew_source_mutation_lease,
)

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
def test_source_mutation_lease_is_exclusive_and_fences_a_stale_owner() -> None:
    """Warehouse Lease는 Generator를 막고 만료 인수 뒤 이전 소유자를 Fencing한다."""
    settings = PostgresSettings.from_environment()
    now = datetime(2026, 9, 7, tzinfo=UTC)
    generator_owner_id = uuid.uuid4()
    warehouse_owner_id = uuid.uuid4()

    ensure_source_mutation_lease_metadata(settings)
    generator_lease = acquire_source_mutation_lease(
        settings,
        owner_type=GENERATOR_OWNER_TYPE,
        owner_id=generator_owner_id,
        now=now,
    )
    try:
        with pytest.raises(LeaseUnavailableError):
            acquire_source_mutation_lease(
                settings,
                owner_type=WAREHOUSE_OWNER_TYPE,
                owner_id=warehouse_owner_id,
                now=now,
            )

        renewed_lease = renew_source_mutation_lease(
            settings, generator_lease, now=now + timedelta(minutes=5)
        )
        assert renewed_lease.version == generator_lease.version
        assert_source_mutation_lease(settings, renewed_lease, now=now + timedelta(minutes=10))

        warehouse_lease = acquire_source_mutation_lease(
            settings,
            owner_type=WAREHOUSE_OWNER_TYPE,
            owner_id=warehouse_owner_id,
            now=renewed_lease.lease_expires_at + timedelta(seconds=1),
        )
        try:
            assert warehouse_lease.version > renewed_lease.version
            with pytest.raises(LeaseOwnershipLostError):
                assert_source_mutation_lease(
                    settings, renewed_lease, now=warehouse_lease.lease_expires_at - timedelta(seconds=1)
                )
            with pytest.raises(LeaseOwnershipLostError):
                release_source_mutation_lease(settings, renewed_lease, now=now + timedelta(hours=1))
        finally:
            release_source_mutation_lease(
                settings, warehouse_lease, now=warehouse_lease.lease_expires_at - timedelta(seconds=1)
            )
    except Exception:
        try:
            release_source_mutation_lease(settings, generator_lease, now=now + timedelta(minutes=10))
        except LeaseOwnershipLostError:
            pass
        raise


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
def test_generator_does_not_acquire_the_lease_while_warehouse_holds_it() -> None:
    """Warehouse 활성 Lease 중 Generator 획득 실패는 Source Row 변경 전에 끝난다."""
    settings = PostgresSettings.from_environment()
    now = datetime(2026, 9, 7, 1, tzinfo=UTC)
    warehouse_lease = acquire_source_mutation_lease(
        settings,
        owner_type=WAREHOUSE_OWNER_TYPE,
        owner_id=uuid.uuid4(),
        now=now,
    )
    try:
        with settings.source_connection() as connection:
            before_count = connection.execute("SELECT count(*) FROM orders").fetchone()[0]

        with pytest.raises(LeaseUnavailableError):
            acquire_source_mutation_lease(
                settings,
                owner_type=GENERATOR_OWNER_TYPE,
                owner_id=uuid.uuid4(),
                now=now + timedelta(minutes=1),
            )

        with settings.source_connection() as connection:
            after_count = connection.execute("SELECT count(*) FROM orders").fetchone()[0]
        assert after_count == before_count
    finally:
        release_source_mutation_lease(settings, warehouse_lease, now=now + timedelta(minutes=2))
