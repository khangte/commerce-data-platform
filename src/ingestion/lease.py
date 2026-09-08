"""- Warehouse Table Lease의 획득·연장·Fencing·해제를 제공한다."""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from src.common.database import PostgresSettings
from src.generator.lease import (
    WAREHOUSE_OWNER_TYPE,
    acquire_source_mutation_lease,
    release_source_mutation_lease,
)
from src.ingestion.metadata import get_or_create_watermark

DEFAULT_TABLE_LEASE_TTL = timedelta(minutes=30)


class TableLeaseUnavailableError(RuntimeError):
    """- 다른 Warehouse Run이 활성 Table Lease를 보유할 때 발생한다."""


class TableLeaseOwnershipLostError(RuntimeError):
    """- Lease가 만료·인수·해제돼 현재 소유권을 잃었을 때 발생한다."""


@dataclass(frozen=True)
class TableLease:
    """- 특정 Pipeline Table Watermark에 대한 Lease 소유권 Snapshot이다."""

    pipeline_name: str
    source_table: str
    owner_id: uuid.UUID
    lease_expires_at: datetime
    watermark_version: int


@contextmanager
def warehouse_source_freeze(
    settings: PostgresSettings,
    *,
    owner_id: uuid.UUID,
    now: datetime | None = None,
    ttl: timedelta = DEFAULT_TABLE_LEASE_TTL,
):
    """- 여러 Table 수집이 공유할 Warehouse Global Source Lease를 수명 동안 유지한다."""
    current_time = _utc_now(now)
    lease = acquire_source_mutation_lease(
        settings,
        owner_type=WAREHOUSE_OWNER_TYPE,
        owner_id=owner_id,
        now=current_time,
        ttl=ttl,
    )
    try:
        yield lease
    finally:
        release_source_mutation_lease(settings, lease, now=current_time)


def acquire_table_lease(
    settings: PostgresSettings,
    *,
    pipeline_name: str,
    source_table: str,
    owner_id: uuid.UUID,
    now: datetime | None = None,
    ttl: timedelta = DEFAULT_TABLE_LEASE_TTL,
) -> TableLease:
    """- 비어 있거나 만료된 Table Lease를 Source Read 전에 원자적으로 획득한다."""
    current_time = _utc_now(now)
    _assert_positive_ttl(ttl)
    get_or_create_watermark(settings, pipeline_name, source_table, now=current_time)
    expires_at = current_time + ttl
    with settings.pipeline_connection() as connection, connection.transaction():
        row = connection.execute(
            """
            UPDATE watermarks
            SET lease_owner = %s, lease_expires_at = %s
            WHERE pipeline_name = %s
              AND source_table = %s
              AND (lease_owner IS NULL OR lease_expires_at <= %s OR lease_owner = %s)
            RETURNING version
            """,
            (owner_id, expires_at, pipeline_name, source_table, current_time, owner_id),
        ).fetchone()
        if row is None:
            raise TableLeaseUnavailableError(
                f"Table lease is held for {pipeline_name}/{source_table}"
            )
    return TableLease(pipeline_name, source_table, owner_id, expires_at, row[0])


def renew_table_lease(
    settings: PostgresSettings,
    lease: TableLease,
    *,
    now: datetime | None = None,
    ttl: timedelta = DEFAULT_TABLE_LEASE_TTL,
) -> TableLease:
    """- 현재 소유자·만료 전제와 Watermark Version을 확인해 Lease를 연장한다."""
    current_time = _utc_now(now)
    _assert_positive_ttl(ttl)
    expires_at = current_time + ttl
    with settings.pipeline_connection() as connection, connection.transaction():
        row = connection.execute(
            """
            UPDATE watermarks
            SET lease_expires_at = %s
            WHERE pipeline_name = %s
              AND source_table = %s
              AND lease_owner = %s
              AND lease_expires_at > %s
              AND version = %s
            RETURNING version
            """,
            (
                expires_at,
                lease.pipeline_name,
                lease.source_table,
                lease.owner_id,
                current_time,
                lease.watermark_version,
            ),
        ).fetchone()
        if row is None:
            raise TableLeaseOwnershipLostError("Table lease ownership was lost before renewal")
    return TableLease(
        lease.pipeline_name, lease.source_table, lease.owner_id, expires_at, row[0]
    )


def assert_table_lease(
    settings: PostgresSettings, lease: TableLease, *, now: datetime | None = None
) -> None:
    """- Snapshot·Upload·Commit 직전 Table Lease 소유권을 Fencing으로 확인한다."""
    current_time = _utc_now(now)
    with settings.pipeline_connection() as connection:
        row = connection.execute(
            """
            SELECT lease_owner, lease_expires_at, version
            FROM watermarks WHERE pipeline_name = %s AND source_table = %s
            """,
            (lease.pipeline_name, lease.source_table),
        ).fetchone()
    if row is None or row[0] != lease.owner_id or row[1] <= current_time or row[2] != lease.watermark_version:
        raise TableLeaseOwnershipLostError("Table lease ownership was lost")


def release_table_lease(
    settings: PostgresSettings, lease: TableLease, *, now: datetime | None = None
) -> None:
    """- 현재 소유자만 Lease를 해제하고 다른 Run의 소유권 변경을 막는다."""
    _utc_now(now)
    with settings.pipeline_connection() as connection, connection.transaction():
        row = connection.execute(
            """
            UPDATE watermarks
            SET lease_owner = NULL, lease_expires_at = NULL
            WHERE pipeline_name = %s AND source_table = %s AND lease_owner = %s
            RETURNING version
            """,
            (lease.pipeline_name, lease.source_table, lease.owner_id),
        ).fetchone()
        if row is None:
            raise TableLeaseOwnershipLostError("Table lease ownership was lost before release")


def _assert_positive_ttl(ttl: timedelta) -> None:
    """- Lease TTL이 양수인지 확인한다."""
    if ttl <= timedelta(0):
        raise ValueError("Table lease ttl must be greater than zero")


def _utc_now(value: datetime | None) -> datetime:
    """- 주입된 UTC 시각 또는 현재 UTC 시각을 반환한다."""
    result = value or datetime.now(UTC)
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise ValueError("Table lease time must be normalized to UTC")
    return result
