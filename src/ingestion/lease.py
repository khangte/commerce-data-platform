"""Warehouse Table Lease의 획득·연장·Fencing·해제를 제공한다."""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import Event, Thread
from typing import Self

from src.common.database import PostgresSettings
from src.generator.lease import (
    WAREHOUSE_OWNER_TYPE,
    SourceMutationLease,
    acquire_source_mutation_lease,
    release_source_mutation_lease,
    renew_source_mutation_lease,
)
from src.ingestion.metadata import get_or_create_watermark

DEFAULT_TABLE_LEASE_TTL = timedelta(minutes=30)
DEFAULT_LEASE_RENEWAL_INTERVAL = timedelta(minutes=5)


class TableLeaseUnavailableError(RuntimeError):
    """다른 Warehouse Run이 활성 Table Lease를 보유할 때 발생한다."""


class TableLeaseOwnershipLostError(RuntimeError):
    """Lease가 만료·인수·해제돼 현재 소유권을 잃었을 때 발생한다."""


@dataclass(frozen=True)
class TableLease:
    """특정 Pipeline Table Watermark에 대한 Lease 소유권 Snapshot이다."""

    pipeline_name: str
    source_table: str
    owner_id: uuid.UUID
    lease_expires_at: datetime
    watermark_version: int


class LeaseHeartbeat:
    """실행 중 Global·Table Lease를 주기적으로 갱신하고 실패를 전달한다."""

    def __init__(
        self,
        settings: PostgresSettings,
        source_lease: SourceMutationLease,
        table_lease: TableLease | None = None,
        *,
        interval: timedelta = DEFAULT_LEASE_RENEWAL_INTERVAL,
        now: datetime | None = None,
    ) -> None:
        """양수 갱신 주기와 공유 Lease Snapshot·시각 기준을 준비한다."""
        _assert_positive_ttl(interval)
        self._settings = settings
        self._source_lease = source_lease
        self._table_lease = table_lease
        self._interval = interval
        self._now = now
        self._stop = Event()
        self._failure: Exception | None = None
        self._thread = Thread(target=self._run, daemon=True, name="ingestion-lease-heartbeat")

    def __enter__(self) -> Self:
        """Background Renewal을 시작한다."""
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        """Background Renewal을 멈추고 발견된 Lease 실패를 다시 발생시킨다."""
        self._stop.set()
        self._thread.join(timeout=self._interval.total_seconds() + 1)
        self.assert_healthy()

    def assert_healthy(self) -> None:
        """Background Renewal 실패가 있으면 현재 수집을 실패시킨다."""
        if self._failure is not None:
            raise self._failure

    def _run(self) -> None:
        """정해진 간격마다 Global Lease와 선택 Table Lease를 갱신한다."""
        while not self._stop.wait(self._interval.total_seconds()):
            try:
                current_time = _utc_now(self._now)
                self._source_lease = renew_source_mutation_lease(
                    self._settings, self._source_lease, now=current_time
                )
                if self._table_lease is not None:
                    self._table_lease = renew_table_lease(
                        self._settings, self._table_lease, now=current_time
                    )
            except Exception as error:  # noqa: BLE001
                self._failure = error
                self._stop.set()
                return


@contextmanager
def warehouse_source_freeze(
    settings: PostgresSettings,
    *,
    owner_id: uuid.UUID,
    now: datetime | None = None,
    ttl: timedelta = DEFAULT_TABLE_LEASE_TTL,
):
    """여러 Table 수집이 공유할 Warehouse Global Source Lease를 수명 동안 유지한다."""
    current_time = _utc_now(now)
    lease = acquire_source_mutation_lease(
        settings,
        owner_type=WAREHOUSE_OWNER_TYPE,
        owner_id=owner_id,
        now=current_time,
        ttl=ttl,
    )
    try:
        with LeaseHeartbeat(settings, lease, now=now) as heartbeat:
            yield lease
            heartbeat.assert_healthy()
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
    """비어 있거나 만료된 Table Lease를 Source Read 전에 원자적으로 획득한다."""
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
    """현재 소유자·만료 전제와 Watermark Version을 확인해 Lease를 연장한다."""
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
    return TableLease(lease.pipeline_name, lease.source_table, lease.owner_id, expires_at, row[0])


def assert_table_lease(
    settings: PostgresSettings, lease: TableLease, *, now: datetime | None = None
) -> None:
    """Snapshot·Upload·Commit 직전 Table Lease 소유권을 Fencing으로 확인한다."""
    current_time = _utc_now(now)
    with settings.pipeline_connection() as connection:
        row = connection.execute(
            """
            SELECT lease_owner, lease_expires_at, version
            FROM watermarks WHERE pipeline_name = %s AND source_table = %s
            """,
            (lease.pipeline_name, lease.source_table),
        ).fetchone()
    if (
        row is None
        or row[0] != lease.owner_id
        or row[1] <= current_time
        or row[2] != lease.watermark_version
    ):
        raise TableLeaseOwnershipLostError("Table lease ownership was lost")


def release_table_lease(
    settings: PostgresSettings, lease: TableLease, *, now: datetime | None = None
) -> None:
    """현재 소유자만 Lease를 해제하고 다른 Run의 소유권 변경을 막는다."""
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
    """Lease TTL이 양수인지 확인한다."""
    if ttl <= timedelta(0):
        raise ValueError("Table lease ttl must be greater than zero")


def _utc_now(value: datetime | None) -> datetime:
    """주입된 UTC 시각 또는 현재 UTC 시각을 반환한다."""
    result = value or datetime.now(UTC)
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise ValueError("Table lease time must be normalized to UTC")
    return result
