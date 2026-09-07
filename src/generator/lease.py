"""Generator와 Warehouse의 Global Source Mutation Lease를 제공한다."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import psycopg

from src.common.database import PostgresSettings, apply_sql_file

RESOURCE_NAME = "commerce_source"
GENERATOR_OWNER_TYPE = "GENERATOR"
WAREHOUSE_OWNER_TYPE = "WAREHOUSE"
DEFAULT_LEASE_TTL = timedelta(minutes=30)


class LeaseUnavailableError(RuntimeError):
    """다른 활성 소유자가 Global Source Mutation Lease를 보유할 때 발생한다."""


class LeaseOwnershipLostError(RuntimeError):
    """Lease 만료·인수·Version 변경으로 현재 소유권을 잃었을 때 발생한다."""


@dataclass(frozen=True)
class SourceMutationLease:
    """Fencing에 사용할 Global Source Mutation Lease의 소유권 Snapshot이다."""

    owner_type: str
    owner_id: uuid.UUID
    lease_expires_at: datetime
    version: int


def ensure_source_mutation_lease_metadata(settings: PostgresSettings) -> None:
    """Global Source Mutation Lease Metadata Table을 멱등적으로 준비한다."""
    with settings.pipeline_connection() as connection:
        apply_sql_file(connection, "sql/metadata/003_create_source_mutation_leases.sql")


def acquire_source_mutation_lease(
    settings: PostgresSettings,
    *,
    owner_type: str,
    owner_id: uuid.UUID,
    now: datetime | None = None,
    ttl: timedelta = DEFAULT_LEASE_TTL,
) -> SourceMutationLease:
    """비어 있거나 만료된 Lease를 CAS Version과 함께 획득한다."""
    _assert_owner_type(owner_type)
    current_time = _utc_now(now)
    _assert_positive_ttl(ttl)
    ensure_source_mutation_lease_metadata(settings)

    with settings.pipeline_connection() as connection, connection.transaction():
        connection.execute(
            """
            INSERT INTO source_mutation_leases (resource_name, version, updated_at)
            VALUES (%s, 0, %s)
            ON CONFLICT (resource_name) DO NOTHING
            """,
            (RESOURCE_NAME, current_time),
        )
        current = _locked_lease(connection)
        if _is_active(current, current_time):
            if current.owner_type == owner_type and current.owner_id == owner_id:
                return current
            raise LeaseUnavailableError(
                f"Source Mutation Lease is held by {current.owner_type}/{current.owner_id} "
                f"until {current.lease_expires_at.isoformat()}"
            )

        next_version = current.version + 1
        expires_at = current_time + ttl
        connection.execute(
            """
            UPDATE source_mutation_leases
            SET owner_type = %s,
                owner_id = %s,
                lease_expires_at = %s,
                version = %s,
                updated_at = %s
            WHERE resource_name = %s AND version = %s
            """,
            (owner_type, owner_id, expires_at, next_version, current_time, RESOURCE_NAME, current.version),
        )
        return SourceMutationLease(
            owner_type=owner_type,
            owner_id=owner_id,
            lease_expires_at=expires_at,
            version=next_version,
        )


def renew_source_mutation_lease(
    settings: PostgresSettings,
    lease: SourceMutationLease,
    *,
    now: datetime | None = None,
    ttl: timedelta = DEFAULT_LEASE_TTL,
) -> SourceMutationLease:
    """현재 소유자와 Version을 확인한 뒤 Lease 만료 시각을 연장한다."""
    current_time = _utc_now(now)
    _assert_positive_ttl(ttl)
    with settings.pipeline_connection() as connection, connection.transaction():
        current = _locked_lease(connection)
        _assert_current_lease(current, lease, current_time)
        expires_at = current_time + ttl
        connection.execute(
            """
            UPDATE source_mutation_leases
            SET lease_expires_at = %s, updated_at = %s
            WHERE resource_name = %s AND version = %s
            """,
            (expires_at, current_time, RESOURCE_NAME, lease.version),
        )
    return SourceMutationLease(
        owner_type=lease.owner_type,
        owner_id=lease.owner_id,
        lease_expires_at=expires_at,
        version=lease.version,
    )


def assert_source_mutation_lease(
    settings: PostgresSettings, lease: SourceMutationLease, *, now: datetime | None = None
) -> None:
    """Source 변경 직전에 Lease 소유권·Version·만료 상태를 Fencing으로 확인한다."""
    current_time = _utc_now(now)
    with settings.pipeline_connection() as connection:
        current = _read_lease(connection)
    _assert_current_lease(current, lease, current_time)


def release_source_mutation_lease(
    settings: PostgresSettings, lease: SourceMutationLease, *, now: datetime | None = None
) -> None:
    """현재 소유자와 Version이 일치할 때만 Global Lease를 해제한다."""
    current_time = _utc_now(now)
    with settings.pipeline_connection() as connection, connection.transaction():
        current = _locked_lease(connection)
        if current.owner_type != lease.owner_type or current.owner_id != lease.owner_id:
            raise LeaseOwnershipLostError("Lease owner changed before release")
        if current.version != lease.version:
            raise LeaseOwnershipLostError("Lease version changed before release")
        connection.execute(
            """
            UPDATE source_mutation_leases
            SET owner_type = NULL,
                owner_id = NULL,
                lease_expires_at = NULL,
                version = %s,
                updated_at = %s
            WHERE resource_name = %s AND version = %s
            """,
            (lease.version + 1, current_time, RESOURCE_NAME, lease.version),
        )


def _locked_lease(connection: psycopg.Connection) -> SourceMutationLease:
    """현재 Lease Row를 Transaction Lock과 함께 읽는다."""
    row = connection.execute(
        """
        SELECT owner_type, owner_id, lease_expires_at, version
        FROM source_mutation_leases
        WHERE resource_name = %s
        FOR UPDATE
        """,
        (RESOURCE_NAME,),
    ).fetchone()
    if row is None:
        raise RuntimeError("Source Mutation Lease row is missing")
    return _lease_from_row(row)


def _read_lease(connection: psycopg.Connection) -> SourceMutationLease:
    """현재 Lease Row를 Fencing 확인용 읽기 전용으로 조회한다."""
    row = connection.execute(
        """
        SELECT owner_type, owner_id, lease_expires_at, version
        FROM source_mutation_leases
        WHERE resource_name = %s
        """,
        (RESOURCE_NAME,),
    ).fetchone()
    if row is None:
        raise RuntimeError("Source Mutation Lease row is missing")
    return _lease_from_row(row)


def _lease_from_row(row: tuple[object, ...]) -> SourceMutationLease:
    """Database Row를 nullable 소유권을 포함한 Lease Snapshot으로 변환한다."""
    if row[0] is None or row[1] is None or row[2] is None:
        return SourceMutationLease(
            owner_type="",
            owner_id=uuid.UUID(int=0),
            lease_expires_at=datetime.min.replace(tzinfo=UTC),
            version=row[3],
        )
    return SourceMutationLease(
        owner_type=row[0],
        owner_id=row[1],
        lease_expires_at=row[2],
        version=row[3],
    )


def _is_active(lease: SourceMutationLease, now: datetime) -> bool:
    """Lease에 유효한 소유자가 있고 아직 만료되지 않았는지 반환한다."""
    return bool(lease.owner_type) and lease.lease_expires_at > now


def _assert_current_lease(
    current: SourceMutationLease, expected: SourceMutationLease, now: datetime
) -> None:
    """현재 Lease가 기대한 소유자·Version이며 아직 유효한지 확인한다."""
    if current.owner_type != expected.owner_type or current.owner_id != expected.owner_id:
        raise LeaseOwnershipLostError("Lease owner changed")
    if current.version != expected.version:
        raise LeaseOwnershipLostError("Lease version changed")
    if not _is_active(current, now):
        raise LeaseOwnershipLostError("Lease expired")


def _assert_owner_type(owner_type: str) -> None:
    """Global Lease를 사용할 수 있는 Source Writer 유형인지 확인한다."""
    if owner_type not in {GENERATOR_OWNER_TYPE, WAREHOUSE_OWNER_TYPE}:
        raise ValueError(f"Unsupported lease owner_type: {owner_type}")


def _assert_positive_ttl(ttl: timedelta) -> None:
    """Lease TTL이 양수인지 확인한다."""
    if ttl <= timedelta(0):
        raise ValueError("Lease ttl must be greater than zero")


def _utc_now(value: datetime | None) -> datetime:
    """주입된 시각 또는 현재 시각을 UTC 기준으로 반환한다."""
    result = value or datetime.now(UTC)
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise ValueError("Lease time must be normalized to UTC")
    return result
