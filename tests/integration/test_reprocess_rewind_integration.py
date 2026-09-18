"""Watermark 되감기가 Lease·방향·CAS 조건을 지키는지 검증한다."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from src.common.database import PostgresSettings
from src.ingestion.lease import (
    TableLeaseOwnershipLostError,
    acquire_table_lease,
)
from src.ingestion.metadata import (
    CursorPosition,
    WatermarkRewindError,
    get_or_create_watermark,
    rewind_watermark,
)

pytestmark = pytest.mark.integration

REWIND_SKIP = pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the PostgreSQL container.",
)

BASE_TIME = datetime(2026, 9, 10, 0, 0, tzinfo=UTC)


@REWIND_SKIP
def test_rewind_moves_the_cursor_back_and_bumps_the_version() -> None:
    """Lease 소유자는 Cursor를 과거로 내리고 Version을 올린다."""
    postgres = PostgresSettings.from_environment()
    pipeline_name = f"test_rewind_{uuid.uuid4().hex}"
    _seed_cursor(postgres, pipeline_name, CursorPosition(BASE_TIME, ("customer-9",)))
    lease = acquire_table_lease(
        postgres,
        pipeline_name=pipeline_name,
        source_table="customers",
        owner_id=uuid.uuid4(),
    )
    try:
        rewound = rewind_watermark(
            postgres,
            pipeline_name=pipeline_name,
            source_table="customers",
            owner_id=lease.owner_id,
            expected_version=lease.watermark_version,
            cursor=CursorPosition(BASE_TIME - timedelta(days=1), ("customer-1",)),
        )

        assert rewound.cursor.timestamp == BASE_TIME - timedelta(days=1)
        assert rewound.cursor.keys == ("customer-1",)
        assert rewound.version == lease.watermark_version + 1
    finally:
        _cleanup(postgres, pipeline_name)


@REWIND_SKIP
def test_rewind_rejects_a_forward_target() -> None:
    """현재보다 이후 Cursor로의 이동은 되감기가 아니므로 거부한다."""
    postgres = PostgresSettings.from_environment()
    pipeline_name = f"test_rewind_{uuid.uuid4().hex}"
    _seed_cursor(postgres, pipeline_name, CursorPosition(BASE_TIME, ("customer-9",)))
    lease = acquire_table_lease(
        postgres,
        pipeline_name=pipeline_name,
        source_table="customers",
        owner_id=uuid.uuid4(),
    )
    try:
        with pytest.raises(WatermarkRewindError):
            rewind_watermark(
                postgres,
                pipeline_name=pipeline_name,
                source_table="customers",
                owner_id=lease.owner_id,
                expected_version=lease.watermark_version,
                cursor=CursorPosition(BASE_TIME + timedelta(days=1), ("customer-9",)),
            )
    finally:
        _cleanup(postgres, pipeline_name)


@REWIND_SKIP
def test_rewind_requires_the_table_lease() -> None:
    """Lease를 쥐지 않은 호출자는 되감을 수 없다."""
    postgres = PostgresSettings.from_environment()
    pipeline_name = f"test_rewind_{uuid.uuid4().hex}"
    _seed_cursor(postgres, pipeline_name, CursorPosition(BASE_TIME, ("customer-9",)))
    watermark = get_or_create_watermark(postgres, pipeline_name, "customers")
    try:
        with pytest.raises(TableLeaseOwnershipLostError):
            rewind_watermark(
                postgres,
                pipeline_name=pipeline_name,
                source_table="customers",
                owner_id=uuid.uuid4(),
                expected_version=watermark.version,
                cursor=CursorPosition(BASE_TIME - timedelta(days=1), ("customer-1",)),
            )
    finally:
        _cleanup(postgres, pipeline_name)


def _seed_cursor(
    postgres: PostgresSettings, pipeline_name: str, cursor: CursorPosition
) -> None:
    """되감기 대상 Watermark를 원하는 Cursor로 준비한다."""
    from psycopg.types.json import Jsonb

    get_or_create_watermark(postgres, pipeline_name, "customers")
    with postgres.pipeline_connection() as connection:
        connection.execute(
            """
            UPDATE watermarks
            SET watermark_timestamp = %s, watermark_keys = %s, updated_at = now()
            WHERE pipeline_name = %s AND source_table = %s
            """,
            (cursor.timestamp, Jsonb(cursor.as_json()), pipeline_name, "customers"),
        )
        connection.commit()


def _cleanup(postgres: PostgresSettings, pipeline_name: str) -> None:
    """Test가 만든 Watermark 행만 지운다."""
    with postgres.pipeline_connection() as connection:
        connection.execute("DELETE FROM watermarks WHERE pipeline_name = %s", (pipeline_name,))
        connection.commit()
