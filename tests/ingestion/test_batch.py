"""6개 Table Batch Identity와 재실행 범위 판정 계약을 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from src.ingestion.batch import (
    BatchIdentity,
    BatchIdentityConflictError,
    CommittedTableBatch,
    assert_reusable_table_batch,
)
from src.ingestion.metadata import CursorPosition, Watermark


def test_batch_identity_builds_one_deterministic_batch_and_six_unique_table_batches() -> None:
    """DAG Logical Date는 모든 Table에 공유되며 Table ID는 Source Table로만 구분된다."""
    batch = BatchIdentity("warehouse", datetime(2026, 9, 7, tzinfo=UTC))

    assert batch.batch_id == "warehouse__20260907T000000Z"
    assert {
        batch.table_batch(name).table_batch_id
        for name in (
            "customers",
            "products",
            "sellers",
            "orders",
            "order_items",
            "order_payments",
        )
    } == {
        "warehouse__20260907T000000Z__customers",
        "warehouse__20260907T000000Z__products",
        "warehouse__20260907T000000Z__sellers",
        "warehouse__20260907T000000Z__orders",
        "warehouse__20260907T000000Z__order_items",
        "warehouse__20260907T000000Z__order_payments",
    }


def test_committed_batch_is_reusable_only_when_current_watermark_matches_its_upper_bound() -> None:
    """동일 Batch 재실행은 이미 Commit된 Upper Cursor에서만 Object를 재사용한다."""
    batch = BatchIdentity("warehouse", datetime(2026, 9, 7, tzinfo=UTC))
    before = CursorPosition(datetime(2026, 9, 7, tzinfo=UTC), ("order-1",))
    after = CursorPosition(datetime(2026, 9, 7, 0, 0, 1, tzinfo=UTC), ("order-2",))
    existing = CommittedTableBatch(
        identity=batch.table_batch("orders"),
        object_key="bronze/orders/data.parquet",
        manifest_key="bronze/orders/manifest.json",
        schema_version=1,
        row_count=2,
        watermark_before=before,
        watermark_after=after,
    )

    assert_reusable_table_batch(
        existing,
        current_watermark=Watermark("orders", "orders", after, version=1),
        schema_version=1,
    )
    with pytest.raises(BatchIdentityConflictError, match="range"):
        assert_reusable_table_batch(
            existing,
            current_watermark=Watermark(
                "orders",
                "orders",
                CursorPosition(after.timestamp + timedelta(seconds=1), ("order-3",)),
                2,
            ),
            schema_version=1,
        )
    with pytest.raises(BatchIdentityConflictError, match="schema"):
        assert_reusable_table_batch(
            existing,
            current_watermark=Watermark("orders", "orders", after, version=1),
            schema_version=2,
        )
