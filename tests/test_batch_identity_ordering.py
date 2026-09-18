"""Watermark 비교가 의존하는 Batch ID 정렬 가정을 고정한다."""

from __future__ import annotations

from datetime import UTC, datetime

from src.ingestion.batch import BatchIdentity


def test_batch_id_lexicographic_order_matches_logical_date_order() -> None:
    """같은 dag_id에서 Batch ID의 사전식 순서가 Logical Date 순서와 일치한다."""
    earlier = BatchIdentity(
        dag_id="warehouse_pipeline", logical_date=datetime(2026, 9, 9, 3, 0, tzinfo=UTC)
    )
    later = BatchIdentity(
        dag_id="warehouse_pipeline", logical_date=datetime(2026, 9, 10, 3, 0, tzinfo=UTC)
    )

    assert earlier.batch_id < later.batch_id


def test_batch_id_order_holds_across_a_year_boundary() -> None:
    """연도 경계에서도 사전식 비교가 시간 순서를 뒤집지 않는다."""
    december = BatchIdentity(
        dag_id="warehouse_pipeline", logical_date=datetime(2026, 12, 31, 23, 0, tzinfo=UTC)
    )
    january = BatchIdentity(
        dag_id="warehouse_pipeline", logical_date=datetime(2027, 1, 1, 0, 0, tzinfo=UTC)
    )

    assert december.batch_id < january.batch_id
