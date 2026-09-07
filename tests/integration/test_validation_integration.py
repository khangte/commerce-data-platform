"""실제 Source Snapshot Page가 검증 Pipeline을 통과하는지 확인한다."""

from __future__ import annotations

import os

import pytest

from src.common.database import PostgresSettings
from src.ingestion.extract import open_table_snapshot
from src.ingestion.metadata import CursorPosition
from src.ingestion.tables import ORDERS_TABLE
from src.ingestion.validation import ValidationPipeline

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
def test_orders_snapshot_pages_pass_source_schema_domain_and_cursor_validation() -> None:
    """정상 Seed `orders`의 여러 Page는 Reject 없이 모두 Valid로 분리된다."""
    settings = PostgresSettings.from_environment()
    lower_bound = _lower_bound_before_five_latest_rows(settings)

    with open_table_snapshot(settings, ORDERS_TABLE, lower_bound, page_size=2) as snapshot:
        assert snapshot.extract_upper_bound is not None
        pipeline = ValidationPipeline(
            ORDERS_TABLE, snapshot.watermark_before, snapshot.extract_upper_bound
        )
        results = [pipeline.validate_page(page) for page in snapshot.pages()]

    assert [len(result.valid_records) for result in results] == [2, 2, 1]
    assert [result.rejected_records for result in results] == [(), (), ()]


def _lower_bound_before_five_latest_rows(settings: PostgresSettings) -> CursorPosition:
    """여러 Page 검증을 위한 최신 다섯 Row 바로 전 주문 Cursor를 읽는다."""
    with settings.source_connection() as connection:
        row = connection.execute(
            """
            SELECT updated_at, order_id FROM orders
            ORDER BY updated_at DESC, order_id COLLATE "C" DESC OFFSET 5 LIMIT 1
            """
        ).fetchone()
    if row is None:
        raise RuntimeError("The seeded source must contain at least six orders")
    return CursorPosition(row[0], (row[1],))
