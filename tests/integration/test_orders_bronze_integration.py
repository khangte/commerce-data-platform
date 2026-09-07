"""실제 `orders` Snapshot Page의 Local Bronze Parquet 기록을 검증한다."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime

import pyarrow.parquet as pq
import pytest

from src.common.database import PostgresSettings
from src.ingestion.bronze import BronzeWriteContext, OrdersBronzeWriter
from src.ingestion.metadata import CursorPosition
from src.ingestion.orders import open_orders_snapshot

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
def test_orders_snapshot_pages_write_one_local_bronze_file(tmp_path) -> None:
    """실제 Source의 여러 Keyset Page가 하나의 명시적 Schema Parquet으로 기록된다."""
    settings = PostgresSettings.from_environment()
    lower_bound = _lower_bound_before_five_latest_rows(settings)
    output_path = tmp_path / "orders.parquet"
    context = BronzeWriteContext(
        batch_id="warehouse__20260907T000000Z",
        run_id=uuid.uuid4(),
        ingested_at=datetime(2026, 9, 7, tzinfo=UTC),
    )
    writer = OrdersBronzeWriter(output_path, context, row_group_target_rows=2)

    with open_orders_snapshot(settings, lower_bound, page_size=2) as snapshot:
        for page in snapshot.pages():
            writer.write_page(page)
    artifact = writer.close()
    table = pq.read_table(output_path)

    assert artifact.row_count == 5
    assert table.num_rows == 5
    assert table.column("_batch_id").to_pylist() == [context.batch_id] * 5
    assert table.column("_run_id").to_pylist() == [str(context.run_id)] * 5
    assert table.column("_source_table").to_pylist() == ["orders"] * 5


def _lower_bound_before_five_latest_rows(settings: PostgresSettings) -> CursorPosition:
    """여러 Page 기록을 검증할 수 있도록 최신 다섯 Row 직전 Cursor를 반환한다."""
    with settings.source_connection() as connection:
        row = connection.execute(
            """
            SELECT updated_at, order_id
            FROM orders
            ORDER BY updated_at DESC, order_id COLLATE "C" DESC
            OFFSET 5
            LIMIT 1
            """
        ).fetchone()
    if row is None:
        raise RuntimeError("The seeded source must contain at least six orders")
    return CursorPosition(row[0], (row[1],))
