"""결정적 Quarantine Record와 Reject Threshold 계약을 검증한다."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from types import MappingProxyType

import pyarrow.parquet as pq
import pytest

from src.ingestion.extract import SourceRecord
from src.ingestion.quarantine import (
    MAX_REJECT_RATE,
    QuarantineWriteContext,
    QuarantineWriter,
    RejectRateExceededError,
    assert_reject_rate,
    quarantine_record_id,
)
from src.ingestion.tables import ORDERS_TABLE
from src.ingestion.validation import RejectedRecord


def test_quarantine_writer_preserves_deterministic_id_raw_payload_and_error_counts(
    tmp_path,
) -> None:
    """동일 Reject는 같은 ID를 갖고 Parquet에 Raw Payload·오류 집계로 남는다."""
    rejected = RejectedRecord(_record(), 7, ("STATUS_DOMAIN_INVALID", "CURSOR_OUT_OF_RANGE"))
    context = QuarantineWriteContext(
        batch_id="warehouse__20260907T000000Z",
        run_id=uuid.UUID("5f809cf8-92e8-4c29-a843-b771a288e5bb"),
        detected_at=datetime(2026, 9, 7, tzinfo=UTC),
        source_table="orders",
    )
    writer = QuarantineWriter(tmp_path / "records.parquet", context)

    writer.write_rejected_records((rejected,))
    artifact = writer.close()
    row = pq.read_table(artifact.path).to_pylist()[0]

    expected_id = quarantine_record_id(context.table_batch_id, rejected.ordinal)
    assert expected_id == quarantine_record_id(context.table_batch_id, rejected.ordinal)
    assert row["_record_id"] == expected_id
    assert row["_detected_at"] == context.detected_at
    assert row["_error_codes"] == list(rejected.error_codes)
    assert json.loads(row["_raw_payload"])["order_status"] == "invalid"
    assert artifact.error_counts == {"CURSOR_OUT_OF_RANGE": 1, "STATUS_DOMAIN_INVALID": 1}


@pytest.mark.parametrize(("total_rows", "rejected_rows"), [(0, 0), (20, 0), (20, 1), (100, 5)])
def test_reject_rate_allows_up_to_five_percent(total_rows: int, rejected_rows: int) -> None:
    """빈 Batch, 0건, 정확히 5%까지는 Batch를 통과시킨다."""
    assert MAX_REJECT_RATE == 0.05
    assert_reject_rate(total_rows=total_rows, rejected_rows=rejected_rows)


@pytest.mark.parametrize(("total_rows", "rejected_rows"), [(20, 2), (100, 6), (1, 1)])
def test_reject_rate_rejects_more_than_five_percent(total_rows: int, rejected_rows: int) -> None:
    """5%를 넘으면 Watermark를 전진시키지 않을 Batch 오류로 만든다."""
    with pytest.raises(RejectRateExceededError, match="5%"):
        assert_reject_rate(total_rows=total_rows, rejected_rows=rejected_rows)


def _record() -> SourceRecord:
    """Type 오류 Quarantine 계약에 쓸 Raw-compatible 주문 Record를 만든다."""
    timestamp = datetime(2026, 9, 7, tzinfo=UTC)
    values = {
        "order_id": "order-0001",
        "customer_id": "customer-0001",
        "order_status": "invalid",
        "order_purchase_timestamp": timestamp,
        "order_approved_at": None,
        "order_delivered_carrier_date": None,
        "order_delivered_customer_date": None,
        "order_estimated_delivery_date": timestamp + timedelta(days=7),
        "created_at": timestamp,
        "updated_at": timestamp,
    }
    return SourceRecord(ORDERS_TABLE, MappingProxyType(values))
