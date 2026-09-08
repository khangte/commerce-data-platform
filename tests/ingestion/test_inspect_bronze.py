"""Bronze 조회 스크립트의 Object Key·출력 계약을 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from scripts import inspect_bronze


def test_inspect_bronze_reads_manifest_schema_row_count_and_requested_sample(monkeypatch) -> None:
    """스크립트는 Batch Object의 Manifest·Parquet 정보와 지정 행 수를 함께 반환한다."""
    timestamp = datetime(2026, 9, 8, tzinfo=UTC)

    class _Body:
        """S3 Body Byte를 한 번에 반환하는 테스트 대역이다."""

        def __init__(self, payload: bytes) -> None:
            """반환할 Object Byte를 저장한다."""
            self._payload = payload

        def read(self) -> bytes:
            """저장한 Object Byte를 반환한다."""
            return self._payload

    class _Client:
        """Manifest와 Parquet Object 응답을 제공하는 S3 Client 대역이다."""

        def get_object(self, *, Key: str, **_: object) -> dict[str, _Body]:
            """Key별 고정 Manifest 또는 Parquet Body를 반환한다."""
            if Key.endswith("manifest.json"):
                return {"Body": _Body(b'{"row_count": 1, "object_state": "VERIFIED"}')}
            return {"Body": _Body(_parquet_bytes(timestamp))}

    monkeypatch.setattr(inspect_bronze, "seaweedfs_s3_client", lambda _: _Client())

    result = inspect_bronze.inspect_bronze(
        SimpleNamespace(bucket="commerce-lake"),
        source_table="orders",
        batch_id="manual__20260908T000000Z",
        logical_date=timestamp,
        limit=1,
    )

    assert result["manifest"] == {"row_count": 1, "object_state": "VERIFIED"}
    assert result["parquet_row_count"] == 1
    assert len(result["sample_rows"]) == 1
    assert result["object_key"].endswith("/data.parquet")


def test_inspect_bronze_rejects_a_negative_sample_limit() -> None:
    """음수 Sample 행 수는 S3 조회 전에 명확히 거부한다."""
    with pytest.raises(ValueError, match="limit"):
        inspect_bronze.inspect_bronze(
            object(),
            source_table="orders",
            batch_id="manual__20260908T000000Z",
            logical_date=datetime(2026, 9, 8, tzinfo=UTC),
            limit=-1,
        )


def _parquet_bytes(timestamp: datetime) -> bytes:
    """조회 스크립트 테스트용 최소 Orders Bronze Parquet Byte를 만든다."""
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    output = io.BytesIO()
    pq.write_table(pa.table({"order_id": ["order-1"], "updated_at": [timestamp]}), output)
    return output.getvalue()
