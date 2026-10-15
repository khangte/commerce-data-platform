"""Bronze 커밋의 메타데이터 기반 검증 계약을 확인한다."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from src.ingestion.bronze import verification
from src.ingestion.contract.schema import SourceContractError
from src.ingestion.contract.tables import table_config
from src.ingestion.control.batch import BatchIdentity


def test_committed_tables_require_only_metadata(monkeypatch) -> None:
    """객체 저장소 증적 없이 메타데이터의 커밋·스키마 계약을 확인한다."""
    batch = BatchIdentity("warehouse", datetime(2026, 10, 15, tzinfo=UTC))
    tables = ("orders", "customers")
    lookup = Mock(
        side_effect=[
            SimpleNamespace(
                object_key=f"bronze/{table}.parquet",
                row_count=index + 1,
                schema_version=table_config(table).schema_version,
            )
            for index, table in enumerate(tables)
        ]
    )
    monkeypatch.setattr(verification, "get_committed_table_batch", lookup)
    postgres = Mock()
    verified = verification.verify_bronze_commit(postgres, batch, tables)
    assert [(item.source_table, item.object_key, item.row_count) for item in verified] == [
        ("orders", "bronze/orders.parquet", 1),
        ("customers", "bronze/customers.parquet", 2),
    ]
    assert [call.args[1] for call in lookup.call_args_list] == [
        batch.table_batch(table) for table in tables
    ]
    postgres.pipeline_connection.assert_not_called()


@pytest.mark.parametrize("status", ["SUCCESS_NO_DATA", "SUCCESS", "FAILED", None])
def test_missing_commit_requires_latest_no_data_run(monkeypatch, status) -> None:
    """커밋이 없을 때 최신 실행의 데이터 없음 성공만 허용한다."""
    batch = BatchIdentity("warehouse", datetime(2026, 10, 15, tzinfo=UTC))
    monkeypatch.setattr(verification, "get_committed_table_batch", Mock(return_value=None))
    latest = Mock(return_value=status)
    monkeypatch.setattr(verification, "_latest_run_status", latest)
    postgres = Mock()
    if status == "SUCCESS_NO_DATA":
        assert verification.verify_bronze_commit(postgres, batch, ("orders",)) == (
            verification.VerifiedTableBatch("orders", None, 0),
        )
    else:
        with pytest.raises(verification.BronzeCommitVerificationError, match="no COMMITTED"):
            verification.verify_bronze_commit(postgres, batch, ("orders",))
    latest.assert_called_once_with(postgres, batch.batch_id, "orders")


@pytest.mark.parametrize("supported", [True, False])
def test_committed_schema_must_match_contract(monkeypatch, supported) -> None:
    """지원 범위 밖이거나 테이블 계약과 다른 스키마를 거부한다."""
    batch = BatchIdentity("warehouse", datetime(2026, 10, 15, tzinfo=UTC))
    existing = SimpleNamespace(schema_version=table_config("orders").schema_version + 1)
    monkeypatch.setattr(verification, "get_committed_table_batch", Mock(return_value=existing))
    if supported:
        monkeypatch.setattr(verification, "assert_supported_schema_version", Mock())
    error = verification.BronzeCommitVerificationError if supported else SourceContractError
    with pytest.raises(error, match="schema_version"):
        verification.verify_bronze_commit(Mock(), batch, ("orders",))
