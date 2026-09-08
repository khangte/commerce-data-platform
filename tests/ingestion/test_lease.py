"""수집 Lease Heartbeat의 갱신과 실패 전달을 검증한다."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from src.generator.lease import LeaseOwnershipLostError, SourceMutationLease
from src.ingestion import lease
from src.ingestion.lease import LeaseHeartbeat, TableLease


def test_lease_heartbeat_renews_global_and_table_leases(monkeypatch) -> None:
    """한 Heartbeat Tick은 같은 Fencing Version의 Global·Table Lease를 모두 연장한다."""
    now = datetime(2026, 9, 7, tzinfo=UTC)
    source = SourceMutationLease("WAREHOUSE", uuid.uuid4(), now + timedelta(minutes=30), 3)
    table = TableLease("orders_bronze", "orders", source.owner_id, now + timedelta(minutes=30), 4)
    heartbeat = LeaseHeartbeat(object(), source, table, interval=timedelta(microseconds=1), now=now)
    calls: list[str] = []

    def _renew_source(*_: object, **__: object) -> SourceMutationLease:
        """Global Lease 갱신 호출을 기록하고 기존 Snapshot을 반환한다."""
        calls.append("source")
        return source

    def _renew_table(*_: object, **__: object) -> TableLease:
        """Table Lease 갱신 호출을 기록하고 Loop 종료를 요청한다."""
        calls.append("table")
        heartbeat._stop.set()
        return table

    monkeypatch.setattr(lease, "renew_source_mutation_lease", _renew_source)
    monkeypatch.setattr(lease, "renew_table_lease", _renew_table)

    heartbeat._run()

    heartbeat.assert_healthy()
    assert calls == ["source", "table"]


def test_lease_heartbeat_propagates_a_renewal_failure(monkeypatch) -> None:
    """Background 갱신의 Fencing 실패는 다음 확인 지점에서 수집 실패가 된다."""
    now = datetime(2026, 9, 7, tzinfo=UTC)
    source = SourceMutationLease("WAREHOUSE", uuid.uuid4(), now + timedelta(minutes=30), 3)
    heartbeat = LeaseHeartbeat(object(), source, interval=timedelta(microseconds=1), now=now)

    def _lose_source_lease(*_: object, **__: object) -> SourceMutationLease:
        """실제 Owner 변경에 해당하는 갱신 실패를 재현한다."""
        raise LeaseOwnershipLostError("Lease owner changed")

    monkeypatch.setattr(lease, "renew_source_mutation_lease", _lose_source_lease)

    heartbeat._run()

    with pytest.raises(LeaseOwnershipLostError, match="owner changed"):
        heartbeat.assert_healthy()
