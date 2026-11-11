"""Run report의 도메인별 지연 판정 DATA_STALE:<domain>과 연속 보류 수집을 검증한다."""

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import duckdb
import pytest

from src.observability import run_report as module
from src.warehouse.domain_gate import DomainHolds
from src.warehouse.publish import WarehousePaths

NOW = datetime(2026, 1, 10, tzinfo=UTC)
OTHER_PROBES = ("dag", "tasks", "ingestion", "leases", "bronze", "publish", "freshness",
                "row_counts", "files")


@pytest.fixture
def healthy():
    """도메인 판정 외 모든 운영 조건을 만족하는 기록이다."""
    return {
        "publish_status": "PUBLISHED", "serving_export_status": "SUCCESS",
        "metabase_repoint_status": "SUCCESS", "exports_count": 3, "probe_errors": {},
    }


@pytest.mark.parametrize(("lags", "expected"), [
    ({"orders": 10.0, "subscriptions": 7201.0}, ["DATA_STALE:subscriptions"]),
    ({"orders": 7200.0, "subscriptions": None}, []),
    ({"orders": 7201.0, "subscriptions": 7201.0}, ["DATA_STALE:orders", "DATA_STALE:subscriptions"]),
])
def test_domain_stale_judgment(healthy, lags, expected):
    """도메인마다 같은 임계값으로 판정하고 경계값·측정 불가는 제외한다."""
    healthy.update(domain_lag_s=lags, stale_after_s=7200.0)
    assert module.judge_run_report(healthy) == expected


def _published_with_payment(tmp_path, payment_at: str) -> WarehousePaths:
    """주문은 5분 전, 결제는 주어진 시각까지 있는 Published 파일을 만든다."""
    paths = WarehousePaths.under(tmp_path)
    with duckdb.connect(str(paths.published)) as connection:
        connection.execute("CREATE SCHEMA facts")
        connection.execute("CREATE TABLE facts.fct_order (purchase_at TIMESTAMPTZ)")
        connection.execute("INSERT INTO facts.fct_order VALUES ('2026-01-09 23:55:00+00')")
        connection.execute("CREATE TABLE facts.fct_subscription_payment (payment_at TIMESTAMPTZ)")
        connection.execute("INSERT INTO facts.fct_subscription_payment VALUES (?)", [payment_at])
    return paths


def _collect(healthy, paths, monkeypatch, holds: DomainHolds, held: list[str]):
    monkeypatch.setenv("SOURCE_DAG_SCHEDULE", "*/10 * * * *")
    for name in OTHER_PROBES:
        monkeypatch.setattr(module, f"_probe_{name}", Mock(return_value={}))
    lookup = Mock(return_value=holds)
    monkeypatch.setattr(module, "get_domain_holds", lookup)
    publish_run_id = uuid.uuid4()
    summary = {**healthy, "batch_id": "b", "publish_run_id": str(publish_run_id),
               "held_domains": held}
    report = module.collect_run_report("settings", summary, {}, paths, None, now=NOW)
    lookup.assert_called_once_with("settings", publish_run_id)
    return report


def test_held_subscriptions_raise_only_subscription_stale(healthy, tmp_path, monkeypatch):
    """구독만 여러 번 보류돼 마지막 비보류 게시가 오래되면 구독 판정만 뜨고 주문 쪽 판정은 없다."""
    paths = _published_with_payment(tmp_path, "2026-01-09 23:00:00+00")
    holds = DomainHolds({"orders": 0, "subscriptions": 3},
                        {"orders": NOW - timedelta(minutes=5), "subscriptions": NOW - timedelta(hours=1)})
    report = _collect(healthy, paths, monkeypatch, holds, ["subscriptions"])

    assert report["failures"] == ["PUBLISH_PARTIAL:subscriptions", "DATA_STALE:subscriptions"]
    assert report["data_lag_s"] == 300.0
    assert report["domain_lag_s"] == {"orders": 300.0, "subscriptions": 3600.0}
    assert report["domain_refreshed_at"] == {"orders": "2026-01-09T23:55:00+00:00",
                                             "subscriptions": "2026-01-09T23:00:00+00:00"}
    assert report["domain_as_of"] == {"orders": "2026-01-09T23:55:00+00:00",
                                      "subscriptions": "2026-01-09T23:00:00+00:00"}
    assert report["consecutive_holds"] == {"orders": 0, "subscriptions": 3}


def test_old_payment_without_hold_is_not_stale(healthy, tmp_path, monkeypatch):
    """보류가 없으면 결제 사건 시각이 몇 주 전이어도 구독 지연 판정을 내지 않는다."""
    paths = _published_with_payment(tmp_path, "2025-12-10 00:00:00+00")
    recent = NOW - timedelta(minutes=5)
    holds = DomainHolds({"orders": 0, "subscriptions": 0},
                        {"orders": recent, "subscriptions": recent})
    report = _collect(healthy, paths, monkeypatch, holds, [])

    assert report["failures"] == []
    assert report["domain_lag_s"] == {"orders": 300.0, "subscriptions": 300.0}
    assert report["domain_as_of"]["subscriptions"] == "2025-12-10T00:00:00+00:00"


def test_no_clean_publish_is_not_judged(healthy, tmp_path, monkeypatch):
    """사슬에서 비보류 게시를 찾지 못하면 지연은 None이고 판정하지 않는다."""
    paths = _published_with_payment(tmp_path, "2026-01-09 23:00:00+00")
    holds = DomainHolds({"orders": 0, "subscriptions": 1},
                        {"orders": NOW - timedelta(minutes=5), "subscriptions": None})
    report = _collect(healthy, paths, monkeypatch, holds, ["subscriptions"])

    assert report["failures"] == ["PUBLISH_PARTIAL:subscriptions"]
    assert report["domain_lag_s"]["subscriptions"] is None


def test_domains_probe_skips_without_publish_run(monkeypatch):
    """게시 실행이 없으면 DB와 파일을 열지 않는다."""
    holds = Mock()
    monkeypatch.setattr(module, "get_domain_holds", holds)
    assert module._probe_domains(None, {"publish_run_id": None}, {}, None, None, NOW) == {
        "domain_as_of": {}, "domain_refreshed_at": {}, "domain_lag_s": {},
        "consecutive_holds": {},
    }
    holds.assert_not_called()
