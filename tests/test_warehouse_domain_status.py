"""도메인별 기준 시각과 연속 보류 횟수 계산(Run report·Serving 공용)을 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime

import duckdb
import pytest

from src.warehouse.domain_gate import (
    AFFECTED_KEY_DOMAIN,
    DOMAIN_TIME_COLUMN,
    DomainHolds,
    domain_as_of,
    summarize_holds,
)

T1, T2, T3 = (datetime(2026, 11, 12, hour, tzinfo=UTC) for hour in (3, 2, 1))


def test_time_columns_cover_every_registered_domain() -> None:
    """기준 시각 대응이 등록된 도메인(Group)과 정확히 같다."""
    assert DOMAIN_TIME_COLUMN.keys() == AFFECTED_KEY_DOMAIN.keys()


@pytest.mark.parametrize(
    ("histories", "counts", "refreshed"),
    [
        pytest.param([], {"orders": 0, "subscriptions": 0},
                     {"orders": None, "subscriptions": None}, id="no-history"),
        pytest.param([(["subscriptions"], T1)], {"orders": 0, "subscriptions": 1},
                     {"orders": T1, "subscriptions": None}, id="held-once-no-clean-run"),
        pytest.param(
            [(["subscriptions"], T1), (["subscriptions"], T2), ([], T3)],
            {"orders": 0, "subscriptions": 2}, {"orders": T1, "subscriptions": T3},
            id="held-twice",
        ),
        pytest.param(
            [([], T1), (["subscriptions"], T2), (["subscriptions"], T3)],
            {"orders": 0, "subscriptions": 0}, {"orders": T1, "subscriptions": T1},
            id="recovered",
        ),
        pytest.param(
            [(["orders"], T1), (["subscriptions"], T2)], {"orders": 1, "subscriptions": 0},
            {"orders": T2, "subscriptions": T1}, id="alternating",
        ),
    ],
)
def test_summarize_holds(histories, counts, refreshed) -> None:
    """최신 게시부터 보류가 끊기는 곳까지 세고, 끊긴 첫 게시의 완료 시각을 갱신 시각으로 삼는다."""
    assert summarize_holds(histories) == DomainHolds(counts, refreshed)


@pytest.mark.parametrize("column_type", ["TIMESTAMPTZ", "TIMESTAMP"])
def test_domain_as_of_reads_each_time_column(tmp_path, column_type: str) -> None:
    """도메인마다 기준 Column 최댓값을 UTC로 재고, Relation이 없거나 비면 None이다."""
    path = tmp_path / "mart.duckdb"
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE SCHEMA facts")
        connection.execute(f"CREATE TABLE facts.fct_order (purchase_at {column_type})")
        connection.execute(
            "INSERT INTO facts.fct_order VALUES ('2026-01-01 00:00:00+00'), ('2026-01-02 00:00:00+00')"
        )
        assert domain_as_of(connection) == {
            "orders": datetime(2026, 1, 2, tzinfo=UTC), "subscriptions": None,
        }
        connection.execute(f"CREATE TABLE facts.fct_subscription_payment (payment_at {column_type})")
        assert domain_as_of(connection)["subscriptions"] is None


def test_domain_as_of_on_attached_catalog(tmp_path) -> None:
    """ATTACH한 Catalog 이름으로도 같은 값을 잰다."""
    path = tmp_path / "mart.duckdb"
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE SCHEMA facts")
        connection.execute("CREATE TABLE facts.fct_subscription_payment (payment_at TIMESTAMPTZ)")
        connection.execute("INSERT INTO facts.fct_subscription_payment VALUES ('2026-01-03 00:00:00+00')")
    with duckdb.connect() as connection:
        connection.execute(f"ATTACH '{path}' AS other (READ_ONLY)")
        assert domain_as_of(connection, "other")["subscriptions"] == datetime(2026, 1, 3, tzinfo=UTC)
