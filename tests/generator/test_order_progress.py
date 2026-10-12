"""정기 Generator 주문 전이의 결정성과 결제 연동을 검증한다."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.generator.config import GENERATOR_VERSION, GeneratorConfig
from src.generator.order_progress import plan_order_progress, planned_order_times
from src.generator.transitions import OrderState, PaymentState


def test_progress_is_deterministic_and_advances_one_stage_per_run() -> None:
    """같은 입력은 같은 계획을 만들고 오래된 주문도 한 실행에 한 단계만 진행한다."""
    created_at = datetime(2026, 9, 4, tzinfo=UTC)
    logical_date = created_at + timedelta(days=20)
    first = plan_order_progress(0, "order-1", "created", created_at, logical_date)
    assert first == plan_order_progress(0, "order-1", "created", created_at, logical_date)
    assert first is not None
    assert first.next_status in {"approved", "canceled"}
    assert first.business_event_time is None or first.business_event_time <= logical_date
    if first.next_status == "approved":
        second = plan_order_progress(0, "order-1", "approved", created_at, logical_date)
        assert second is not None
        assert second.next_status in {"shipped", "canceled"}


def test_progress_waits_until_approval_is_due() -> None:
    """승인 예정 전에는 주문과 결제를 변경하지 않는다."""
    created_at = datetime(2026, 9, 4, tzinfo=UTC)
    assert plan_order_progress(0, "order-1", "created", created_at, created_at) is None


def test_progress_payment_mapping() -> None:
    """주문 승인과 취소는 허용된 결제 상태 전이에 연결된다."""
    created_at = datetime(2026, 9, 4, tzinfo=UTC)
    logical_date = created_at + timedelta(days=20)
    plans = {
        status: plan_order_progress(0, "order-1", status, created_at, logical_date)
        for status in ("created", "approved", "shipped")
    }
    assert plans["created"].payment_status in {"completed", "failed"}
    assert plans["approved"].payment_status in {None, "refunded"}
    assert plans["shipped"].next_status == "delivered"
    assert plans["shipped"].payment_status is None


def test_cancellation_branches_update_payment_consistently() -> None:
    """승인 전 취소는 실패, 승인 후 취소는 환불 결제로 연결된다."""
    created_at = datetime(2026, 9, 4, tzinfo=UTC)
    logical_date = created_at + timedelta(days=20)
    before_approval = plan_order_progress(0, "order-23", "created", created_at, logical_date)
    after_approval = plan_order_progress(0, "order-484", "approved", created_at, logical_date)
    assert (before_approval.next_status, before_approval.payment_status) == ("canceled", "failed")
    assert (after_approval.next_status, after_approval.payment_status) == ("canceled", "refunded")
    assert before_approval.payment_event_time <= logical_date
    assert after_approval.payment_event_time <= logical_date


def test_source_step_excludes_seed_and_updates_order_with_payment(monkeypatch) -> None:
    """Seed 뒤 생성된 주문 한 건에만 한 단계와 결제 전이를 같은 실행 시각으로 적용한다."""
    from src.generator import service

    created_at = datetime(2026, 9, 4, tzinfo=UTC)
    logical_date = created_at + timedelta(days=20)
    seeded_at = created_at - timedelta(days=1)
    config = GeneratorConfig("seed:test", 0, logical_date, 0, "default", GENERATOR_VERSION)
    settings = MagicMock()
    metadata = settings.pipeline_connection.return_value.__enter__.return_value
    metadata.execute.return_value.fetchone.return_value = (seeded_at,)
    connection = MagicMock()

    def source_result(query, *_):
        """후보 조회에는 Generator 주문만 반환하고 결제는 1건을 반환한다."""
        result = MagicMock()
        result.fetchall.return_value = (
            [("order-1", "created", created_at, None, None)] if "FROM orders" in query else [(1,)]
        )
        return result

    connection.execute.side_effect = source_result
    order = OrderState("order-1", "created", None, None, None, created_at)
    payment = PaymentState("order-1", 1, "pending", created_at, None, None, None, created_at)
    monkeypatch.setattr(service, "fetch_order_state", lambda *_: order)
    monkeypatch.setattr(service, "fetch_payment_state", lambda *_: payment)
    saved_order = MagicMock(return_value=SimpleNamespace(updated=1))
    saved_payment = MagicMock(return_value=SimpleNamespace(updated=1))
    monkeypatch.setattr(service, "persist_order_transition", saved_order)
    monkeypatch.setattr(service, "persist_payment_transition", saved_payment)
    counts = service._empty_result_counts()
    rows = []
    lease = MagicMock()
    lease_check = MagicMock()
    monkeypatch.setattr(service, "assert_source_mutation_lease", lease_check)

    service._run_existing_order_transitions(connection, settings, config, lease, counts, rows)

    candidate_query = connection.execute.call_args_list[0]
    assert "created_at > %s" in candidate_query.args[0]
    assert "ORDER BY order_id" in candidate_query.args[0]
    assert candidate_query.args[1] == (seeded_at,)
    assert saved_order.call_count == 1
    assert saved_payment.call_count == 1
    lease_check.assert_called_once_with(settings, lease)
    assert saved_order.call_args.args[1].mutation_time == logical_date
    assert saved_payment.call_args.args[1].mutation_time == logical_date
    assert counts["orders_updated"] == counts["payments_updated"] == 1
    assert rows[0]["order_status"] in {"approved", "canceled"}


def test_future_candidate_does_not_fetch_row_or_check_lease(monkeypatch) -> None:
    """전이 예정 전 주문은 후보 조회 후 추가 조회와 쓰기를 하지 않는다."""
    from src.generator import service

    logical_date = datetime(2026, 9, 29, 10, tzinfo=UTC)
    seeded_at = logical_date - timedelta(days=30)
    config = GeneratorConfig("seed:test", 0, logical_date, 0, "default", GENERATOR_VERSION)
    settings = MagicMock()
    settings.pipeline_connection.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = (seeded_at,)
    connection = MagicMock()
    connection.execute.return_value.fetchall.return_value = [("new-order", "created", logical_date, None, None)]
    fetched = MagicMock()
    lease_check = MagicMock()
    monkeypatch.setattr(service, "fetch_order_state", fetched)
    monkeypatch.setattr(service, "assert_source_mutation_lease", lease_check)

    service._run_existing_order_transitions(
        connection, settings, config, MagicMock(), service._empty_result_counts(), []
    )

    assert "order_status" in connection.execute.call_args.args[0]
    fetched.assert_not_called()
    lease_check.assert_not_called()


def test_scale_one_matches_previous_schedule() -> None:
    """배율 1은 기존 지연 분포와 같은 예정 시각을 만든다."""
    created_at = datetime(2026, 9, 4, tzinfo=UTC)
    approved_at, shipped_at, delivered_at = planned_order_times(0, "order-1", created_at)
    assert timedelta(minutes=10) <= approved_at - created_at <= timedelta(minutes=30)
    assert timedelta(hours=36) <= shipped_at - approved_at <= timedelta(hours=60)
    assert timedelta(days=5) <= delivered_at - shipped_at <= timedelta(days=9)
    assert all(value.microsecond == 0 and value.second == 0 for value in (approved_at, shipped_at, delivered_at))


def test_source_step_passes_scale_and_recorded_times(monkeypatch) -> None:
    """후보의 기록된 승인·출고 시각과 Config 배율을 진행 계획에 넘긴다."""
    from src.generator import service

    created_at = datetime(2026, 9, 4, tzinfo=UTC)
    approved_at = created_at + timedelta(minutes=1)
    logical_date = created_at + timedelta(days=1)
    config = GeneratorConfig("seed:test", 0, logical_date, 0, "default", GENERATOR_VERSION, 144)
    settings = MagicMock()
    settings.pipeline_connection.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = (
        created_at - timedelta(days=1),
    )
    connection = MagicMock()
    connection.execute.return_value.fetchall.return_value = [("order-1", "approved", created_at, approved_at, None)]
    planner = MagicMock(return_value=None)
    monkeypatch.setattr(service, "plan_order_progress", planner)

    service._run_existing_order_transitions(
        connection, settings, config, MagicMock(), service._empty_result_counts(), []
    )

    candidate_query = connection.execute.call_args_list[0].args[0]
    assert "order_approved_at" in candidate_query
    assert "order_delivered_carrier_date" in candidate_query
    planner.assert_called_once_with(
        0, "order-1", "approved", created_at, logical_date,
        progress_time_scale=144, approved_at=approved_at, shipped_at=None,
    )


def test_scale_divides_each_stage_delay() -> None:
    """배율 144는 같은 Hash 지연을 144로 나눈다."""
    created_at = datetime(2026, 9, 4, tzinfo=UTC)
    base = planned_order_times(0, "order-1", created_at)
    scaled = planned_order_times(0, "order-1", created_at, progress_time_scale=144)
    base_delays = (base[0] - created_at, base[1] - base[0], base[2] - base[1])
    scaled_delays = (scaled[0] - created_at, scaled[1] - scaled[0], scaled[2] - scaled[1])
    assert scaled_delays == tuple(delay / 144 for delay in base_delays)
    assert scaled == planned_order_times(0, "order-1", created_at, progress_time_scale=144)


def test_next_stage_anchors_on_recorded_time() -> None:
    """기록된 직전 단계 시각이 있으면 그 시각에 지연을 더한다."""
    created_at = datetime(2026, 9, 4, tzinfo=UTC)
    recorded_approved_at = created_at + timedelta(hours=5)
    recorded_shipped_at = created_at + timedelta(days=3)
    base = planned_order_times(0, "order-1", created_at)
    anchored = planned_order_times(
        0, "order-1", created_at, approved_at=recorded_approved_at, shipped_at=recorded_shipped_at
    )
    assert anchored[1] == recorded_approved_at + (base[1] - base[0])
    assert anchored[2] == recorded_shipped_at + (base[2] - base[1])


def test_scale_change_keeps_stage_times_increasing() -> None:
    """배율 1로 출고된 주문을 144로 바꿔도 배송 완료는 기록된 출고 이후다."""
    created_at = datetime(2026, 9, 4, tzinfo=UTC)
    _, shipped_at, _ = planned_order_times(0, "order-1", created_at)
    logical_date = shipped_at + timedelta(days=1)
    plan = plan_order_progress(
        0, "order-1", "shipped", created_at, logical_date, progress_time_scale=144, shipped_at=shipped_at
    )
    assert plan is not None
    assert plan.next_status == "delivered"
    assert shipped_at < plan.business_event_time <= logical_date


def test_scale_decrease_waits_full_delay_from_recorded_time() -> None:
    """배율 144로 승인된 주문을 1로 바꾸면 출고는 기록된 승인 + 36시간 이전에 일어나지 않는다."""
    created_at = datetime(2026, 9, 4, tzinfo=UTC)
    approved_at = planned_order_times(0, "order-1", created_at, progress_time_scale=144)[0]
    logical_date = approved_at + timedelta(hours=35)
    assert plan_order_progress(
        0, "order-1", "approved", created_at, logical_date, progress_time_scale=1, approved_at=approved_at
    ) is None


def test_missing_successful_seed_has_clear_error() -> None:
    """성공한 Seed 이력이 사라졌으면 원인을 드러내며 중단한다."""
    from src.generator import service

    logical_date = datetime(2026, 9, 29, 10, tzinfo=UTC)
    config = GeneratorConfig("seed:test", 0, logical_date, 0, "default", GENERATOR_VERSION)
    settings = MagicMock()
    settings.pipeline_connection.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = None
    with pytest.raises(ValueError, match="successful seed run"):
        service._run_existing_order_transitions(
            MagicMock(), settings, config, MagicMock(), service._empty_result_counts(), []
        )
