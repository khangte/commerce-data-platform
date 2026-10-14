"""기존 Generator 주문의 결정적 단계별 예정 시각을 계산한다."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from src.generator.core.ids import logical_hash


@dataclass(frozen=True)
class OrderProgressPlan:
    """한 실행에서 적용할 주문 전이와 연결된 결제 전이를 나타낸다."""

    next_status: str
    business_event_time: datetime | None
    payment_status: str | None
    payment_event_time: datetime | None


def _choice(random_seed: int, order_id: str, stage: str) -> int:
    """Seed·주문 ID·단계 이름에서 변하지 않는 선택값을 만든다."""
    return int(logical_hash({"random_seed": random_seed, "order_id": order_id, "stage": stage}), 16)


def _stage_delays(random_seed: int, order_id: str) -> tuple[timedelta, timedelta, timedelta]:
    """Seed 주문의 단계별 중앙 지연에 가까운 승인·출고·배송 지연을 Hash로 고른다."""
    return (
        timedelta(minutes=10 + _choice(random_seed, order_id, "approved-at") % 21),
        timedelta(hours=36 + _choice(random_seed, order_id, "shipped-at") % 25),
        timedelta(days=5, hours=_choice(random_seed, order_id, "delivered-at") % 97),
    )


def planned_order_times(
    random_seed: int,
    order_id: str,
    created_at: datetime,
    *,
    progress_time_scale: int = 1,
    approved_at: datetime | None = None,
    shipped_at: datetime | None = None,
) -> tuple[datetime, datetime, datetime]:
    """배율로 나눈 지연을 기록된 직전 단계 시각(없으면 예정 시각)에 더해 단계별 예정 시각을 만든다."""
    approval_delay, shipping_delay, delivery_delay = _stage_delays(random_seed, order_id)
    planned_approved_at = created_at + approval_delay / progress_time_scale
    planned_shipped_at = (approved_at or planned_approved_at) + shipping_delay / progress_time_scale
    planned_delivered_at = (shipped_at or planned_shipped_at) + delivery_delay / progress_time_scale
    return planned_approved_at, planned_shipped_at, planned_delivered_at


def plan_order_progress(
    random_seed: int,
    order_id: str,
    current_status: str,
    created_at: datetime,
    logical_date: datetime,
    *,
    progress_time_scale: int = 1,
    approved_at: datetime | None = None,
    shipped_at: datetime | None = None,
) -> OrderProgressPlan | None:
    """현재 상태에서 예정 시각이 지난 다음 단계 하나만 반환한다."""
    planned_approved_at, planned_shipped_at, planned_delivered_at = planned_order_times(
        random_seed, order_id, created_at,
        progress_time_scale=progress_time_scale, approved_at=approved_at, shipped_at=shipped_at,
    )
    if current_status == "created" and logical_date >= planned_approved_at:
        if _choice(random_seed, order_id, "created-cancel") % 1000 < 4:
            return OrderProgressPlan("canceled", None, "failed", planned_approved_at)
        return OrderProgressPlan("approved", planned_approved_at, "completed", planned_approved_at)
    if current_status == "approved" and logical_date >= planned_shipped_at:
        if _choice(random_seed, order_id, "approved-cancel") % 1000 < 2:
            return OrderProgressPlan("canceled", None, "refunded", planned_shipped_at)
        return OrderProgressPlan("shipped", planned_shipped_at, None, None)
    if current_status == "shipped" and logical_date >= planned_delivered_at:
        return OrderProgressPlan("delivered", planned_delivered_at, None, None)
    return None
