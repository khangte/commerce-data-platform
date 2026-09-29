"""기존 Generator 주문의 결정적 단계별 예정 시각을 계산한다."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from src.generator.ids import logical_hash


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


def planned_order_times(random_seed: int, order_id: str, created_at: datetime) -> tuple[datetime, datetime, datetime]:
    """Seed 주문의 단계별 중앙 지연에 가까운 승인·출고·배송 예정 시각을 만든다."""
    approved_at = created_at + timedelta(minutes=10 + _choice(random_seed, order_id, "approved-at") % 21)
    shipped_at = approved_at + timedelta(hours=36 + _choice(random_seed, order_id, "shipped-at") % 25)
    delivered_at = shipped_at + timedelta(days=5, hours=_choice(random_seed, order_id, "delivered-at") % 97)
    return approved_at, shipped_at, delivered_at


def plan_order_progress(
    random_seed: int,
    order_id: str,
    current_status: str,
    created_at: datetime,
    logical_date: datetime,
) -> OrderProgressPlan | None:
    """현재 상태에서 예정 시각이 지난 다음 단계 하나만 반환한다."""
    approved_at, shipped_at, delivered_at = planned_order_times(random_seed, order_id, created_at)
    if current_status == "created" and logical_date >= approved_at:
        if _choice(random_seed, order_id, "created-cancel") % 1000 < 4:
            return OrderProgressPlan("canceled", None, "failed", approved_at)
        return OrderProgressPlan("approved", approved_at, "completed", approved_at)
    if current_status == "approved" and logical_date >= shipped_at:
        if _choice(random_seed, order_id, "approved-cancel") % 1000 < 2:
            return OrderProgressPlan("canceled", None, "refunded", shipped_at)
        return OrderProgressPlan("shipped", shipped_at, None, None)
    if current_status == "shipped" and logical_date >= delivered_at:
        return OrderProgressPlan("delivered", delivered_at, None, None)
    return None
