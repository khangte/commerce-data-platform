"""membership-change와 구독 Profile이 고른 한 사람의 상태를 결정적으로 바꾼다."""

from __future__ import annotations

from src.generator.core.config import GeneratorConfig
from src.generator.core.ids import logical_hash
from src.generator.customers.customers import (
    MembershipTierRecord,
    SubscriptionRecord,
    membership_tier_change_records,
    new_subscription_record_for_customer,
    persist_membership_tier_records,
    persist_subscription_records,
    subscription_transition_records,
)


def apply_membership_tier_change(connection, config: GeneratorConfig):
    """현재 BRONZE·SILVER 사람 하나를 결정적으로 선택해 등급 변경을 저장한다."""
    rows = connection.execute(
        """
        SELECT customer_unique_id, membership_tier, created_at, updated_at
        FROM customer_membership_tiers
        WHERE membership_tier IN ('BRONZE', 'SILVER')
          AND updated_at < %s
        ORDER BY customer_unique_id COLLATE "C"
        """,
        (config.logical_date,),
    ).fetchall()
    if not rows:
        raise ValueError("membership-change requires at least one BRONZE or SILVER tier record")
    candidates = tuple(MembershipTierRecord(*row) for row in rows)
    selector = int(
        logical_hash(
            {
                "generator_inputs": config.deterministic_inputs(),
                "entity": "membership-change-selection",
            }
        ),
        16,
    ) % len(candidates)
    current = candidates[selector]
    delivered_order_count = 5 if current.membership_tier == "BRONZE" else 15
    changed = membership_tier_change_records(config, (current,), delivered_order_count)
    result = persist_membership_tier_records(connection, changed)
    return result, {
        "customer_unique_id": current.customer_unique_id,
        "membership_tier": changed[0].membership_tier,
        "membership_tier_updated_at": changed[0].updated_at.isoformat(),
    }


def apply_subscription_transition(
    connection, config: GeneratorConfig, result_counts: dict[str, int], logical_rows: list
):
    """Profile에 맞는 구독 상태를 바꾼다."""
    if config.anomaly_profile == "subscription-active":
        return _start_subscription_contract(connection, config)
    next_status, source_statuses = _subscription_profile_contract(config.anomaly_profile)
    rows = connection.execute(
        """
        SELECT subscription_id, customer_unique_id, subscription_status, auto_renew_enabled,
               subscription_started_at, current_period_started_at, current_period_ends_at,
               billing_due_at, next_payment_attempt_at, payment_failed_at, cancel_requested_at,
               ended_at, status_changed_at, created_at, updated_at
        FROM customer_subscriptions
        WHERE subscription_status = ANY(%s)
          AND updated_at < %s
        ORDER BY customer_unique_id COLLATE "C"
        """,
        (list(source_statuses), config.logical_date),
    ).fetchall()
    candidates = tuple(SubscriptionRecord(*row) for row in rows)
    candidates = tuple(
        record
        for record in candidates
        if next_status != "CHURNED"
        or (
            record.current_period_ends_at is not None
            and config.logical_date >= record.current_period_ends_at
        )
    )
    if not candidates:
        raise ValueError(f"{config.anomaly_profile} requires an eligible subscription record")
    selector = int(
        logical_hash(
            {
                "generator_inputs": config.deterministic_inputs(),
                "entity": "subscription-transition-selection",
            }
        ),
        16,
    ) % len(candidates)
    current = candidates[selector]
    changed = subscription_transition_records(config, (current,), next_status)
    result = persist_subscription_records(connection, changed)
    return result, {
        "customer_unique_id": current.customer_unique_id,
        "subscription_status": changed[0].subscription_status,
        "subscription_updated_at": changed[0].updated_at.isoformat(),
    }


def _start_subscription_contract(connection, config: GeneratorConfig):
    """현재 유효 계약이 없는 고객 한 명의 자동갱신 구독 계약을 시작한다."""
    rows = connection.execute(
        """
        SELECT DISTINCT customers.customer_unique_id COLLATE "C" AS customer_unique_id
        FROM customers
        WHERE NOT EXISTS (
            SELECT 1
            FROM customer_subscriptions
            WHERE customer_subscriptions.customer_unique_id = customers.customer_unique_id
              AND customer_subscriptions.subscription_status <> 'CHURNED'
        )
        ORDER BY customer_unique_id
        """
    ).fetchall()
    if not rows:
        raise ValueError("subscription-active requires a customer without an open subscription")
    selector = int(
        logical_hash(
            {
                "generator_inputs": config.deterministic_inputs(),
                "entity": "subscription-contract-start-selection",
            }
        ),
        16,
    ) % len(rows)
    record = new_subscription_record_for_customer(rows[selector][0], config.logical_date)
    result = persist_subscription_records(connection, (record,))
    return result, {
        "customer_unique_id": record.customer_unique_id,
        "subscription_id": str(record.subscription_id),
        "subscription_status": record.subscription_status,
        "subscription_updated_at": record.updated_at.isoformat(),
    }


def _subscription_profile_contract(profile: str) -> tuple[str, frozenset[str]]:
    """실행 Profile의 목표 상태와 허용 시작 상태를 반환한다."""
    contracts = {
        "subscription-cancel-requested": (
            "CANCEL_REQUESTED",
            frozenset({"ACTIVE", "PAYMENT_FAILED"}),
        ),
        "subscription-churned": ("CHURNED", frozenset({"PAYMENT_FAILED", "CANCEL_REQUESTED"})),
    }
    return contracts[profile]
