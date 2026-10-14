"""Seed 이후 Generator 주문을 결정적 예정 시각에 따라 한 단계씩 전이한다."""

from __future__ import annotations

import psycopg

from src.common.database import PostgresSettings
from src.generator.core.config import GeneratorConfig
from src.generator.core.lease import SourceMutationLease, assert_source_mutation_lease
from src.generator.orders.order_progress import plan_order_progress
from src.generator.orders.transitions import (
    fetch_order_state,
    fetch_payment_state,
    persist_order_transition,
    persist_payment_transition,
    plan_order_transition,
    plan_payment_transition,
)


def run_existing_order_transitions(
    connection: psycopg.Connection,
    settings: PostgresSettings,
    config: GeneratorConfig,
    lease: SourceMutationLease,
    result_counts: dict[str, int],
    logical_rows: list[dict],
) -> None:
    """Seed 이후 주문을 ID 순서로 읽고 각 주문을 최대 한 단계 전이한다."""
    with settings.pipeline_connection() as metadata_connection:
        seed_row = metadata_connection.execute(
            """
            SELECT seeded_at FROM seed_runs
            WHERE status = 'SUCCESS'
            ORDER BY finished_at DESC LIMIT 1
            """
        ).fetchone()
    if seed_row is None:
        raise ValueError("A successful seed run is required before advancing generator orders")
    seeded_at = seed_row[0]
    candidates = connection.execute(
        """
        SELECT order_id, order_status, created_at, order_approved_at, order_delivered_carrier_date
        FROM orders
        WHERE created_at > %s AND order_status IN ('created', 'approved', 'shipped')
        ORDER BY order_id COLLATE "C"
        """,
        (seeded_at,),
    ).fetchall()
    for order_id, current_status, created_at, approved_at, shipped_at in candidates:
        plan = plan_order_progress(
            config.random_seed,
            order_id,
            current_status,
            created_at,
            config.logical_date,
            progress_time_scale=config.progress_time_scale,
            approved_at=approved_at,
            shipped_at=shipped_at,
        )
        if plan is None:
            continue
        current = fetch_order_state(connection, order_id)
        order_transition = plan_order_transition(
            current, plan.next_status, config.logical_date, plan.business_event_time
        )
        assert_source_mutation_lease(settings, lease)
        result_counts["orders_updated"] += persist_order_transition(
            connection, order_transition
        ).updated
        if plan.payment_status is not None:
            payment_sequences = connection.execute(
                """
                SELECT payment_sequential FROM order_payments
                WHERE order_id = %s ORDER BY payment_sequential
                """,
                (order_id,),
            ).fetchall()
            if not payment_sequences:
                raise ValueError(f"Generator order has no payment: {order_id}")
            for (payment_sequential,) in payment_sequences:
                payment = fetch_payment_state(connection, order_id, payment_sequential)
                payment_transition = plan_payment_transition(
                    payment,
                    plan.payment_status,
                    config.logical_date,
                    plan.payment_event_time,
                )
                result_counts["payments_updated"] += persist_payment_transition(
                    connection, payment_transition
                ).updated
        logical_rows.append(
            {
                "order_id": order_id,
                "order_status": plan.next_status,
                "business_event_time": plan.business_event_time,
                "payment_status": plan.payment_status,
                "payment_event_time": plan.payment_event_time,
            }
        )
