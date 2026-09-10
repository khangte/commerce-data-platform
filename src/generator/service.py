"""결정적 Generator 구성 요소를 실제 Source Mutation 실행으로 조합한다."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import timedelta

from src.common.database import PostgresSettings
from src.generator.config import EXECUTABLE_ANOMALY_PROFILES, GeneratorConfig
from src.generator.customers import (
    MembershipTierRecord,
    SubscriptionRecord,
    membership_tier_change_records,
    new_customer_record,
    persist_membership_tier_records,
    persist_subscription_records,
    subscription_transition_records,
)
from src.generator.ids import logical_hash
from src.generator.lease import (
    GENERATOR_OWNER_TYPE,
    SourceMutationLease,
    acquire_source_mutation_lease,
    assert_source_mutation_lease,
    ensure_source_mutation_lease_metadata,
    release_source_mutation_lease,
)
from src.generator.metadata import (
    ensure_generator_metadata,
    record_finished_run,
    record_started_run,
)
from src.generator.orders import fetch_order_catalog, new_order_bundle, persist_order_bundle
from src.generator.scenarios import late_order_bundle
from src.generator.subscription_payments import (
    next_billing_sequence,
    persist_subscription_payments,
    plan_subscription_payment,
)


@dataclass(frozen=True)
class GeneratorResult:
    """Generator 실행 또는 기존 성공 결과 재사용의 결정성 증적이다."""

    generator_run_id: uuid.UUID
    result_counts: dict[str, int]
    logical_content_hash: str
    reused_successful_run: bool


def resolve_source_snapshot_id(settings: PostgresSettings) -> str:
    """가장 최근 성공한 Seed의 Checksum과 기준 시각으로 Snapshot ID를 만든다."""
    with settings.pipeline_connection() as connection:
        row = connection.execute(
            """
            SELECT raw_checksum, seeded_at
            FROM seed_runs
            WHERE status = 'SUCCESS'
            ORDER BY finished_at DESC
            LIMIT 1
            """
        ).fetchone()
    if row is None:
        raise ValueError("A successful seed run is required before running the generator")
    return f"seed:{row[0]}:{row[1].isoformat()}"


def run_generator(config: GeneratorConfig, settings: PostgresSettings) -> GeneratorResult:
    """Lease 보호 아래서 결정적 Bundle을 적재하고 Generator 실행 증적을 기록한다."""
    expected_snapshot_id = resolve_source_snapshot_id(settings)
    if config.source_snapshot_id != expected_snapshot_id:
        raise ValueError(
            "source_snapshot_id differs from the current successful seed snapshot: "
            f"{expected_snapshot_id}"
        )
    if config.anomaly_profile not in EXECUTABLE_ANOMALY_PROFILES:
        executable_profiles = ", ".join(sorted(EXECUTABLE_ANOMALY_PROFILES))
        raise ValueError(
            f"The executable generator currently supports {executable_profiles} profiles; "
            "delayed-payment remains a reusable scenario fixture."
        )

    ensure_generator_metadata(settings)
    ensure_source_mutation_lease_metadata(settings)
    existing = _successful_result(settings, config)
    if existing is not None:
        return existing

    generator_run_id = uuid.uuid4()
    lease: SourceMutationLease | None = None
    started = False
    try:
        lease = acquire_source_mutation_lease(
            settings,
            owner_type=GENERATOR_OWNER_TYPE,
            owner_id=generator_run_id,
        )
        with settings.pipeline_connection() as connection:
            record_started_run(connection, generator_run_id, config)
        started = True

        with settings.source_connection() as connection, connection.transaction():
            catalog = fetch_order_catalog(connection)
            result_counts = _empty_result_counts()
            logical_rows = []
            for order_ordinal in range(1, config.order_count + 1):
                assert_source_mutation_lease(settings, lease)
                customer = new_customer_record(config, order_ordinal)
                bundle = _bundle_for_profile(config, customer, catalog, order_ordinal)
                mutation_result = persist_order_bundle(connection, bundle)
                _add_mutation_counts(result_counts, mutation_result)
                logical_rows.append(
                    {
                        "customer_id": bundle.customer.customer_id,
                        "order_id": bundle.order.order_id,
                        "item_ids": [item.order_item_id for item in bundle.items],
                        "payment_sequences": [
                            payment.payment_sequential for payment in bundle.payments
                        ],
                    }
                )
            _run_subscription_expiry_scan(connection, config, result_counts, logical_rows)
            if config.anomaly_profile == "membership-change":
                tier_result, tier_row = _apply_membership_tier_change(connection, config)
                result_counts["membership_tiers_inserted"] += tier_result.inserted
                result_counts["membership_tiers_updated"] += tier_result.updated
                result_counts["membership_tiers_skipped"] += tier_result.skipped
                logical_rows.append(tier_row)
            elif config.anomaly_profile.startswith("subscription-"):
                subscription_result, subscription_row = _apply_subscription_transition(
                    connection, config
                )
                result_counts["subscriptions_inserted"] += subscription_result.inserted
                result_counts["subscriptions_updated"] += subscription_result.updated
                result_counts["subscriptions_skipped"] += subscription_result.skipped
                logical_rows.append(subscription_row)

        logical_content_hash = logical_hash(
            {
                "generator_inputs": config.deterministic_inputs(),
                "result_counts": result_counts,
                "rows": logical_rows,
            }
        )
        record_finished_run(
            settings,
            generator_run_id,
            status="SUCCESS",
            result_counts=result_counts,
            logical_content_hash=logical_content_hash,
        )
        return GeneratorResult(
            generator_run_id=generator_run_id,
            result_counts=result_counts,
            logical_content_hash=logical_content_hash,
            reused_successful_run=False,
        )
    except Exception as error:
        if started:
            record_finished_run(
                settings,
                generator_run_id,
                status="FAILED",
                error_message=str(error),
            )
        raise
    finally:
        if lease is not None:
            release_source_mutation_lease(settings, lease)


def _successful_result(
    settings: PostgresSettings, config: GeneratorConfig
) -> GeneratorResult | None:
    """동일 결정성 입력으로 이미 성공한 실행 결과가 있으면 재사용 증적을 반환한다."""
    with settings.pipeline_connection() as connection:
        row = connection.execute(
            """
            SELECT generator_run_id, result_counts, logical_hash
            FROM generator_runs
            WHERE source_snapshot_id = %s
              AND random_seed = %s
              AND logical_date = %s
              AND order_count = %s
              AND anomaly_profile = %s
              AND generator_version = %s
              AND status = 'SUCCESS'
            """,
            (
                config.source_snapshot_id,
                config.random_seed,
                config.logical_date,
                config.order_count,
                config.anomaly_profile,
                config.generator_version,
            ),
        ).fetchone()
    if row is None:
        return None
    return GeneratorResult(
        generator_run_id=row[0],
        result_counts=dict(row[1]),
        logical_content_hash=row[2],
        reused_successful_run=True,
    )


def _bundle_for_profile(config: GeneratorConfig, customer, catalog, order_ordinal: int):
    """실행 가능한 Anomaly Profile에 맞는 결정적 Order Bundle을 만든다."""
    if config.anomaly_profile == "late-arrival":
        business_event_time = config.logical_date - timedelta(days=3 + order_ordinal % 3)
        return late_order_bundle(config, customer, catalog, order_ordinal, business_event_time)
    return new_order_bundle(config, customer, catalog, order_ordinal)


def _empty_result_counts() -> dict[str, int]:
    """Generator Metadata에 저장할 Entity별 초기 Mutation Count를 반환한다."""
    return {
        "customers_inserted": 0,
        "customers_updated": 0,
        "customers_skipped": 0,
        "subscriptions_inserted": 0,
        "subscriptions_updated": 0,
        "subscriptions_skipped": 0,
        "membership_tiers_inserted": 0,
        "membership_tiers_updated": 0,
        "membership_tiers_skipped": 0,
        "subscription_payments_inserted": 0,
        "orders_inserted": 0,
        "orders_skipped": 0,
        "order_items_inserted": 0,
        "order_items_skipped": 0,
        "payments_inserted": 0,
        "payments_skipped": 0,
    }


def _add_mutation_counts(result_counts: dict[str, int], mutation_result) -> None:
    """Order Bundle 저장 결과를 Generator Metadata의 Entity별 Count에 누적한다."""
    result_counts["customers_inserted"] += mutation_result.customer.inserted
    result_counts["customers_updated"] += mutation_result.customer.updated
    result_counts["customers_skipped"] += mutation_result.customer.skipped
    result_counts["subscriptions_inserted"] += mutation_result.subscription.inserted
    result_counts["subscriptions_updated"] += mutation_result.subscription.updated
    result_counts["subscriptions_skipped"] += mutation_result.subscription.skipped
    result_counts["membership_tiers_inserted"] += mutation_result.membership_tier.inserted
    result_counts["membership_tiers_updated"] += mutation_result.membership_tier.updated
    result_counts["membership_tiers_skipped"] += mutation_result.membership_tier.skipped
    result_counts["orders_inserted"] += mutation_result.orders_inserted
    result_counts["orders_skipped"] += mutation_result.orders_skipped
    result_counts["order_items_inserted"] += mutation_result.items_inserted
    result_counts["order_items_skipped"] += mutation_result.items_skipped
    result_counts["payments_inserted"] += mutation_result.payments_inserted
    result_counts["payments_skipped"] += mutation_result.payments_skipped


def _fetch_subscriptions_ordered(connection) -> tuple[SubscriptionRecord, ...]:
    """구독 상태 Record 전체를 사람 키 순서로 읽는다."""
    rows = connection.execute(
        """
        SELECT customer_unique_id, subscription_status,
               trial_ends_at, benefit_ends_at, next_billing_at,
               payment_failed_at, cancel_requested_at, created_at, updated_at
        FROM customer_subscriptions
        ORDER BY customer_unique_id COLLATE "C"
        """
    ).fetchall()
    return tuple(SubscriptionRecord(*row) for row in rows)


def _run_subscription_expiry_scan(
    connection, config: GeneratorConfig, result_counts: dict[str, int], logical_rows: list
) -> None:
    """logical_date 기준 시각 스캔으로 만료 종료·체험 종료·정기 결제·재결제를 처리한다.

    요구사항 5.1절 순서를 그대로 따른다. 무작위가 아니라 결정적 스캔이므로 같은 logical_date로
    재실행하면 같은 결과가 나온다.
    """
    now = config.logical_date
    for record in _fetch_subscriptions_ordered(connection):
        if record.updated_at >= now:
            continue
        status = record.subscription_status
        # 1. 만료 종료
        if (
            status in {"CANCEL_REQUESTED", "PAYMENT_FAILED"}
            and record.benefit_ends_at is not None
            and record.benefit_ends_at <= now
        ):
            _persist_scan_transition(connection, config, record, "CHURNED", result_counts)
            logical_rows.append(_subscription_scan_row(record.customer_unique_id, "CHURNED"))
            continue
        # 2. 체험 종료
        if (
            status == "TRIAL"
            and record.trial_ends_at is not None
            and record.trial_ends_at <= now
        ):
            _bill_and_transition(connection, config, record, result_counts, logical_rows)
            continue
        # 3. 정기 결제
        if (
            status == "ACTIVE"
            and record.next_billing_at is not None
            and record.next_billing_at <= now
        ):
            _bill_and_transition(connection, config, record, result_counts, logical_rows)
            continue
        # 4. 재결제
        if (
            status == "PAYMENT_FAILED"
            and record.benefit_ends_at is not None
            and record.benefit_ends_at > now
        ):
            _bill_and_transition(connection, config, record, result_counts, logical_rows)


def _bill_and_transition(
    connection,
    config: GeneratorConfig,
    record: SubscriptionRecord,
    result_counts: dict[str, int],
    logical_rows: list,
) -> None:
    """결제를 시도해 subscription_payments 행을 남기고 성공·실패에 따라 상태를 바꾼다."""
    billing_sequence = next_billing_sequence(connection, record.customer_unique_id)
    period_start = record.next_billing_at or record.trial_ends_at or config.logical_date
    payment = plan_subscription_payment(
        config, record.customer_unique_id, billing_sequence, period_start
    )
    result_counts["subscription_payments_inserted"] += persist_subscription_payments(
        connection, (payment,)
    )
    logical_rows.append(
        {
            "customer_unique_id": record.customer_unique_id,
            "billing_sequence": billing_sequence,
            "payment_status": payment.payment_status,
        }
    )
    if payment.payment_status == "completed":
        if record.subscription_status == "ACTIVE":
            _advance_active_billing(connection, config, record, payment, result_counts)
        else:
            _persist_scan_transition(connection, config, record, "ACTIVE", result_counts)
            logical_rows.append(_subscription_scan_row(record.customer_unique_id, "ACTIVE"))
    elif record.subscription_status != "PAYMENT_FAILED":
        _persist_scan_transition(connection, config, record, "PAYMENT_FAILED", result_counts)
        logical_rows.append(
            _subscription_scan_row(record.customer_unique_id, "PAYMENT_FAILED")
        )


def _advance_active_billing(
    connection, config: GeneratorConfig, record: SubscriptionRecord, payment, result_counts
) -> None:
    """ACTIVE 유지 결제 성공 시 상태 변화 없이 next_billing_at만 1개월 뒤로 민다."""
    advanced = SubscriptionRecord(
        customer_unique_id=record.customer_unique_id,
        subscription_status="ACTIVE",
        trial_ends_at=None,
        benefit_ends_at=payment.billing_period_end,
        next_billing_at=payment.billing_period_end,
        payment_failed_at=None,
        cancel_requested_at=None,
        created_at=record.created_at,
        updated_at=config.logical_date,
    )
    outcome = persist_subscription_records(connection, (advanced,))
    result_counts["subscriptions_updated"] += outcome.updated
    result_counts["subscriptions_skipped"] += outcome.skipped


def _persist_scan_transition(
    connection,
    config: GeneratorConfig,
    record: SubscriptionRecord,
    next_status: str,
    result_counts: dict[str, int],
) -> None:
    """스캔이 만든 구독 상태 전이를 저장하고 Count에 누적한다."""
    changed = subscription_transition_records(config, (record,), next_status)
    outcome = persist_subscription_records(connection, changed)
    result_counts["subscriptions_updated"] += outcome.updated
    result_counts["subscriptions_skipped"] += outcome.skipped


def _subscription_scan_row(customer_unique_id: str, next_status: str) -> dict[str, str]:
    """만료 스캔이 만든 구독 전이의 결정성 Hash 입력 Row를 반환한다."""
    return {
        "customer_unique_id": customer_unique_id,
        "scan_subscription_status": next_status,
    }


def _apply_membership_tier_change(connection, config: GeneratorConfig):
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


def _apply_subscription_transition(connection, config: GeneratorConfig):
    """Profile에 맞는 현재 상태 사람 하나를 골라 구독 상태 전이를 저장한다."""
    next_status, source_statuses = _subscription_profile_contract(config.anomaly_profile)
    rows = connection.execute(
        """
        SELECT customer_unique_id, subscription_status,
               trial_ends_at, benefit_ends_at, next_billing_at,
               payment_failed_at, cancel_requested_at, created_at, updated_at
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
        or (record.benefit_ends_at is not None and config.logical_date >= record.benefit_ends_at)
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


def _subscription_profile_contract(profile: str) -> tuple[str, frozenset[str]]:
    """실행 Profile의 목표 상태와 허용 시작 상태를 반환한다."""
    contracts = {
        "subscription-trial": ("TRIAL", frozenset({"NON_MEMBER", "CHURNED"})),
        "subscription-active": ("ACTIVE", frozenset({"NON_MEMBER", "TRIAL", "PAYMENT_FAILED"})),
        "subscription-payment-failed": ("PAYMENT_FAILED", frozenset({"TRIAL", "ACTIVE"})),
        "subscription-cancel-requested": (
            "CANCEL_REQUESTED",
            frozenset({"TRIAL", "ACTIVE", "PAYMENT_FAILED"}),
        ),
        "subscription-churned": ("CHURNED", frozenset({"PAYMENT_FAILED", "CANCEL_REQUESTED"})),
        "subscription-rejoined": ("ACTIVE", frozenset({"CHURNED"})),
    }
    return contracts[profile]


