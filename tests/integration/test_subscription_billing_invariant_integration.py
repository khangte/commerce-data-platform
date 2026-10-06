"""격리된 PostgreSQL 임시 테이블에서 구독 결제 불변식을 연속 실행으로 검증한다."""

from __future__ import annotations

import os
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from src.common.database import PostgresSettings
from src.generator import service, subscription_payments
from src.generator.config import GENERATOR_VERSION, GeneratorConfig

pytestmark = pytest.mark.integration


class _SharedConnection:
    """연속 실행 동안 하나의 PostgreSQL 트랜잭션을 유지한다."""

    def __init__(self, connection):
        """임시 테이블을 담은 연결을 보관한다."""
        self.connection = connection

    def __enter__(self):
        """서비스에 같은 연결을 제공한다."""
        return self.connection

    def __exit__(self, *_):
        """바깥 트랜잭션의 최종 롤백을 위해 연결을 유지한다."""
        return False


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting PostgreSQL.",
)
def test_run_generator_preserves_subscription_billing_invariants(monkeypatch) -> None:
    """105일 연속 실행에서 실패·재시도·취소 뒤 I1~I5 위반이 없다."""
    settings = PostgresSettings.from_environment()
    with settings.source_connection() as connection:
        connection.execute(
            "CREATE TEMP TABLE customers (LIKE public.customers INCLUDING ALL) ON COMMIT PRESERVE ROWS"
        )
        connection.execute(
            "CREATE TEMP TABLE customer_subscriptions (LIKE public.customer_subscriptions INCLUDING ALL) ON COMMIT PRESERVE ROWS"
        )
        connection.execute(
            "CREATE TEMP TABLE subscription_payments (LIKE public.subscription_payments INCLUDING ALL) ON COMMIT PRESERVE ROWS"
        )
        connection.execute("SET LOCAL search_path TO pg_temp, public")
        start = datetime(2027, 1, 1, tzinfo=UTC)
        for ordinal in range(3):
            connection.execute(
                "INSERT INTO customers (customer_id, customer_unique_id, created_at) VALUES (%s, %s, %s)",
                (f"billing-test-{ordinal}", f"billing-test-{ordinal}", start),
            )
        shared = _SharedConnection(connection)
        monkeypatch.setattr(settings.__class__, "source_connection", lambda _: shared)
        monkeypatch.setattr(service, "resolve_source_snapshot_id", lambda _: "seed:test")
        monkeypatch.setattr(service, "ensure_generator_metadata", lambda _: None)
        monkeypatch.setattr(service, "ensure_source_mutation_lease_metadata", lambda _: None)
        monkeypatch.setattr(service, "ensure_generator_commits", lambda _: None)
        monkeypatch.setattr(service, "_successful_result", lambda *_: None)
        monkeypatch.setattr(service, "committed_result", lambda *_: None)
        monkeypatch.setattr(
            service, "acquire_source_mutation_lease", lambda *_args, **_kwargs: MagicMock()
        )
        monkeypatch.setattr(service, "assert_source_mutation_lease", lambda *_: None)
        monkeypatch.setattr(service, "fenced_source_commit", lambda *_args, **_kwargs: nullcontext())
        monkeypatch.setattr(service, "release_source_mutation_lease", lambda *_: None)
        monkeypatch.setattr(service, "record_source_commit", lambda *_: None)
        monkeypatch.setattr(service, "record_started_run", lambda *_: None)
        monkeypatch.setattr(service, "record_finished_run", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(service, "_assert_source_cursor_forward", lambda *_: None)
        monkeypatch.setattr(service, "_run_existing_order_transitions", lambda *_: None)
        monkeypatch.setattr(service, "fetch_order_catalog", lambda *_: None)
        monkeypatch.setattr(
            subscription_payments,
            "_billing_outcome",
            lambda config, _sid, _cycle, attempt: (
                "failed"
                if attempt == 2 and config.logical_date == start + timedelta(days=32)
                else "completed"
            ),
        )

        def run(day: int, profile: str = "default", ordinal: int = 0):
            """하루의 Generator를 실행하고 결과 Count를 반환한다."""
            config = GeneratorConfig(
                "seed:test", ordinal, start + timedelta(days=day), 0, profile, GENERATOR_VERSION
            )
            return service.run_generator(config, settings).result_counts

        try:
            for ordinal in range(3):
                run(ordinal, "subscription-active", ordinal)
            before_early_profile = connection.execute(
                "SELECT count(*) FROM subscription_payments"
            ).fetchone()[0]
            with pytest.raises(ValueError, match="requires an ACTIVE subscription due for billing"):
                run(29, "subscription-payment-failed")
            assert (
                connection.execute("SELECT count(*) FROM subscription_payments").fetchone()[0]
                == before_early_profile
            )
            profile_failures = 0
            retry_failures = 0
            retry_successes = 0
            for day in range(3, 106):
                profile = (
                    "subscription-payment-failed"
                    if day == 30
                    else "subscription-cancel-requested"
                    if day == 50
                    else "default"
                )
                before = connection.execute(
                    "SELECT count(*) FROM subscription_payments WHERE payment_status='failed'"
                ).fetchone()[0]
                run(day, profile)
                after = connection.execute(
                    "SELECT count(*) FROM subscription_payments WHERE payment_status='failed'"
                ).fetchone()[0]
                if day == 30:
                    profile_failures += after - before
                if day == 32:
                    retry_failures += after - before
                if day == 34:
                    retry_successes += connection.execute(
                        "SELECT count(*) FROM subscription_payments WHERE payment_status='completed' AND attempt_sequence > 1"
                    ).fetchone()[0]
            assert profile_failures >= 1
            assert retry_failures >= 1
            assert retry_successes >= 1
            queries = (
                """SELECT count(*) FROM subscription_payments a JOIN subscription_payments b ON a.subscription_id=b.subscription_id AND a.payment_id<b.payment_id AND a.payment_status='completed' AND b.payment_status='completed' AND a.billing_period_start_at<b.billing_period_end_at AND b.billing_period_start_at<a.billing_period_end_at""",
                """SELECT count(*) FROM (SELECT subscription_id,billing_cycle_sequence FROM subscription_payments GROUP BY 1,2 HAVING count(*) FILTER (WHERE payment_status='completed')>1 OR max(attempt_sequence)>max(attempt_sequence) FILTER (WHERE payment_status='completed')) t""",
                "SELECT count(*) FROM subscription_payments WHERE payment_at<billing_period_start_at",
                """WITH latest AS (SELECT DISTINCT ON (subscription_id) subscription_id,billing_period_start_at s,billing_period_end_at e FROM subscription_payments WHERE payment_status='completed' ORDER BY subscription_id,billing_cycle_sequence DESC) SELECT count(*) FROM customer_subscriptions c LEFT JOIN latest l USING (subscription_id) WHERE c.subscription_status='ACTIVE' AND ((l.subscription_id IS NULL AND (c.current_period_started_at<>c.subscription_started_at OR c.current_period_ends_at<>c.subscription_started_at+interval '30 days')) OR (l.subscription_id IS NOT NULL AND (c.current_period_started_at<>l.s OR c.current_period_ends_at<>l.e)) OR c.billing_due_at<>c.current_period_ends_at OR c.next_payment_attempt_at<>c.current_period_ends_at)""",
                "SELECT count(*) FROM customer_subscriptions WHERE subscription_status='PAYMENT_FAILED' AND next_payment_attempt_at<=updated_at",
            )
            assert [connection.execute(query).fetchone()[0] for query in queries] == [0, 0, 0, 0, 0]
        finally:
            connection.rollback()
