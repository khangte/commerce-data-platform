"""Generator의 원천 커서 역행 방지와 오류 분류를 검증한다."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.generator.config import GENERATOR_VERSION, GeneratorConfig
from src.generator.errors import SourceCursorRegressionError
from src.generator.service import GeneratorResult, run_generator
from src.ingestion.errors import SOURCE_CONTRACT_ERROR, classify_error, is_retryable


def _config(logical_date: datetime) -> GeneratorConfig:
    """역행 검증에 사용할 결정적 실행 입력을 만든다."""
    return GeneratorConfig(
        source_snapshot_id="seed:test",
        random_seed=42,
        logical_date=logical_date,
        order_count=1,
        anomaly_profile="default",
        generator_version=GENERATOR_VERSION,
    )


@pytest.mark.parametrize("offset", (-timedelta(hours=1), timedelta(0)))
def test_generator_rejects_earlier_or_equal_cursor_before_source_write(monkeypatch, offset) -> None:
    """이전 최대 갱신 시각 이하의 새 실행은 원천 쓰기 없이 실패한다."""
    from src.generator import service

    maximum = datetime(2026, 9, 29, 12, tzinfo=UTC)
    config = _config(maximum + offset)
    settings = MagicMock()
    source = settings.source_connection.return_value.__enter__.return_value

    def cursor_result(query, *_):
        """주문 테이블에서만 역행하는 커서 최대값을 반환한다."""
        result = MagicMock()
        result.fetchone.return_value = (maximum if '"orders"' in query else maximum - timedelta(days=1),)
        return result

    source.execute.side_effect = cursor_result
    monkeypatch.setattr(service, "resolve_source_snapshot_id", lambda _: "seed:test")
    monkeypatch.setattr(service, "ensure_generator_metadata", lambda _: None)
    monkeypatch.setattr(service, "ensure_source_mutation_lease_metadata", lambda _: None)
    monkeypatch.setattr(service, "_successful_result", lambda *_: None)
    monkeypatch.setattr(service, "acquire_source_mutation_lease", lambda *_, **__: MagicMock())
    monkeypatch.setattr(service, "assert_source_mutation_lease", lambda *_: None)
    monkeypatch.setattr(service, "release_source_mutation_lease", lambda *_: None)
    monkeypatch.setattr(service, "record_started_run", lambda *_: None)
    monkeypatch.setattr(service, "record_finished_run", lambda *_, **__: None)
    monkeypatch.setattr(service, "fetch_order_catalog", lambda *_: [])
    monkeypatch.setattr(service, "_bundle_for_profile", lambda *_: MagicMock())
    persist = MagicMock(side_effect=AssertionError("원천 쓰기가 먼저 실행됨"))
    monkeypatch.setattr(service, "persist_order_bundle", persist)

    with pytest.raises(SourceCursorRegressionError, match="orders"):
        run_generator(config, settings)

    persist.assert_not_called()
    assert source.execute.call_count >= 1


def test_generator_rejects_created_at_cursor_regression(monkeypatch) -> None:
    """주문 항목의 생성 시각 커서만 앞서 있어도 원천 쓰기 전에 실패한다."""
    from src.generator import service

    logical_date = datetime(2026, 9, 29, 12, tzinfo=UTC)
    settings = MagicMock()
    source = settings.source_connection.return_value.__enter__.return_value

    def cursor_result(query, *_):
        """주문 항목의 생성 시각만 실행 시각보다 앞선 결과를 만든다."""
        result = MagicMock()
        result.fetchall.return_value = [("orders",)]
        result.fetchone.return_value = (
            logical_date + timedelta(hours=1)
            if '"order_items"' in query else logical_date - timedelta(days=1),
        )
        return result

    source.execute.side_effect = cursor_result
    monkeypatch.setattr(service, "resolve_source_snapshot_id", lambda _: "seed:test")
    monkeypatch.setattr(service, "ensure_generator_metadata", lambda _: None)
    monkeypatch.setattr(service, "ensure_source_mutation_lease_metadata", lambda _: None)
    monkeypatch.setattr(service, "_successful_result", lambda *_: None)
    monkeypatch.setattr(service, "acquire_source_mutation_lease", lambda *_, **__: MagicMock())
    monkeypatch.setattr(service, "assert_source_mutation_lease", lambda *_: None)
    monkeypatch.setattr(service, "release_source_mutation_lease", lambda *_: None)
    monkeypatch.setattr(service, "record_started_run", lambda *_: None)
    monkeypatch.setattr(service, "record_finished_run", lambda *_, **__: None)
    monkeypatch.setattr(service, "fetch_order_catalog", lambda *_: [])
    monkeypatch.setattr(service, "_bundle_for_profile", lambda *_: MagicMock())
    persist = MagicMock(side_effect=AssertionError("원천 쓰기가 먼저 실행됨"))
    monkeypatch.setattr(service, "persist_order_bundle", persist)

    with pytest.raises(SourceCursorRegressionError, match="order_items.created_at"):
        run_generator(_config(logical_date), settings)

    persist.assert_not_called()


def test_generator_accepts_forward_cursor_with_empty_tables(monkeypatch) -> None:
    """모든 커서가 이전이거나 비어 있으면 검사를 통과해 원천 쓰기를 시작한다."""
    from src.generator import service

    logical_date = datetime(2026, 9, 29, 12, tzinfo=UTC)
    settings = MagicMock()
    source = settings.source_connection.return_value.__enter__.return_value

    def cursor_result(query, *_):
        """빈 구독 테이블은 NULL, 나머지는 이전 커서 최대값을 돌려준다."""
        result = MagicMock()
        maximum = None if any(
            f'"{name}"' in query for name in ("customer_subscriptions", "subscription_payments")
        ) else logical_date - timedelta(seconds=1)
        result.fetchone.return_value = (maximum,)
        return result

    source.execute.side_effect = cursor_result
    bundle = SimpleNamespace(
        customer=SimpleNamespace(customer_id="customer-test"),
        order=SimpleNamespace(order_id="order-test"),
        items=(),
        payments=(),
    )
    monkeypatch.setattr(service, "resolve_source_snapshot_id", lambda _: "seed:test")
    monkeypatch.setattr(service, "ensure_generator_metadata", lambda _: None)
    monkeypatch.setattr(service, "ensure_source_mutation_lease_metadata", lambda _: None)
    monkeypatch.setattr(service, "_successful_result", lambda *_: None)
    monkeypatch.setattr(service, "acquire_source_mutation_lease", lambda *_, **__: MagicMock())
    monkeypatch.setattr(service, "assert_source_mutation_lease", lambda *_: None)
    monkeypatch.setattr(service, "release_source_mutation_lease", lambda *_: None)
    monkeypatch.setattr(service, "record_started_run", lambda *_: None)
    monkeypatch.setattr(service, "record_finished_run", lambda *_, **__: None)
    monkeypatch.setattr(service, "fetch_order_catalog", lambda *_: [])
    monkeypatch.setattr(service, "_bundle_for_profile", lambda *_: bundle)
    monkeypatch.setattr(service, "_add_mutation_counts", lambda *_: None)
    monkeypatch.setattr(service, "_run_subscription_expiry_scan", lambda *_: None)
    persist = MagicMock()
    monkeypatch.setattr(service, "persist_order_bundle", persist)

    result = run_generator(_config(logical_date), settings)

    assert result.reused_successful_run is False
    persist.assert_called_once()
    assert source.execute.call_count == len(service.MUTABLE_SOURCE_TABLES) + 1


def test_generator_reuses_identical_success_before_cursor_check(monkeypatch) -> None:
    """같은 입력의 성공 결과는 Lease와 원천 검사 전에 재사용한다."""
    from src.generator import service

    settings = MagicMock()
    config = _config(datetime(2026, 9, 29, 12, tzinfo=UTC))
    expected = GeneratorResult(uuid.uuid4(), {"orders_inserted": 1}, "hash", True)
    monkeypatch.setattr(service, "resolve_source_snapshot_id", lambda _: "seed:test")
    monkeypatch.setattr(service, "ensure_generator_metadata", lambda _: None)
    monkeypatch.setattr(service, "ensure_source_mutation_lease_metadata", lambda _: None)
    monkeypatch.setattr(service, "_successful_result", lambda *_: expected)

    assert run_generator(config, settings) == expected
    settings.source_connection.assert_not_called()


def test_cursor_regression_is_non_retryable_source_contract_error() -> None:
    """역행 예외는 기존 원천 계약 오류로 분류하고 재시도하지 않는다."""
    error = SourceCursorRegressionError("logical_date is not after orders max(updated_at)")

    assert classify_error(error) == SOURCE_CONTRACT_ERROR
    assert is_retryable(error) is False
