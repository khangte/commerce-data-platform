"""Generator의 원천 커서 역행 방지와 오류 분류를 검증한다."""

from __future__ import annotations

import uuid
from contextlib import contextmanager, nullcontext
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.generator.commits import CommittedResult
from src.generator.config import GENERATOR_VERSION, GeneratorConfig
from src.generator.errors import SourceCursorRegressionError
from src.generator.lease import LeaseOwnershipLostError
from src.generator.service import GeneratorResult, run_generator
from src.ingestion.errors import SOURCE_CONTRACT_ERROR, classify_error, is_retryable


@pytest.fixture(autouse=True)
def mock_source_commit_metadata(monkeypatch) -> None:
    """커서 단위 테스트에서 Source 실행 마커 저장소를 격리한다."""
    from src.generator import service

    monkeypatch.setattr(service, "ensure_generator_commits", lambda _: None)
    monkeypatch.setattr(service, "committed_result", lambda *_: None)
    monkeypatch.setattr(service, "record_source_commit", lambda *_, **__: None)
    monkeypatch.setattr(service, "fenced_source_commit", lambda *_: nullcontext())


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


def test_generator_preserves_cursor_error_when_lease_release_fails(monkeypatch) -> None:
    """원천 커서 오류 뒤 잠금 해제도 실패하면 최초 계약 오류를 유지한다."""
    from src.generator import service

    settings = MagicMock()
    config = _config(datetime(2026, 9, 29, 12, tzinfo=UTC))
    monkeypatch.setattr(service, "resolve_source_snapshot_id", lambda _: "seed:test")
    monkeypatch.setattr(service, "ensure_generator_metadata", lambda _: None)
    monkeypatch.setattr(service, "ensure_source_mutation_lease_metadata", lambda _: None)
    monkeypatch.setattr(service, "ensure_generator_commits", lambda _: None)
    monkeypatch.setattr(service, "_successful_result", lambda *_: None)
    monkeypatch.setattr(service, "committed_result", lambda *_: None)
    monkeypatch.setattr(service, "acquire_source_mutation_lease", lambda *_, **__: MagicMock())
    monkeypatch.setattr(service, "record_started_run", lambda *_: None)
    monkeypatch.setattr(service, "record_finished_run", lambda *_, **__: None)

    def reject_cursor(*_) -> None:
        """원래 실행에서 발생하는 재시도 불가 커서 오류를 모의한다."""
        raise SourceCursorRegressionError("cursor regression")

    monkeypatch.setattr(
        service, "_assert_source_cursor_forward",
        reject_cursor,
    )
    release = MagicMock(side_effect=OSError("lease release failed"))
    monkeypatch.setattr(service, "release_source_mutation_lease", release)

    with pytest.raises(SourceCursorRegressionError, match="cursor regression") as captured:
        run_generator(config, settings)

    assert classify_error(captured.value) == SOURCE_CONTRACT_ERROR
    assert is_retryable(captured.value) is False
    release.assert_called_once()


def test_generator_reports_lease_release_failure_after_success(monkeypatch) -> None:
    """성공 결과를 복구한 뒤 잠금 해제가 실패하면 호출자에게 오류를 알린다."""
    from src.generator import service

    settings = MagicMock()
    config = _config(datetime(2026, 9, 29, 12, tzinfo=UTC))
    committed = CommittedResult(uuid.uuid4(), {"orders_inserted": 1}, "a" * 64)
    monkeypatch.setattr(service, "resolve_source_snapshot_id", lambda _: "seed:test")
    monkeypatch.setattr(service, "ensure_generator_metadata", lambda _: None)
    monkeypatch.setattr(service, "ensure_source_mutation_lease_metadata", lambda _: None)
    monkeypatch.setattr(service, "ensure_generator_commits", lambda _: None)
    monkeypatch.setattr(service, "_successful_result", lambda *_: None)
    monkeypatch.setattr(service, "committed_result", lambda *_: committed)
    monkeypatch.setattr(service, "acquire_source_mutation_lease", lambda *_, **__: MagicMock())
    monkeypatch.setattr(service, "record_finished_run", lambda *_, **__: None)
    release = MagicMock(side_effect=OSError("lease release failed"))
    monkeypatch.setattr(service, "release_source_mutation_lease", release)

    try:
        raise RuntimeError("outer error being handled")
    except RuntimeError:
        with pytest.raises(OSError, match="lease release failed"):
            run_generator(config, settings)

    release.assert_called_once()


def test_generator_recovers_failed_metadata_write_from_source_commit(monkeypatch) -> None:
    """성공 기록 실패 뒤 재시도가 Source 마커로 원래 실행을 성공으로 복구한다."""
    from src.generator import service

    config = replace(_config(datetime(2026, 9, 29, 12, tzinfo=UTC)), order_count=0)
    settings = MagicMock()
    source = settings.source_connection.return_value.__enter__.return_value
    source.execute.return_value.fetchone.return_value = (config.logical_date - timedelta(days=1),)
    committed = None
    statuses = []

    def save_commit(_, run_id, __, counts, content_hash) -> None:
        """같은 Source 트랜잭션에 저장될 실행 결과를 포착한다."""
        nonlocal committed
        committed = CommittedResult(run_id, counts.copy(), content_hash)

    def finish(_, run_id, *, status, **kwargs) -> None:
        """첫 성공 기록만 실패시키고 이후 상태 전이를 기록한다."""
        if status == "SUCCESS" and not statuses:
            statuses.append("WRITE_ERROR")
            raise OSError("metadata unavailable")
        statuses.append(status)

    monkeypatch.setattr(service, "resolve_source_snapshot_id", lambda _: "seed:test")
    monkeypatch.setattr(service, "ensure_generator_metadata", lambda _: None)
    monkeypatch.setattr(service, "ensure_source_mutation_lease_metadata", lambda _: None)
    monkeypatch.setattr(service, "_successful_result", lambda *_: None)
    monkeypatch.setattr(service, "committed_result", lambda *_: committed)
    monkeypatch.setattr(service, "record_source_commit", save_commit)
    monkeypatch.setattr(service, "record_finished_run", finish)
    monkeypatch.setattr(service, "record_started_run", lambda *_: None)
    monkeypatch.setattr(service, "acquire_source_mutation_lease", lambda *_, **__: MagicMock())
    monkeypatch.setattr(service, "assert_source_mutation_lease", lambda *_: None)
    monkeypatch.setattr(service, "release_source_mutation_lease", lambda *_: None)
    monkeypatch.setattr(service, "fetch_order_catalog", lambda *_: [])
    monkeypatch.setattr(service, "_run_existing_order_transitions", lambda *_: None)
    monkeypatch.setattr(service, "_run_subscription_expiry_scan", lambda *_: None)

    with pytest.raises(OSError, match="metadata unavailable"):
        run_generator(config, settings)
    recovered = run_generator(config, settings)

    assert statuses == ["WRITE_ERROR", "FAILED", "SUCCESS"]
    assert recovered.generator_run_id == committed.generator_run_id
    assert recovered.reused_successful_run is True
    assert source.execute.call_count == len(service.MUTABLE_SOURCE_TABLES)


@pytest.mark.parametrize("fence_fails", [False, True])
def test_source_commit_fence_order_and_rollback(monkeypatch, fence_fails) -> None:
    """마커 기록 뒤 잠금을 잡아 Source 커밋까지 유지하고 진입 실패 시 롤백한다."""
    from src.generator import service

    config = replace(_config(datetime(2026, 9, 29, 12, tzinfo=UTC)), order_count=0)
    settings = MagicMock()
    source = settings.source_connection.return_value.__enter__.return_value
    source.execute.return_value.fetchone.return_value = (config.logical_date - timedelta(days=1),)
    events = []
    markers = []

    def save_marker(*args) -> None:
        """거래 내 Source 마커 생성을 추적한다."""
        markers.append(args[1])
        events.append("marker")

    def transaction_exit(error_type, *_):
        """가짜 Source 거래의 커밋 또는 롤백을 기록한다."""
        if error_type is None:
            events.append("commit")
        else:
            markers.clear()
            events.append("rollback")
        return False

    @contextmanager
    def fence(*_):
        """잠금 진입·종료 순서와 진입 실패를 모의한다."""
        events.append("fence-enter")
        if fence_fails:
            raise LeaseOwnershipLostError("lease lost")
        try:
            yield
        finally:
            events.append("fence-exit")

    source.transaction.return_value.__exit__.side_effect = transaction_exit
    monkeypatch.setattr(service, "resolve_source_snapshot_id", lambda _: "seed:test")
    monkeypatch.setattr(service, "ensure_generator_metadata", lambda _: None)
    monkeypatch.setattr(service, "ensure_source_mutation_lease_metadata", lambda _: None)
    monkeypatch.setattr(service, "_successful_result", lambda *_: None)
    monkeypatch.setattr(service, "committed_result", lambda *_: None)
    monkeypatch.setattr(service, "acquire_source_mutation_lease", lambda *_, **__: MagicMock())
    monkeypatch.setattr(service, "release_source_mutation_lease", lambda *_: None)
    monkeypatch.setattr(service, "assert_source_mutation_lease", lambda *_: None)
    monkeypatch.setattr(service, "record_started_run", lambda *_: None)
    monkeypatch.setattr(service, "record_finished_run", lambda *_, **__: None)
    monkeypatch.setattr(service, "fetch_order_catalog", lambda *_: [])
    monkeypatch.setattr(service, "_run_existing_order_transitions", lambda *_: None)
    monkeypatch.setattr(service, "_run_subscription_expiry_scan", lambda *_: None)
    monkeypatch.setattr(service, "record_source_commit", save_marker)
    monkeypatch.setattr(service, "fenced_source_commit", fence, raising=False)

    if fence_fails:
        with pytest.raises(LeaseOwnershipLostError):
            run_generator(config, settings)
        assert events == ["marker", "fence-enter", "rollback"]
        assert markers == []
    else:
        run_generator(config, settings)
        assert events == ["marker", "fence-enter", "commit", "fence-exit"]
        assert len(markers) == 1
