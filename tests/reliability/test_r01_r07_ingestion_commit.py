"""Ingestion Commit Protocol 신뢰성 시나리오를 검증한다."""

from __future__ import annotations

import os
import re
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from psycopg.types.json import Jsonb

from src.common.database import PostgresSettings
from src.generator.ids import logical_hash
from src.ingestion import service as ingestion_service
from src.ingestion.corruption import INVALID_STATUS, CorruptionPlan
from src.ingestion.errors import (
    LEASE_OWNERSHIP_LOST,
    OBJECT_STORAGE_ERROR,
    UNKNOWN_ERROR,
    WATERMARK_CONFLICT,
    classify_error,
    is_retryable,
)
from src.ingestion.lease import (
    TableLeaseOwnershipLostError,
    acquire_table_lease,
    assert_table_lease,
    release_table_lease,
)
from src.ingestion.metadata import (
    CursorPosition,
    PipelineRun,
    TableCommit,
    VerifiedBronzeObject,
    WatermarkConflictError,
    commit_table_run,
    get_or_create_watermark,
    record_failed_run,
    record_started_run,
)
from src.ingestion.orphan import OrphanReconciliationError, find_orphan_candidates, reconcile_orphan
from src.ingestion.schema import SourceContractError as BronzeSchemaContractError
from src.ingestion.service import (
    OrdersIngestionRequest,
    ingest_orders,
    orders_object_keys,
    quarantine_object_keys,
)
from src.ingestion.storage import SeaweedFSSettings, seaweedfs_s3_client, stored_object_from_head
from src.warehouse.publish_metadata import ensure_publish_metadata
from tests.reliability.faults import (
    InjectedCrash,
    corrupt_manifest,
    crash_metadata_commit,
    fail_metadata_commit,
    fail_object_upload,
)
from tests.reliability.harness import collect_state, write_evidence

pytestmark = [
    pytest.mark.integration,
    pytest.mark.reliability,
    pytest.mark.skipif(
        os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
        or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
        reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
    ),
]


class _CrashAfterFinalObject(BaseException):
    """Metadata Commit 직전 프로세스 중단을 재현하는 테스트 전용 예외다."""


def test_r01_duplicate_batch_is_idempotent_across_three_runs(tmp_path) -> None:
    """같은 표준 Batch 세 번 실행이 Object와 Watermark를 한 번만 Commit하는지 검증한다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    ensure_publish_metadata(postgres)
    now = datetime(2026, 9, 20, tzinfo=UTC)
    pipeline_name = f"r01-{uuid.uuid4().hex}"
    request = OrdersIngestionRequest.for_dag_run(
        dag_id=pipeline_name,
        logical_date=now,
        pipeline_name=pipeline_name,
        page_size=2,
    )
    _set_watermark(postgres, pipeline_name, _lower_bound_before_five_rows(postgres), now)
    try:
        first = ingest_orders(postgres, storage, request, local_directory=tmp_path, now=now)
        prefix = first.object_key.rsplit("/", 1)[0] if first.object_key else ""
        after_first = collect_state(
            postgres, storage, warehouse_path=None, marts=(), object_prefix=prefix
        )
        second = ingest_orders(postgres, storage, request, local_directory=tmp_path, now=now)
        after_second = collect_state(
            postgres, storage, warehouse_path=None, marts=(), object_prefix=prefix
        )
        third = ingest_orders(postgres, storage, request, local_directory=tmp_path, now=now)
        after_third = collect_state(
            postgres, storage, warehouse_path=None, marts=(), object_prefix=prefix
        )
        object_proof = stored_object_from_head(storage, first.object_key)

        assert first.status == "SUCCESS"
        assert second.status == third.status == "SKIPPED_ALREADY_COMMITTED"
        assert first.object_key == second.object_key == third.object_key
        assert after_first.object_keys == after_second.object_keys == after_third.object_keys
        assert after_first.watermarks == after_second.watermarks == after_third.watermarks
        assert first.row_count == second.row_count == third.row_count
        assert object_proof.content_sha256 == stored_object_from_head(storage, second.object_key).content_sha256
        write_evidence(
            "r01",
            {
                "batch_id": request.batch_id,
                "content_sha256": object_proof.content_sha256[:12],
                "object_key": first.object_key,
                "object_key_count": len(after_third.object_keys),
                "row_count": first.row_count,
                "run_ids": [str(first.run.run_id), str(second.run.run_id), str(third.run.run_id)],
                "watermark_after": after_third.watermarks,
                "watermark_before": after_first.watermarks,
            },
        )
    finally:
        _cleanup(postgres, storage, pipeline_name, request)


def test_r06_orphan_reconciliation_commits_a_match_and_rejects_a_mismatch(
    monkeypatch, tmp_path
) -> None:
    """Orphan 수용과 Quarantine 거부를 실제 상태 전이와 함께 검증한다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    now = datetime(2026, 9, 20, tzinfo=UTC)
    accepted = _orphan_request("r06-accepted", now, page_size=2)
    rejected = _orphan_request(
        "r06-rejected", now, page_size=7, corruption_plan=CorruptionPlan({0: INVALID_STATUS})
    )
    _set_watermark(postgres, accepted.pipeline_name, _lower_bound_before_five_rows(postgres), now)
    _set_watermark(postgres, rejected.pipeline_name, _lower_bound_before_twenty_rows(postgres), now)

    def _crash(*_: object, **__: object) -> None:
        """검증된 Object 게시 뒤 Commit 전에 프로세스 중단을 재현한다."""
        raise _CrashAfterFinalObject()

    try:
        with monkeypatch.context() as scoped:
            scoped.setattr(ingestion_service, "commit_table_run", _crash)
            with pytest.raises(_CrashAfterFinalObject):
                ingest_orders(postgres, storage, accepted, local_directory=tmp_path, now=now)
        accepted_key, accepted_manifest = orders_object_keys(accepted.batch_id, now)
        accepted_before = collect_state(
            postgres,
            storage,
            warehouse_path=None,
            marts=(),
            object_prefix=accepted_key.rsplit("/", 1)[0],
        )
        accepted_candidate = next(
            item for item in find_orphan_candidates(postgres, storage) if item.object_key == accepted_key
        )
        reconcile_orphan(postgres, storage, accepted_candidate, now=now)
        accepted_after = collect_state(
            postgres,
            storage,
            warehouse_path=None,
            marts=(),
            object_prefix=accepted_key.rsplit("/", 1)[0],
        )
        with monkeypatch.context() as scoped:
            scoped.setattr(ingestion_service, "commit_table_run", _crash)
            with pytest.raises(_CrashAfterFinalObject):
                ingest_orders(postgres, storage, rejected, local_directory=tmp_path, now=now)
        rejected_key, _ = orders_object_keys(rejected.batch_id, now)
        rejected_before = collect_state(
            postgres, storage, warehouse_path=None, marts=(), object_prefix=rejected_key.rsplit("/", 1)[0]
        )
        rejected_candidate = next(
            item for item in find_orphan_candidates(postgres, storage) if item.object_key == rejected_key
        )
        with pytest.raises(
            OrphanReconciliationError,
            match="Orphans with rejects require manual reconciliation",
        ) as error:
            reconcile_orphan(postgres, storage, rejected_candidate, now=now)
        rejected_after = collect_state(
            postgres, storage, warehouse_path=None, marts=(), object_prefix=rejected_key.rsplit("/", 1)[0]
        )
        assert accepted_key in accepted_before.orphan_candidates
        assert accepted_key not in accepted_after.orphan_candidates
        watermark_key = f"{accepted.pipeline_name}:orders"
        assert accepted_before.watermarks[watermark_key] != accepted_after.watermarks[watermark_key]
        assert rejected_before.orphan_candidates == rejected_after.orphan_candidates
        with postgres.pipeline_connection() as connection:
            accepted_metadata = connection.execute(
                "SELECT status FROM bronze_objects WHERE table_batch_id = %s",
                (f"{accepted.batch_id}__orders",),
            ).fetchone()
            accepted_run = connection.execute(
                "SELECT status FROM pipeline_runs WHERE pipeline_name = %s AND source_table = 'orders'",
                (accepted.pipeline_name,),
            ).fetchone()
            rejected_metadata = connection.execute(
                "SELECT status FROM bronze_objects WHERE table_batch_id = %s",
                (f"{rejected.batch_id}__orders",),
            ).fetchone()
        assert accepted_metadata == ("COMMITTED",)
        assert accepted_run == ("SUCCESS",)
        assert rejected_metadata is None
        write_evidence("r06", {"accepted": {"manifest_key": accepted_manifest, "object_key": accepted_key, "orphan_after": accepted_after.orphan_candidates, "orphan_before": accepted_before.orphan_candidates, "watermark_after": accepted_after.watermarks, "watermark_before": accepted_before.watermarks}, "rejected": {"error": str(error.value), "object_key": rejected_key, "orphan_after": rejected_after.orphan_candidates, "orphan_before": rejected_before.orphan_candidates}})
    finally:
        _cleanup(postgres, storage, accepted.pipeline_name, accepted)
        _cleanup(postgres, storage, rejected.pipeline_name, rejected)


@pytest.mark.parametrize(
    ("kind", "expected_exception", "expected_message"),
    [
        (
            "CHECKSUM",
            OrphanReconciliationError,
            "Object HEAD or checksum differs from the VERIFIED manifest",
        ),
        (
            "ROW_RANGE",
            OrphanReconciliationError,
            "Pipeline run range differs from the orphan manifest",
        ),
        ("SCHEMA_VERSION", BronzeSchemaContractError, "SOURCE_CONTRACT_ERROR"),
    ],
)
def test_r07_broken_manifest_variants_block_automatic_commit(
    monkeypatch, tmp_path, kind, expected_exception, expected_message
) -> None:
    """변조된 Manifest 3종이 자동 Commit과 Watermark 이동을 막는지 검증한다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    now = datetime(2026, 9, 20, tzinfo=UTC)
    request = _orphan_request(f"r07-{kind.lower()}", now, page_size=2)
    _set_watermark(postgres, request.pipeline_name, _lower_bound_before_five_rows(postgres), now)
    object_key, manifest_key = orders_object_keys(request.batch_id, now)
    baseline = collect_state(
        postgres, storage, warehouse_path=None, marts=(), object_prefix=object_key.rsplit("/", 1)[0]
    )

    def _crash(*_: object, **__: object) -> None:
        """검증된 Object·Manifest 게시 뒤 Commit 전에 프로세스 중단을 재현한다."""
        raise _CrashAfterFinalObject()

    try:
        with monkeypatch.context() as scoped:
            scoped.setattr(ingestion_service, "commit_table_run", _crash)
            with pytest.raises(_CrashAfterFinalObject):
                ingest_orders(postgres, storage, request, local_directory=tmp_path, now=now)
        corrupt_manifest(storage, manifest_key, kind=kind)
        candidate = next(
            item for item in find_orphan_candidates(postgres, storage) if item.object_key == object_key
        )
        with pytest.raises(expected_exception, match=re.escape(expected_message)) as error:
            reconcile_orphan(postgres, storage, candidate, now=now)
        after = collect_state(
            postgres, storage, warehouse_path=None, marts=(), object_prefix=object_key.rsplit("/", 1)[0]
        )
        with postgres.pipeline_connection() as connection:
            catalog = connection.execute(
                "SELECT count(*) FROM bronze_objects WHERE table_batch_id = %s",
                (f"{request.batch_id}__orders",),
            ).fetchone()[0]
        assert catalog == 0
        assert baseline.watermarks == after.watermarks
        assert object_key in after.orphan_candidates
        write_evidence(
            f"r07-{kind.lower()}",
            {
                "batch_id": request.batch_id,
                "kind": kind,
                "object_key": object_key,
                "manifest_key": manifest_key,
                "refusal": str(error.value),
                "watermarks_before": baseline.watermarks,
                "watermarks_after": after.watermarks,
            },
        )
    finally:
        _cleanup(postgres, storage, request.pipeline_name, request)


def test_r02_upload_failure_keeps_the_watermark_and_commits_nothing(monkeypatch, tmp_path) -> None:
    """업로드 실패가 Metadata·Watermark를 바꾸지 않고 정상 재시도로 복구되는지 검증한다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    ensure_publish_metadata(postgres)
    now = datetime(2026, 9, 20, tzinfo=UTC)
    request = _orphan_request("r02", now, page_size=2)
    lower_bound = _lower_bound_before_five_rows(postgres)
    _set_watermark(postgres, request.pipeline_name, lower_bound, now)
    object_key, _ = orders_object_keys(request.batch_id, now)
    prefix = object_key.rsplit("/", 1)[0]
    before = collect_state(postgres, storage, warehouse_path=None, marts=(), object_prefix=prefix)
    try:
        with fail_object_upload(monkeypatch), pytest.raises(Exception) as error:
            ingest_orders(postgres, storage, request, local_directory=tmp_path, now=now)
        failed = collect_state(postgres, storage, warehouse_path=None, marts=(), object_prefix=prefix)
        with postgres.pipeline_connection() as connection:
            object_count = connection.execute(
                "SELECT count(*) FROM bronze_objects WHERE table_batch_id = %s",
                (f"{request.batch_id}__orders",),
            ).fetchone()[0]
            failed_run = connection.execute(
                "SELECT status, error_type FROM pipeline_runs WHERE pipeline_name = %s",
                (request.pipeline_name,),
            ).fetchone()
        assert object_count == 0
        assert before.watermarks == failed.watermarks
        assert object_key not in failed.object_keys
        assert failed_run == ("FAILED", OBJECT_STORAGE_ERROR)
        classified = classify_error(error.value)
        assert classified == OBJECT_STORAGE_ERROR
        http_status = error.value.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        assert is_retryable(error.value)
        recovered = ingest_orders(postgres, storage, request, local_directory=tmp_path, now=now)
        assert recovered.status == "SUCCESS"
        write_evidence(
            "r02",
            {
                "batch_id": request.batch_id,
                "classified": classified,
                "exception": type(error.value).__name__,
                "http_status": http_status,
                "is_retryable": is_retryable(error.value),
                "object_key": object_key,
                "stored_error_type": failed_run[1],
                "watermark_after_failure": failed.watermarks,
                "watermark_before": before.watermarks,
            },
        )
    finally:
        _cleanup(postgres, storage, request.pipeline_name, request)


def test_r03_a_commit_crash_leaves_a_reconcilable_orphan(monkeypatch, tmp_path) -> None:
    """Commit 경계 Crash가 RUNNING Orphan을 남기고 같은 Object로 복구되는지 검증한다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    now = datetime(2026, 9, 20, tzinfo=UTC)
    request = _orphan_request("r03-crash", now, page_size=2)
    _set_watermark(postgres, request.pipeline_name, _lower_bound_before_five_rows(postgres), now)
    object_key, _ = orders_object_keys(request.batch_id, now)
    baseline = collect_state(postgres, storage, warehouse_path=None, marts=(), object_prefix=object_key.rsplit("/", 1)[0])
    try:
        with crash_metadata_commit(monkeypatch), pytest.raises(InjectedCrash):
            ingest_orders(postgres, storage, request, local_directory=tmp_path, now=now)
        before = collect_state(postgres, storage, warehouse_path=None, marts=(), object_prefix=object_key.rsplit("/", 1)[0])
        candidate = next(item for item in find_orphan_candidates(postgres, storage) if item.object_key == object_key)
        assert object_key in before.object_keys
        assert object_key in before.orphan_candidates
        assert baseline.watermarks == before.watermarks
        with postgres.pipeline_connection() as connection:
            catalog_before = connection.execute(
                "SELECT count(*) FROM bronze_objects WHERE table_batch_id = %s",
                (f"{request.batch_id}__orders",),
            ).fetchone()[0]
        assert catalog_before == 0
        reconcile_orphan(postgres, storage, candidate, now=now)
        after = collect_state(postgres, storage, warehouse_path=None, marts=(), object_prefix=object_key.rsplit("/", 1)[0])
        with postgres.pipeline_connection() as connection:
            status = connection.execute("SELECT status FROM bronze_objects WHERE table_batch_id = %s", (f"{request.batch_id}__orders",)).fetchone()
            run_status = connection.execute("SELECT status FROM pipeline_runs WHERE pipeline_name = %s", (request.pipeline_name,)).fetchone()
        assert status == ("COMMITTED",)
        assert run_status == ("SUCCESS",)
        assert before.object_keys == after.object_keys
        assert object_key not in after.orphan_candidates
        assert baseline.watermarks != after.watermarks
        write_evidence("r03", {"batch_id": request.batch_id, "object_key": object_key, "orphan_before": before.orphan_candidates, "orphan_after": after.orphan_candidates, "watermarks_before": before.watermarks, "watermarks_after": after.watermarks, "status": status[0]})
    finally:
        _cleanup(postgres, storage, request.pipeline_name, request)


def test_r03_a_handled_commit_failure_refuses_automatic_reconciliation(monkeypatch, tmp_path) -> None:
    """처리된 Commit 실패는 FAILED Orphan으로 남고 자동 재조정을 거부하는지 검증한다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    now = datetime(2026, 9, 20, tzinfo=UTC)
    request = _orphan_request("r03-handled", now, page_size=2)
    _set_watermark(postgres, request.pipeline_name, _lower_bound_before_five_rows(postgres), now)
    object_key, _ = orders_object_keys(request.batch_id, now)
    baseline = collect_state(postgres, storage, warehouse_path=None, marts=(), object_prefix=object_key.rsplit("/", 1)[0])
    try:
        with fail_metadata_commit(monkeypatch), pytest.raises(RuntimeError) as error:
            ingest_orders(postgres, storage, request, local_directory=tmp_path, now=now)
        before = collect_state(postgres, storage, warehouse_path=None, marts=(), object_prefix=object_key.rsplit("/", 1)[0])
        candidate = next(item for item in find_orphan_candidates(postgres, storage) if item.object_key == object_key)
        with pytest.raises(
            OrphanReconciliationError,
            match="Only a crash-interrupted RUNNING pipeline run can be reconciled",
        ) as refusal:
            reconcile_orphan(postgres, storage, candidate, now=now)
        refused = collect_state(postgres, storage, warehouse_path=None, marts=(), object_prefix=object_key.rsplit("/", 1)[0])
        with postgres.pipeline_connection() as connection:
            run = connection.execute("SELECT status, error_type FROM pipeline_runs WHERE pipeline_name = %s", (request.pipeline_name,)).fetchone()
        assert run == ("FAILED", UNKNOWN_ERROR)
        assert classify_error(error.value) == UNKNOWN_ERROR
        assert baseline.watermarks == before.watermarks == refused.watermarks
        assert before.orphan_candidates == refused.orphan_candidates
        with postgres.pipeline_connection() as connection:
            catalog = connection.execute("SELECT count(*) FROM bronze_objects WHERE table_batch_id = %s", (f"{request.batch_id}__orders",)).fetchone()[0]
        assert catalog == 0
        write_evidence("r03", {"batch_id": request.batch_id, "classified": classify_error(error.value), "exception": type(error.value).__name__, "injected_error": str(error.value), "object_key": object_key, "orphan_candidates": refused.orphan_candidates, "refusal": str(refusal.value), "stored_error_type": run[1], "watermarks": refused.watermarks})
    finally:
        _cleanup(postgres, storage, request.pipeline_name, request)




def test_r04_only_the_cas_winner_commits() -> None:
    """같은 Watermark를 경쟁하는 두 Run 중 승자만 Commit되고 패자는 롤백되는지 검증한다."""
    postgres = PostgresSettings.from_environment()
    now = datetime(2026, 9, 20, tzinfo=UTC)
    pipeline_name = f"r04-{uuid.uuid4().hex}"
    owner_id = uuid.uuid4()
    initial = get_or_create_watermark(postgres, pipeline_name, "orders", now=now)
    lease = acquire_table_lease(
        postgres,
        pipeline_name=pipeline_name,
        source_table="orders",
        owner_id=owner_id,
        now=now,
        ttl=timedelta(minutes=30),
    )
    winner = _competing_run(pipeline_name, "r04-winner", now)
    loser = _competing_run(pipeline_name, "r04-loser", now)
    try:
        record_started_run(postgres, winner, now=now)
        record_started_run(postgres, loser, now=now)
        winner_object = _competing_object(winner, _after_cursor(now, "order-winner"))
        loser_object = _competing_object(loser, _after_cursor(now, "order-loser"))

        commit_table_run(
            postgres,
            TableCommit(
                run=winner,
                object=winner_object,
                expected_watermark=initial,
                rows_extracted=1,
                rows_valid=1,
                rows_rejected=0,
                rows_loaded=1,
                lease_owner=lease.owner_id,
            ),
            now=now,
        )
        with pytest.raises(WatermarkConflictError) as conflict:
            commit_table_run(
                postgres,
                TableCommit(
                    run=loser,
                    object=loser_object,
                    expected_watermark=initial,
                    rows_extracted=1,
                    rows_valid=1,
                    rows_rejected=0,
                    rows_loaded=1,
                    lease_owner=lease.owner_id,
                ),
                now=now,
            )
        classified = classify_error(conflict.value)
        assert classified == WATERMARK_CONFLICT
        record_failed_run(postgres, loser, error_type=classified, now=now)

        with postgres.pipeline_connection() as connection:
            winner_object_status = connection.execute(
                "SELECT status FROM bronze_objects WHERE table_batch_id = %s",
                (winner_object.table_batch_id,),
            ).fetchone()
            loser_object_count = connection.execute(
                "SELECT count(*) FROM bronze_objects WHERE table_batch_id = %s",
                (loser_object.table_batch_id,),
            ).fetchone()[0]
            winner_run_status = connection.execute(
                "SELECT status FROM pipeline_runs WHERE run_id = %s AND source_table = 'orders'",
                (winner.run_id,),
            ).fetchone()
            loser_run_status = connection.execute(
                "SELECT status, error_type FROM pipeline_runs WHERE run_id = %s AND source_table = 'orders'",
                (loser.run_id,),
            ).fetchone()
            final_watermark = connection.execute(
                "SELECT watermark_timestamp, watermark_keys FROM watermarks"
                " WHERE pipeline_name = %s AND source_table = 'orders'",
                (pipeline_name,),
            ).fetchone()
        assert winner_object_status == ("COMMITTED",)
        assert loser_object_count == 0
        assert winner_run_status == ("SUCCESS",)
        assert loser_run_status == ("FAILED", classified)
        assert final_watermark[0] == winner_object.watermark_after.timestamp
        assert final_watermark[1] == list(winner_object.watermark_after.keys)
        write_evidence(
            "r04",
            {
                "pipeline_name": pipeline_name,
                "scope": "metadata layer only (commit_table_run); no S3 object was uploaded",
                "timeline": [
                    {"run_id": str(winner.run_id), "role": "winner", "result": "COMMITTED"},
                    {
                        "run_id": str(loser.run_id),
                        "role": "loser",
                        "result": classified,
                        "run_status": loser_run_status[0],
                    },
                ],
                "watermark_final": {
                    "timestamp": final_watermark[0].isoformat(),
                    "keys": final_watermark[1],
                },
                "winner_table_batch_id": winner_object.table_batch_id,
            },
        )
    finally:
        release_table_lease(postgres, lease, now=now + timedelta(minutes=1))
        _cleanup_metadata_only(postgres, pipeline_name, (winner, loser))


def test_r05_an_expired_lease_owner_cannot_commit() -> None:
    """만료된 Table Lease의 이전 소유자가 새 소유자 인수 뒤 Commit에서 Fencing되는지 검증한다."""
    postgres = PostgresSettings.from_environment()
    now = datetime(2026, 9, 20, tzinfo=UTC)
    pipeline_name = f"r05-{uuid.uuid4().hex}"
    stale_owner_id = uuid.uuid4()
    new_owner_id = uuid.uuid4()
    ttl = timedelta(minutes=30)
    acquired_at = now
    expires_at = acquired_at + ttl
    takeover_at = expires_at + timedelta(seconds=1)
    raw_run: PipelineRun | None = None
    stale_run: PipelineRun | None = None
    expiry_run: PipelineRun | None = None
    expiry_owner_id = uuid.uuid4()
    pipeline_name_expiry = f"r05-expiry-{uuid.uuid4().hex}"

    stale_lease = acquire_table_lease(
        postgres,
        pipeline_name=pipeline_name,
        source_table="orders",
        owner_id=stale_owner_id,
        now=acquired_at,
        ttl=ttl,
    )
    try:
        assert_table_lease(postgres, stale_lease, now=acquired_at + timedelta(minutes=1))

        new_lease = acquire_table_lease(
            postgres,
            pipeline_name=pipeline_name,
            source_table="orders",
            owner_id=new_owner_id,
            now=takeover_at,
            ttl=ttl,
        )
        try:
            assert new_lease.owner_id != stale_lease.owner_id
            with pytest.raises(TableLeaseOwnershipLostError):
                assert_table_lease(postgres, stale_lease, now=takeover_at + timedelta(seconds=1))

            stale_run = _competing_run(pipeline_name, "r05-stale", now)
            stale_watermark = get_or_create_watermark(postgres, pipeline_name, "orders", now=now)
            record_started_run(postgres, stale_run, now=now)
            stale_object = _competing_object(stale_run, _after_cursor(now, "order-stale"))
            with postgres.pipeline_connection() as connection:
                object_count_before = connection.execute(
                    "SELECT count(*) FROM bronze_objects WHERE table_batch_id = %s",
                    (stale_object.table_batch_id,),
                ).fetchone()[0]

            commit_attempted = False
            with pytest.raises(TableLeaseOwnershipLostError) as fenced:
                assert_table_lease(postgres, stale_lease, now=takeover_at + timedelta(minutes=1))
                commit_attempted = True
                commit_table_run(
                    postgres,
                    TableCommit(
                        run=stale_run,
                        object=stale_object,
                        expected_watermark=stale_watermark,
                        rows_extracted=1,
                        rows_valid=1,
                        rows_rejected=0,
                        rows_loaded=1,
                        lease_owner=stale_owner_id,
                    ),
                    now=now,
                )
            classified = classify_error(fenced.value)
            assert classified == LEASE_OWNERSHIP_LOST
            record_failed_run(postgres, stale_run, error_type=classified, now=now)

            with postgres.pipeline_connection() as connection:
                object_count_after = connection.execute(
                    "SELECT count(*) FROM bronze_objects WHERE table_batch_id = %s",
                    (stale_object.table_batch_id,),
                ).fetchone()[0]
                lease_row = connection.execute(
                    "SELECT lease_owner FROM watermarks WHERE pipeline_name = %s AND source_table = 'orders'",
                    (pipeline_name,),
                ).fetchone()
            assert not commit_attempted
            assert object_count_before == 0
            assert object_count_after == 0
            assert lease_row == (new_owner_id,)
            assert stale_watermark.version == new_lease.watermark_version

            raw_run = _competing_run(pipeline_name, "r05-stale-raw", now)
            record_started_run(postgres, raw_run, now=now)
            raw_object = _competing_object(raw_run, _after_cursor(now, "order-stale-raw"))
            with pytest.raises(TableLeaseOwnershipLostError) as raw_fenced:
                commit_table_run(
                    postgres,
                    TableCommit(
                        run=raw_run,
                        object=raw_object,
                        expected_watermark=stale_watermark,
                        rows_extracted=1,
                        rows_valid=1,
                        rows_rejected=0,
                        rows_loaded=1,
                        lease_owner=stale_owner_id,
                    ),
                    now=now,
                )
            raw_commit_outcome = "RAISED"
            raw_commit_classified = classify_error(raw_fenced.value)
            assert raw_commit_classified == LEASE_OWNERSHIP_LOST
            record_failed_run(postgres, raw_run, error_type=raw_commit_classified, now=now)
            with postgres.pipeline_connection() as connection:
                raw_object_count = connection.execute(
                    "SELECT count(*) FROM bronze_objects WHERE table_batch_id = %s",
                    (raw_object.table_batch_id,),
                ).fetchone()[0]
                raw_run_status = connection.execute(
                    "SELECT status FROM pipeline_runs WHERE run_id = %s AND source_table = 'orders'",
                    (raw_run.run_id,),
                ).fetchone()
                raw_watermark = connection.execute(
                    "SELECT watermark_timestamp, watermark_keys, version FROM watermarks"
                    " WHERE pipeline_name = %s AND source_table = 'orders'",
                    (pipeline_name,),
                ).fetchone()
            assert raw_object_count == 0
            assert raw_run_status == ("FAILED",)
            assert raw_watermark[2] == stale_watermark.version

            acquire_table_lease(
                postgres,
                pipeline_name=pipeline_name_expiry,
                source_table="orders",
                owner_id=expiry_owner_id,
                now=acquired_at,
                ttl=ttl,
            )
            expiry_watermark = get_or_create_watermark(
                postgres, pipeline_name_expiry, "orders", now=acquired_at
            )
            expiry_run = _competing_run(pipeline_name_expiry, "r05-expiry", now)
            record_started_run(postgres, expiry_run, now=now)
            expiry_object = _competing_object(expiry_run, _after_cursor(now, "order-expiry"))
            with pytest.raises(TableLeaseOwnershipLostError) as expiry_fenced:
                commit_table_run(
                    postgres,
                    TableCommit(
                        run=expiry_run,
                        object=expiry_object,
                        expected_watermark=expiry_watermark,
                        rows_extracted=1,
                        rows_valid=1,
                        rows_rejected=0,
                        rows_loaded=1,
                        lease_owner=expiry_owner_id,
                    ),
                    now=expires_at + timedelta(seconds=1),
                )
            expiry_classified = classify_error(expiry_fenced.value)
            assert expiry_classified == LEASE_OWNERSHIP_LOST
            record_failed_run(
                postgres, expiry_run, error_type=expiry_classified, now=expires_at + timedelta(seconds=1)
            )
            with postgres.pipeline_connection() as connection:
                expiry_object_count = connection.execute(
                    "SELECT count(*) FROM bronze_objects WHERE table_batch_id = %s",
                    (expiry_object.table_batch_id,),
                ).fetchone()[0]
                expiry_watermark_after = connection.execute(
                    "SELECT version FROM watermarks WHERE pipeline_name = %s AND source_table = 'orders'",
                    (pipeline_name_expiry,),
                ).fetchone()
            assert expiry_object_count == 0
            assert expiry_watermark_after[0] == expiry_watermark.version

            write_evidence(
                "r05",
                {
                    "pipeline_name": pipeline_name,
                    "scope": (
                        "fencing checked at the same assert_table_lease -> commit_table_run"
                        " boundary the service uses before a commit; assert_table_lease raised"
                        " first, so commit_table_run was never reached"
                    ),
                    "timeline": [
                        {"event": "acquire", "owner_id": str(stale_owner_id), "at": acquired_at.isoformat()},
                        {"event": "expire", "at": expires_at.isoformat()},
                        {"event": "takeover", "owner_id": str(new_owner_id), "at": takeover_at.isoformat()},
                        {
                            "event": "commit_attempt_fenced",
                            "owner_id": str(stale_owner_id),
                            "result": classified,
                            "commit_table_run_reached": commit_attempted,
                            "stale_run_status": "FAILED",
                        },
                        {
                            "event": "commit_without_lease_check",
                            "owner_id": str(stale_owner_id),
                            "note": (
                                "commit_table_run called directly with the stale owner's"
                                " lease, bypassing assert_table_lease, to confirm"
                                " commit_table_run's own FOR UPDATE lease check (ADR 017)"
                                " now closes the gap"
                            ),
                            "outcome": raw_commit_outcome,
                            "classified_error": raw_commit_classified,
                            "bronze_objects_count": raw_object_count,
                            "run_status": raw_run_status[0] if raw_run_status else None,
                            "watermark_version_after": raw_watermark[2] if raw_watermark else None,
                        },
                        {
                            "event": "commit_after_expiry_no_takeover",
                            "owner_id": str(expiry_owner_id),
                            "note": (
                                "same owner, no takeover; lease simply expired and the owner"
                                " tried to commit past lease_expires_at, exercising the"
                                " lease_expires_at <= current_time branch that owner mismatch"
                                " alone cannot reach"
                            ),
                            "outcome": "RAISED",
                            "classified_error": expiry_classified,
                            "bronze_objects_count": expiry_object_count,
                            "watermark_version_after": expiry_watermark_after[0]
                            if expiry_watermark_after
                            else None,
                        },
                    ],
                },
            )
        finally:
            release_table_lease(postgres, new_lease, now=takeover_at + timedelta(minutes=1))
    finally:
        _cleanup_metadata_only(
            postgres,
            pipeline_name_expiry,
            tuple(run for run in (expiry_run,) if run is not None),
        )
        _cleanup_metadata_only(
            postgres, pipeline_name, tuple(run for run in (stale_run, raw_run) if run is not None)
        )


def _competing_run(pipeline_name: str, batch_prefix: str, now: datetime) -> PipelineRun:
    """R-04/R-05 동시성 시나리오에 쓸 초기 Watermark 기준 Run을 반환한다."""
    return PipelineRun(
        run_id=uuid.uuid4(),
        pipeline_name=pipeline_name,
        source_table="orders",
        batch_id=f"{batch_prefix}-{uuid.uuid4().hex}",
        logical_date=now,
        attempt_number=1,
        watermark_before=CursorPosition(None),
        extract_upper_bound=_after_cursor(now, "order-0001"),
    )


def _after_cursor(now: datetime, key: str) -> CursorPosition:
    """초기 Watermark를 전진시키는 단일 Key Composite Cursor를 반환한다."""
    return CursorPosition(now + timedelta(seconds=1), (key,))


def _competing_object(run: PipelineRun, watermark_after: CursorPosition) -> VerifiedBronzeObject:
    """실제 업로드 없이 Commit 계약만 검증할 고유 Bronze Object 증적을 반환한다."""
    table_batch_id = f"{run.batch_id}__{run.source_table}"
    logical_rows = {"run_id": str(run.run_id), "orders": [watermark_after.keys[0]]}
    return VerifiedBronzeObject(
        table_batch_id=table_batch_id,
        object_key=f"bronze/orders/batch_id={run.batch_id}/data.parquet",
        manifest_key=f"bronze/orders/batch_id={run.batch_id}/manifest.json",
        schema_version=2,
        row_count=1,
        content_sha256="a" * 64,
        logical_hash=logical_hash(logical_rows),
        watermark_after=watermark_after,
    )


def _cleanup_metadata_only(
    postgres: PostgresSettings, pipeline_name: str, runs: tuple[PipelineRun, ...]
) -> None:
    """R-04/R-05가 만든 Metadata 행만 Object Storage 접근 없이 제거한다."""
    with postgres.pipeline_connection() as connection:
        for run in runs:
            connection.execute(
                "DELETE FROM bronze_objects WHERE table_batch_id = %s",
                (f"{run.batch_id}__{run.source_table}",),
            )
        connection.execute("DELETE FROM pipeline_runs WHERE pipeline_name = %s", (pipeline_name,))
        connection.execute("DELETE FROM watermarks WHERE pipeline_name = %s", (pipeline_name,))
        connection.commit()


def _lower_bound_before_five_rows(settings: PostgresSettings) -> CursorPosition:
    """다섯 건을 수집할 수 있는 직전 orders Composite Cursor를 읽는다."""
    with settings.source_connection() as connection:
        row = connection.execute(
            'SELECT updated_at, order_id FROM orders ORDER BY updated_at DESC, order_id COLLATE "C" DESC OFFSET 5 LIMIT 1'
        ).fetchone()
    if row is None:
        raise RuntimeError("The seeded source must contain at least six orders")
    return CursorPosition(row[0], (row[1],))


def _lower_bound_before_twenty_rows(settings: PostgresSettings) -> CursorPosition:
    """Quarantine Object를 만들 최신 스무 건 직전 Cursor를 읽는다."""
    with settings.source_connection() as connection:
        row = connection.execute(
            'SELECT updated_at, order_id FROM orders ORDER BY updated_at DESC, order_id COLLATE "C" DESC OFFSET 20 LIMIT 1'
        ).fetchone()
    if row is None:
        raise RuntimeError("The seeded source must contain at least 21 orders")
    return CursorPosition(row[0], (row[1],))


def _orphan_request(
    prefix: str, now: datetime, **kwargs: object
) -> OrdersIngestionRequest:
    """시나리오 전용 표준 Batch 요청을 만든다."""
    identifier = f"{prefix}-{uuid.uuid4().hex}"
    return OrdersIngestionRequest.for_dag_run(
        dag_id=identifier, logical_date=now, pipeline_name=identifier, **kwargs
    )


def _set_watermark(
    settings: PostgresSettings, pipeline_name: str, cursor: CursorPosition, now: datetime
) -> None:
    """테스트 Pipeline의 Watermark를 대상 범위 직전으로 설정한다."""
    get_or_create_watermark(settings, pipeline_name, "orders", now=now)
    with settings.pipeline_connection() as connection:
        connection.execute(
            "UPDATE watermarks SET watermark_timestamp = %s, watermark_keys = %s WHERE pipeline_name = %s AND source_table = 'orders'",
            (cursor.timestamp, Jsonb(cursor.as_json()), pipeline_name),
        )
        connection.commit()


def _cleanup(
    postgres: PostgresSettings, storage: SeaweedFSSettings, pipeline_name: str, request: OrdersIngestionRequest
) -> None:
    """이 시나리오의 Metadata와 Final Object를 역순으로 제거한다."""
    object_key, manifest_key = orders_object_keys(request.batch_id, request.logical_date)
    quarantine_key, quarantine_manifest_key = quarantine_object_keys(
        "orders", request.batch_id, request.logical_date
    )
    client = seaweedfs_s3_client(storage)
    for key in (manifest_key, object_key, quarantine_manifest_key, quarantine_key):
        client.delete_object(Bucket=storage.bucket, Key=key)
    with postgres.pipeline_connection() as connection:
        connection.execute("DELETE FROM bronze_objects WHERE table_batch_id = %s", (f"{request.batch_id}__orders",))
        connection.execute("DELETE FROM pipeline_runs WHERE pipeline_name = %s", (pipeline_name,))
        connection.execute("DELETE FROM watermarks WHERE pipeline_name = %s", (pipeline_name,))
        connection.commit()
