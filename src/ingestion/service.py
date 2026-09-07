"""`orders` Bronze Commit Protocol을 한 번의 Framework-independent 실행으로 연결한다."""

from __future__ import annotations

import uuid
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from src.common.database import PostgresSettings
from src.ingestion.batch import (
    BatchIdentity,
    BatchIdentityConflictError,
    CommittedTableBatch,
    TableBatchIdentity,
    assert_reusable_table_batch,
    get_committed_table_batch,
)
from src.ingestion.bronze import (
    BRONZE_SCHEMA_VERSION,
    BronzeWriteContext,
    OrdersBronzeWriter,
    orders_logical_hash,
)
from src.ingestion.manifest import BronzeManifest
from src.ingestion.metadata import (
    PipelineRun,
    TableCommit,
    VerifiedBronzeObject,
    commit_table_run,
    get_or_create_watermark,
    record_failed_run,
    record_skipped_already_committed_run,
    record_started_run,
    record_success_no_data_run,
)
from src.ingestion.orders import open_orders_snapshot
from src.ingestion.storage import (
    BRONZE_PREFIX,
    SeaweedFSSettings,
    ensure_bucket,
    upload_new_bytes,
    upload_new_file,
    verify_parquet_object,
)

ORDERS_PIPELINE_NAME = "orders_bronze"
ORDERS_SOURCE_TABLE = "orders"


@dataclass(frozen=True)
class OrdersIngestionRequest:
    """하나의 `orders` 수집 시도에 외부 실행기가 부여하는 식별자와 범위 설정이다."""

    batch_id: str
    logical_date: datetime
    attempt_number: int = 1
    run_id: uuid.UUID | None = None
    pipeline_name: str = ORDERS_PIPELINE_NAME
    page_size: int | None = None

    def __post_init__(self) -> None:
        """Batch·Pipeline 이름, UTC Logical Date와 선택적 Page Size를 확인한다."""
        if not self.batch_id.strip() or not self.pipeline_name.strip():
            raise ValueError("batch_id and pipeline_name must not be empty")
        if self.attempt_number <= 0:
            raise ValueError("attempt_number must be greater than zero")
        _assert_utc(self.logical_date, "logical_date")
        if self.page_size is not None and self.page_size <= 0:
            raise ValueError("page_size must be greater than zero")

    @classmethod
    def for_dag_run(
        cls,
        *,
        dag_id: str,
        logical_date: datetime,
        attempt_number: int = 1,
        run_id: uuid.UUID | None = None,
        pipeline_name: str = ORDERS_PIPELINE_NAME,
        page_size: int | None = None,
    ) -> OrdersIngestionRequest:
        """DAG ID와 Logical Date로 표준 Batch ID를 만든 `orders` 실행 요청을 반환한다."""
        identity = BatchIdentity(dag_id=dag_id, logical_date=logical_date)
        return cls(
            batch_id=identity.batch_id,
            logical_date=logical_date,
            attempt_number=attempt_number,
            run_id=run_id,
            pipeline_name=pipeline_name,
            page_size=page_size,
        )


@dataclass(frozen=True)
class OrdersIngestionResult:
    """실행 상태와 성공 시 Catalog에 Commit된 Object 증적을 반환한다."""

    run: PipelineRun
    status: str
    object_key: str | None = None
    manifest_key: str | None = None
    row_count: int = 0


def ingest_orders(
    postgres: PostgresSettings,
    storage: SeaweedFSSettings,
    request: OrdersIngestionRequest,
    *,
    local_directory: Path,
    now: datetime | None = None,
) -> OrdersIngestionResult:
    """고정 `orders` 범위를 Local Parquet·VERIFIED Manifest·CAS Commit까지 처리한다."""
    current_time = _utc_now(now)
    ensure_bucket(storage)
    watermark = get_or_create_watermark(
        postgres, request.pipeline_name, ORDERS_SOURCE_TABLE, now=current_time
    )
    identity = _orders_table_batch_identity(request)
    existing = get_committed_table_batch(postgres, identity)
    if existing is not None:
        return _reuse_or_reject_committed_batch(
            postgres, request, watermark, existing, current_time
        )

    with open_orders_snapshot(postgres, watermark.cursor, page_size=request.page_size) as snapshot:
        run = PipelineRun(
            run_id=request.run_id or uuid.uuid4(),
            pipeline_name=request.pipeline_name,
            source_table=ORDERS_SOURCE_TABLE,
            batch_id=request.batch_id,
            logical_date=request.logical_date,
            attempt_number=request.attempt_number,
            watermark_before=watermark.cursor,
            extract_upper_bound=snapshot.extract_upper_bound,
        )
        record_started_run(postgres, run, now=current_time)
        if snapshot.extract_upper_bound is None:
            record_success_no_data_run(postgres, run, now=current_time)
            return OrdersIngestionResult(run=run, status="SUCCESS_NO_DATA")

        try:
            artifact = _write_local_parquet(snapshot, request, run, local_directory, current_time)
            verified_object = _upload_and_verify(
                storage,
                request,
                run,
                artifact.path,
                artifact.row_count,
                current_time,
            )
            commit_table_run(
                postgres,
                TableCommit(
                    run=run,
                    object=verified_object,
                    expected_watermark=watermark,
                    rows_extracted=artifact.row_count,
                    rows_valid=artifact.row_count,
                    rows_rejected=0,
                    rows_loaded=artifact.row_count,
                ),
                now=current_time,
            )
        except Exception as error:
            _record_failure_without_masking(postgres, run, error, current_time)
            raise

    return OrdersIngestionResult(
        run=run,
        status="SUCCESS",
        object_key=verified_object.object_key,
        manifest_key=verified_object.manifest_key,
        row_count=verified_object.row_count,
    )


def orders_object_keys(batch_id: str, logical_date: datetime) -> tuple[str, str]:
    """불변 `orders` Final Parquet와 Manifest의 Bronze Key를 반환한다."""
    if not batch_id.strip():
        raise ValueError("batch_id must not be empty")
    _assert_utc(logical_date, "logical_date")
    prefix = (
        f"{BRONZE_PREFIX}/{ORDERS_SOURCE_TABLE}/ingestion_date={logical_date.date().isoformat()}"
        f"/batch_id={batch_id}"
    )
    return f"{prefix}/data.parquet", f"{prefix}/manifest.json"


def _orders_table_batch_identity(request: OrdersIngestionRequest) -> TableBatchIdentity:
    """외부 Batch ID를 표준 Table Batch Identity와 같은 계약으로 검증해 반환한다."""
    standard = BatchIdentity(
        dag_id=_dag_id_from_batch_id(request.batch_id), logical_date=request.logical_date
    )
    identity = standard.table_batch(ORDERS_SOURCE_TABLE)
    if identity.batch_id != request.batch_id:
        raise ValueError("batch_id must match '{dag_id}__{logical_date_utc:%Y%m%dT%H%M%SZ}'")
    return identity


def _dag_id_from_batch_id(batch_id: str) -> str:
    """표준 Batch ID에서 마지막 구분자 앞의 DAG ID를 안전하게 분리한다."""
    dag_id, separator, timestamp = batch_id.rpartition("__")
    if not separator or len(timestamp) != 16:
        raise ValueError("batch_id must use the standard DAG logical-date format")
    return dag_id


def _reuse_or_reject_committed_batch(
    postgres: PostgresSettings,
    request: OrdersIngestionRequest,
    watermark,
    existing: CommittedTableBatch,
    current_time: datetime,
) -> OrdersIngestionResult:
    """기존 Commit 범위가 같으면 Skip하고 다르면 Source Read 전 Conflict로 종료한다."""
    try:
        assert_reusable_table_batch(
            existing,
            current_watermark=watermark,
            schema_version=BRONZE_SCHEMA_VERSION,
        )
    except BatchIdentityConflictError as error:
        run = PipelineRun(
            run_id=request.run_id or uuid.uuid4(),
            pipeline_name=request.pipeline_name,
            source_table=ORDERS_SOURCE_TABLE,
            batch_id=request.batch_id,
            logical_date=request.logical_date,
            attempt_number=request.attempt_number,
            watermark_before=watermark.cursor,
            extract_upper_bound=None,
        )
        record_started_run(postgres, run, now=current_time)
        record_failed_run(
            postgres,
            run,
            error_type="BATCH_IDENTITY_CONFLICT",
            error_message=str(error),
            now=current_time,
        )
        raise
    run = PipelineRun(
        run_id=request.run_id or uuid.uuid4(),
        pipeline_name=request.pipeline_name,
        source_table=ORDERS_SOURCE_TABLE,
        batch_id=request.batch_id,
        logical_date=request.logical_date,
        attempt_number=request.attempt_number,
        watermark_before=existing.watermark_before,
        extract_upper_bound=existing.watermark_after,
    )
    record_started_run(postgres, run, now=current_time)
    record_skipped_already_committed_run(
        postgres, run, row_count=existing.row_count, now=current_time
    )
    return OrdersIngestionResult(
        run=run,
        status="SKIPPED_ALREADY_COMMITTED",
        object_key=existing.object_key,
        manifest_key=existing.manifest_key,
        row_count=existing.row_count,
    )


def _write_local_parquet(
    snapshot: object,
    request: OrdersIngestionRequest,
    run: PipelineRun,
    local_directory: Path,
    current_time: datetime,
):
    """같은 Source Snapshot의 모든 Page를 Run 고유 Local Parquet으로 기록한다."""
    output_path = local_directory / ORDERS_SOURCE_TABLE / f"{run.run_id}.parquet"
    writer = OrdersBronzeWriter(
        output_path,
        BronzeWriteContext(batch_id=request.batch_id, run_id=run.run_id, ingested_at=current_time),
    )
    for page in snapshot.pages():
        writer.write_page(page)
    return writer.close()


def _upload_and_verify(
    storage: SeaweedFSSettings,
    request: OrdersIngestionRequest,
    run: PipelineRun,
    parquet_path: Path,
    expected_row_count: int,
    current_time: datetime,
) -> VerifiedBronzeObject:
    """Final Parquet·VERIFIED Manifest를 업로드하고 Byte·Row Count를 다시 검증한다."""
    if run.extract_upper_bound is None:
        raise RuntimeError("Non-empty uploads require an extract upper bound")
    object_key, manifest_key = orders_object_keys(request.batch_id, request.logical_date)
    uploaded = upload_new_file(storage, object_key, parquet_path)
    verified = verify_parquet_object(storage, uploaded)
    if verified.row_count != expected_row_count:
        raise RuntimeError("Final Parquet row count differs from the local artifact")
    logical_hash = orders_logical_hash(parquet_path)
    manifest = BronzeManifest(
        batch_id=request.batch_id,
        run_id=run.run_id,
        source_table=run.source_table,
        logical_date=run.logical_date,
        watermark_before=run.watermark_before,
        extract_upper_bound=run.extract_upper_bound,
        object_key=object_key,
        object_size=verified.size,
        content_sha256=verified.content_sha256,
        logical_hash=logical_hash,
        row_count=verified.row_count,
        created_at=current_time,
        schema_version=BRONZE_SCHEMA_VERSION,
    )
    upload_new_bytes(storage, manifest_key, manifest.to_bytes())
    return VerifiedBronzeObject(
        table_batch_id=f"{request.batch_id}__{run.source_table}",
        object_key=object_key,
        manifest_key=manifest_key,
        schema_version=BRONZE_SCHEMA_VERSION,
        row_count=verified.row_count,
        content_sha256=verified.content_sha256,
        logical_hash=logical_hash,
        watermark_after=run.extract_upper_bound,
    )


def _record_failure_without_masking(
    postgres: PostgresSettings, run: PipelineRun, error: Exception, current_time: datetime
) -> None:
    """실패 원인을 남기되 이미 발생한 실행 오류를 Metadata 오류로 가리지 않는다."""
    with suppress(Exception):
        record_failed_run(
            postgres,
            run,
            error_type=type(error).__name__[:64],
            error_message=str(error) or None,
            now=current_time,
        )


def _assert_utc(value: datetime, name: str) -> None:
    """요청과 Key 구성에 쓰는 Logical Date가 UTC인지 확인한다."""
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be normalized to UTC")


def _utc_now(value: datetime | None) -> datetime:
    """주입된 UTC 시각 또는 현재 UTC 시각을 실행 기준 시각으로 반환한다."""
    if value is None:
        return datetime.now(UTC)
    _assert_utc(value, "now")
    return value
