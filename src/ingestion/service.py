"""- 6개 Source Table의 검증·Quarantine·Bronze Commit Protocol을 연결한다."""

from __future__ import annotations

import uuid
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from src.common.database import PostgresSettings
from src.generator.lease import (
    WAREHOUSE_OWNER_TYPE,
    SourceMutationLease,
    acquire_source_mutation_lease,
    assert_source_mutation_lease,
    release_source_mutation_lease,
)
from src.ingestion.batch import (
    BatchIdentity,
    BatchIdentityConflictError,
    CommittedTableBatch,
    TableBatchIdentity,
    assert_reusable_table_batch,
    get_committed_table_batch,
)
from src.ingestion.bronze import (
    BronzeWriteContext,
    LocalParquetArtifact,
    TableBronzeWriter,
    table_logical_hash,
)
from src.ingestion.corruption import CorruptionPlan
from src.ingestion.extract import SourcePage, open_table_snapshot
from src.ingestion.lease import (
    TableLease,
    acquire_table_lease,
    assert_table_lease,
    release_table_lease,
)
from src.ingestion.manifest import BronzeManifest, QuarantineManifest
from src.ingestion.metadata import (
    PipelineRun,
    QuarantineBatch,
    TableCommit,
    VerifiedBronzeObject,
    commit_table_run,
    get_or_create_watermark,
    record_failed_run,
    record_skipped_already_committed_run,
    record_started_run,
    record_success_no_data_run,
)
from src.ingestion.quarantine import (
    LocalQuarantineArtifact,
    QuarantineWriteContext,
    QuarantineWriter,
    assert_reject_rate,
)
from src.ingestion.references import find_broken_parent_references
from src.ingestion.storage import (
    BRONZE_PREFIX,
    QUARANTINE_PREFIX,
    SeaweedFSSettings,
    ensure_bucket,
    upload_new_bytes,
    upload_new_file,
    verify_parquet_object,
)
from src.ingestion.tables import TableConfig, table_config
from src.ingestion.validation import ValidationPipeline

ORDERS_PIPELINE_NAME = "orders_bronze"
ORDERS_SOURCE_TABLE = "orders"


@dataclass(frozen=True)
class TableIngestionRequest:
    """- 한 Source Table 수집 시도의 외부 식별자·범위 설정을 보관한다."""

    source_table: str
    batch_id: str
    logical_date: datetime
    attempt_number: int = 1
    run_id: uuid.UUID | None = None
    pipeline_name: str | None = None
    page_size: int | None = None
    corruption_plan: CorruptionPlan | None = None

    def __post_init__(self) -> None:
        """- 등록 Table·Batch·UTC Logical Date·선택 Page Size를 검증한다."""
        table_config(self.source_table)
        if not self.batch_id.strip():
            raise ValueError("batch_id must not be empty")
        if self.pipeline_name is not None and not self.pipeline_name.strip():
            raise ValueError("pipeline_name must not be empty")
        if self.attempt_number <= 0:
            raise ValueError("attempt_number must be greater than zero")
        _assert_utc(self.logical_date, "logical_date")
        if self.page_size is not None and self.page_size <= 0:
            raise ValueError("page_size must be greater than zero")

    @property
    def resolved_pipeline_name(self) -> str:
        """- 명시 이름 또는 Table별 기본 Bronze Pipeline 이름을 반환한다."""
        return self.pipeline_name or f"{self.source_table}_bronze"

    @classmethod
    def for_dag_run(
        cls,
        *,
        source_table: str,
        dag_id: str,
        logical_date: datetime,
        attempt_number: int = 1,
        run_id: uuid.UUID | None = None,
        pipeline_name: str | None = None,
        page_size: int | None = None,
        corruption_plan: CorruptionPlan | None = None,
    ) -> TableIngestionRequest:
        """- DAG ID와 Logical Date로 표준 Batch ID의 Table 실행 요청을 만든다."""
        return cls(
            source_table=source_table,
            batch_id=BatchIdentity(dag_id=dag_id, logical_date=logical_date).batch_id,
            logical_date=logical_date,
            attempt_number=attempt_number,
            run_id=run_id,
            pipeline_name=pipeline_name,
            page_size=page_size,
            corruption_plan=corruption_plan,
        )


@dataclass(frozen=True)
class OrdersIngestionRequest:
    """- 기존 호출부를 위한 `orders` 전용 요청 호환 계약이다."""

    batch_id: str
    logical_date: datetime
    attempt_number: int = 1
    run_id: uuid.UUID | None = None
    pipeline_name: str = ORDERS_PIPELINE_NAME
    page_size: int | None = None
    corruption_plan: CorruptionPlan | None = None

    def __post_init__(self) -> None:
        """- `orders` 공통 요청으로 변환 가능한 입력인지 검증한다."""
        self.as_table_request()

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
        corruption_plan: CorruptionPlan | None = None,
    ) -> OrdersIngestionRequest:
        """- 기존 DAG 호출을 위한 표준 `orders` Batch 요청을 만든다."""
        return cls(
            batch_id=BatchIdentity(dag_id=dag_id, logical_date=logical_date).batch_id,
            logical_date=logical_date,
            attempt_number=attempt_number,
            run_id=run_id,
            pipeline_name=pipeline_name,
            page_size=page_size,
            corruption_plan=corruption_plan,
        )

    def as_table_request(self) -> TableIngestionRequest:
        """- 현재 값을 공통 `orders` Table 요청으로 변환한다."""
        return TableIngestionRequest(
            source_table=ORDERS_SOURCE_TABLE,
            batch_id=self.batch_id,
            logical_date=self.logical_date,
            attempt_number=self.attempt_number,
            run_id=self.run_id,
            pipeline_name=self.pipeline_name,
            page_size=self.page_size,
            corruption_plan=self.corruption_plan,
        )


@dataclass(frozen=True)
class TableIngestionResult:
    """- 실행 상태·Commit된 Bronze 증적과 Count를 반환한다."""

    run: PipelineRun
    status: str
    object_key: str | None = None
    manifest_key: str | None = None
    row_count: int = 0
    rows_rejected: int = 0
    rows_corrupted: int = 0
    quarantine_object_key: str | None = None


OrdersIngestionResult = TableIngestionResult


def ingest_table(
    postgres: PostgresSettings,
    storage: SeaweedFSSettings,
    request: TableIngestionRequest,
    *,
    local_directory: Path,
    now: datetime | None = None,
    source_lease: SourceMutationLease | None = None,
) -> TableIngestionResult:
    """- 6개 Table의 고정 범위를 검증·격리·Bronze Commit까지 처리한다."""
    current_time = _utc_now(now)
    config = table_config(request.source_table)
    pipeline_name = request.resolved_pipeline_name
    run_id = request.run_id or uuid.uuid4()
    ensure_bucket(storage)
    watermark = get_or_create_watermark(postgres, pipeline_name, config.source_table, now=current_time)
    identity = _table_batch_identity(request, config)
    existing = get_committed_table_batch(postgres, identity)
    if existing is not None:
        return _reuse_or_reject_committed_batch(postgres, request, config, watermark, existing, current_time)

    owned_source_lease = source_lease is None
    active_source_lease = source_lease or acquire_source_mutation_lease(
        postgres, owner_type=WAREHOUSE_OWNER_TYPE, owner_id=run_id, now=current_time
    )
    table_lease: TableLease | None = None
    try:
        assert_source_mutation_lease(postgres, active_source_lease, now=current_time)
        table_lease = acquire_table_lease(
            postgres,
            pipeline_name=pipeline_name,
            source_table=config.source_table,
            owner_id=run_id,
            now=current_time,
        )
    except Exception as error:
        _record_lease_failure(postgres, request, config, watermark, run_id, error, current_time)
        if owned_source_lease:
            with suppress(Exception):
                release_source_mutation_lease(postgres, active_source_lease, now=current_time)
        raise

    try:
      with open_table_snapshot(postgres, config, watermark.cursor, page_size=request.page_size) as snapshot:
        run = PipelineRun(
            run_id=run_id,
            pipeline_name=pipeline_name,
            source_table=config.source_table,
            batch_id=request.batch_id,
            logical_date=request.logical_date,
            attempt_number=request.attempt_number,
            watermark_before=watermark.cursor,
            extract_upper_bound=snapshot.extract_upper_bound,
        )
        record_started_run(postgres, run, now=current_time)
        if snapshot.extract_upper_bound is None:
            record_success_no_data_run(postgres, run, now=current_time)
            return TableIngestionResult(run=run, status="SUCCESS_NO_DATA")

        try:
            (
                bronze_artifact,
                quarantine_artifact,
                rows_extracted,
                rows_rejected,
                rows_corrupted,
            ) = _write_local_artifacts(snapshot, request, run, config, local_directory, current_time)
            assert_source_mutation_lease(postgres, active_source_lease, now=current_time)
            assert_table_lease(postgres, table_lease, now=current_time)
            assert_reject_rate(total_rows=rows_extracted, rejected_rows=rows_rejected)
            verified_object = _upload_and_verify_bronze(
                storage, request, run, config, bronze_artifact, current_time
            )
            quarantine_batch = _publish_quarantine(
                storage, request, run, config, quarantine_artifact, current_time
            )
            assert_source_mutation_lease(postgres, active_source_lease, now=current_time)
            assert_table_lease(postgres, table_lease, now=current_time)
            commit_table_run(
                postgres,
                TableCommit(
                    run=run,
                    object=verified_object,
                    expected_watermark=watermark,
                    rows_extracted=rows_extracted,
                    rows_valid=bronze_artifact.row_count,
                    rows_rejected=rows_rejected,
                    rows_loaded=bronze_artifact.row_count,
                    quarantine=quarantine_batch,
                ),
                now=current_time,
            )
        except Exception as error:
            _record_failure_without_masking(postgres, run, error, current_time)
            raise

      return TableIngestionResult(
          run=run,
          status="SUCCESS",
          object_key=verified_object.object_key,
          manifest_key=verified_object.manifest_key,
          row_count=verified_object.row_count,
          rows_rejected=rows_rejected,
          rows_corrupted=rows_corrupted,
          quarantine_object_key=quarantine_batch.object_key if quarantine_batch is not None else None,
      )
    finally:
        if table_lease is not None:
            with suppress(Exception):
                release_table_lease(postgres, table_lease, now=current_time)
        if owned_source_lease:
            with suppress(Exception):
                release_source_mutation_lease(postgres, active_source_lease, now=current_time)


def ingest_orders(
    postgres: PostgresSettings,
    storage: SeaweedFSSettings,
    request: OrdersIngestionRequest,
    *,
    local_directory: Path,
    now: datetime | None = None,
) -> OrdersIngestionResult:
    """- 기존 `orders` API를 공통 6개 Table Commit 서비스로 위임한다."""
    return ingest_table(postgres, storage, request.as_table_request(), local_directory=local_directory, now=now)


def table_object_keys(source_table: str, batch_id: str, logical_date: datetime) -> tuple[str, str]:
    """- 지정 Table의 불변 Bronze Parquet와 Manifest Key를 반환한다."""
    table_config(source_table)
    _assert_nonempty_batch_id(batch_id)
    _assert_utc(logical_date, "logical_date")
    prefix = f"{BRONZE_PREFIX}/{source_table}/ingestion_date={logical_date.date().isoformat()}/batch_id={batch_id}"
    return f"{prefix}/data.parquet", f"{prefix}/manifest.json"


def orders_object_keys(batch_id: str, logical_date: datetime) -> tuple[str, str]:
    """- 기존 호출부를 위해 `orders` Bronze Key를 반환한다."""
    return table_object_keys(ORDERS_SOURCE_TABLE, batch_id, logical_date)


def quarantine_object_keys(source_table: str, batch_id: str, logical_date: datetime) -> tuple[str, str]:
    """- 지정 Table의 불변 Quarantine Parquet와 Manifest Key를 반환한다."""
    table_config(source_table)
    _assert_nonempty_batch_id(batch_id)
    _assert_utc(logical_date, "logical_date")
    prefix = f"{QUARANTINE_PREFIX}/{source_table}/ingestion_date={logical_date.date().isoformat()}/batch_id={batch_id}"
    return f"{prefix}/records.parquet", f"{prefix}/manifest.json"


def _write_local_artifacts(
    snapshot,
    request: TableIngestionRequest,
    run: PipelineRun,
    config: TableConfig,
    local_directory: Path,
    current_time: datetime,
) -> tuple[LocalParquetArtifact, LocalQuarantineArtifact | None, int, int, int]:
    """- Snapshot Page를 검증해 Valid Bronze와 Reject Quarantine Local 파일로 분리한다."""
    bronze_writer = TableBronzeWriter(
        local_directory / config.source_table / f"{run.run_id}.parquet",
        config,
        BronzeWriteContext(request.batch_id, run.run_id, current_time, config.source_table),
    )
    quarantine_writer: QuarantineWriter | None = None
    validation = ValidationPipeline(config, snapshot.watermark_before, snapshot.extract_upper_bound)
    rows_extracted = 0
    rows_rejected = 0
    rows_corrupted = 0
    try:
        for page in snapshot.pages():
            records = tuple(
                request.corruption_plan.apply(record, rows_extracted + index)
                if request.corruption_plan is not None
                else record
                for index, record in enumerate(page.records)
            )
            if request.corruption_plan is not None:
                rows_corrupted += sum(
                    request.corruption_plan.rules.get(rows_extracted + index) is not None
                    for index in range(len(page.records))
                )
            rows_extracted += len(page.records)
            validation_page = SourcePage(
                records=records,
                lower_bound=page.lower_bound,
                extract_upper_bound=page.extract_upper_bound,
            )
            broken_cursors = frozenset(
                item.child_cursor
                for item in find_broken_parent_references(snapshot, validation_page)
            )
            validated = validation.validate_page(
                validation_page, broken_reference_cursors=broken_cursors
            )
            bronze_writer.write_records(validated.valid_records)
            if validated.rejected_records:
                if quarantine_writer is None:
                    quarantine_writer = QuarantineWriter(
                        local_directory / config.source_table / f"{run.run_id}.quarantine.parquet",
                        QuarantineWriteContext(request.batch_id, run.run_id, current_time, config.source_table),
                    )
                quarantine_writer.write_rejected_records(validated.rejected_records)
                rows_rejected += len(validated.rejected_records)
        return (
            bronze_writer.close(),
            quarantine_writer.close() if quarantine_writer else None,
            rows_extracted,
            rows_rejected,
            rows_corrupted,
        )
    except Exception:
        with suppress(Exception):
            bronze_writer.close()
        if quarantine_writer is not None:
            with suppress(Exception):
                quarantine_writer.close()
        raise


def _upload_and_verify_bronze(storage: SeaweedFSSettings, request: TableIngestionRequest, run: PipelineRun, config: TableConfig, artifact: LocalParquetArtifact, current_time: datetime) -> VerifiedBronzeObject:
    """- Final Bronze·VERIFIED Manifest를 업로드하고 Byte·Row Count를 재검증한다."""
    if run.extract_upper_bound is None:
        raise RuntimeError("Non-empty uploads require an extract upper bound")
    object_key, manifest_key = table_object_keys(config.source_table, request.batch_id, request.logical_date)
    verified = verify_parquet_object(storage, upload_new_file(storage, object_key, artifact.path))
    if verified.row_count != artifact.row_count:
        raise RuntimeError("Final Parquet row count differs from the local artifact")
    logical_hash = table_logical_hash(artifact.path, config)
    manifest = BronzeManifest(request.batch_id, run.run_id, config.source_table, run.logical_date, run.watermark_before, run.extract_upper_bound, object_key, verified.size, verified.content_sha256, logical_hash, verified.row_count, current_time, config.schema_version)
    upload_new_bytes(storage, manifest_key, manifest.to_bytes())
    return VerifiedBronzeObject(f"{request.batch_id}__{config.source_table}", object_key, manifest_key, config.schema_version, verified.row_count, verified.content_sha256, logical_hash, run.extract_upper_bound)


def _publish_quarantine(
    storage: SeaweedFSSettings,
    request: TableIngestionRequest,
    run: PipelineRun,
    config: TableConfig,
    artifact: LocalQuarantineArtifact | None,
    current_time: datetime,
) -> QuarantineBatch | None:
    """- Reject Parquet·VERIFIED Manifest를 게시하고 Raw Payload 없는 Metadata를 기록한다."""
    if artifact is None:
        return None
    object_key, manifest_key = quarantine_object_keys(config.source_table, request.batch_id, request.logical_date)
    verified = verify_parquet_object(storage, upload_new_file(storage, object_key, artifact.path))
    if verified.row_count != artifact.row_count:
        raise RuntimeError("Final quarantine row count differs from the local artifact")
    manifest = QuarantineManifest(request.batch_id, run.run_id, config.source_table, object_key, verified.size, verified.content_sha256, verified.row_count, artifact.error_counts, current_time)
    upload_new_bytes(storage, manifest_key, manifest.to_bytes())
    return QuarantineBatch(
        table_batch_id=f"{request.batch_id}__{config.source_table}",
        object_key=object_key,
        row_count=verified.row_count,
        error_counts=artifact.error_counts,
    )


def _table_batch_identity(request: TableIngestionRequest, config: TableConfig) -> TableBatchIdentity:
    """- 외부 Batch ID를 표준 Table Batch Identity와 같은 계약으로 검증한다."""
    standard = BatchIdentity(dag_id=_dag_id_from_batch_id(request.batch_id), logical_date=request.logical_date)
    identity = standard.table_batch(config.source_table)
    if identity.batch_id != request.batch_id:
        raise ValueError("batch_id must match '{dag_id}__{logical_date_utc:%Y%m%dT%H%M%SZ}'")
    return identity


def _reuse_or_reject_committed_batch(postgres: PostgresSettings, request: TableIngestionRequest, config: TableConfig, watermark, existing: CommittedTableBatch, current_time: datetime) -> TableIngestionResult:
    """- 기존 Commit 범위가 같으면 Skip하고 다르면 Source Read 전에 Conflict로 끝낸다."""
    try:
        assert_reusable_table_batch(existing, current_watermark=watermark, schema_version=config.schema_version)
    except BatchIdentityConflictError as error:
        run = PipelineRun(request.run_id or uuid.uuid4(), request.resolved_pipeline_name, config.source_table, request.batch_id, request.logical_date, request.attempt_number, watermark.cursor, None)
        record_started_run(postgres, run, now=current_time)
        record_failed_run(postgres, run, error_type="BATCH_IDENTITY_CONFLICT", error_message=str(error), now=current_time)
        raise
    run = PipelineRun(request.run_id or uuid.uuid4(), request.resolved_pipeline_name, config.source_table, request.batch_id, request.logical_date, request.attempt_number, existing.watermark_before, existing.watermark_after)
    record_started_run(postgres, run, now=current_time)
    record_skipped_already_committed_run(postgres, run, row_count=existing.row_count, now=current_time)
    return TableIngestionResult(run, "SKIPPED_ALREADY_COMMITTED", existing.object_key, existing.manifest_key, existing.row_count)


def _dag_id_from_batch_id(batch_id: str) -> str:
    """- 표준 Batch ID에서 마지막 구분자 앞의 DAG ID를 안전하게 분리한다."""
    dag_id, separator, timestamp = batch_id.rpartition("__")
    if not separator or len(timestamp) != 16:
        raise ValueError("batch_id must use the standard DAG logical-date format")
    return dag_id


def _record_failure_without_masking(postgres: PostgresSettings, run: PipelineRun, error: Exception, current_time: datetime) -> None:
    """- 실패 원인을 남기되 기존 실행 오류를 Metadata 오류로 가리지 않는다."""
    with suppress(Exception):
        record_failed_run(postgres, run, error_type=type(error).__name__[:64], error_message=str(error) or None, now=current_time)


def _record_lease_failure(
    postgres: PostgresSettings,
    request: TableIngestionRequest,
    config: TableConfig,
    watermark,
    run_id: uuid.UUID,
    error: Exception,
    current_time: datetime,
) -> None:
    """- Lease 충돌 Run을 Source Snapshot 없이 FAILED Metadata로 기록한다."""
    run = PipelineRun(
        run_id=run_id,
        pipeline_name=request.resolved_pipeline_name,
        source_table=config.source_table,
        batch_id=request.batch_id,
        logical_date=request.logical_date,
        attempt_number=request.attempt_number,
        watermark_before=watermark.cursor,
        extract_upper_bound=None,
    )
    error_type = "SOURCE_MUTATION_CONFLICT" if isinstance(error, RuntimeError) else type(error).__name__
    with suppress(Exception):
        record_started_run(postgres, run, now=current_time)
        record_failed_run(
            postgres,
            run,
            error_type=error_type[:64],
            error_message=str(error) or None,
            now=current_time,
        )


def _assert_nonempty_batch_id(batch_id: str) -> None:
    """- Object Key에 쓰기 전 Batch ID가 비어 있지 않은지 확인한다."""
    if not batch_id.strip():
        raise ValueError("batch_id must not be empty")


def _assert_utc(value: datetime, name: str) -> None:
    """- 요청과 Key 구성에 쓰는 시각이 UTC인지 확인한다."""
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be normalized to UTC")


def _utc_now(value: datetime | None) -> datetime:
    """- 주입된 UTC 시각 또는 현재 UTC 시각을 실행 기준으로 반환한다."""
    if value is None:
        return datetime.now(UTC)
    _assert_utc(value, "now")
    return value
