"""Phase 3 Bronze 수집 API를 Task 경계로 나눠 호출하는 Warehouse Pipeline DAG."""

from __future__ import annotations

import tempfile
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG, task
from airflow.sdk.exceptions import AirflowFailException

from src.common.database import PROJECT_ROOT, PostgresSettings
from src.generator.lease import (
    WAREHOUSE_OWNER_TYPE,
    LeaseOwnershipLostError,
    LeaseUnavailableError,
    SourceMutationLease,
    acquire_source_mutation_lease,
    release_source_mutation_lease,
)
from src.ingestion.batch import BatchIdentity
from src.ingestion.catalog import sync_bronze_catalog
from src.ingestion.errors import classify_error, is_retryable
from src.ingestion.service import TableIngestionRequest, ingest_table
from src.ingestion.storage import SeaweedFSSettings
from src.ingestion.verification import verify_bronze_commit

CATALOG_PATH = PROJECT_ROOT / "data" / "warehouse" / "warehouse.duckdb"
LOCAL_DIRECTORY = Path(tempfile.gettempdir()) / "commerce-data-platform" / "warehouse-pipeline-dag"

DEFAULT_TASK_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(seconds=60),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=20),
}

SOURCE_TABLES = (
    "customers",
    "customer_memberships",
    "products",
    "sellers",
    "orders",
    "order_items",
    "order_payments",
)


def _lease_token(lease: SourceMutationLease) -> dict:
    """SourceMutationLease를 XCom에 저장할 JSON 직렬화 가능한 Token으로 바꾼다."""
    return {
        "owner_type": lease.owner_type,
        "owner_id": str(lease.owner_id),
        "lease_expires_at": lease.lease_expires_at.isoformat(),
        "version": lease.version,
    }


def _lease_from_token(token: dict) -> SourceMutationLease:
    """XCom Lease Token을 SourceMutationLease로 복원한다."""
    return SourceMutationLease(
        owner_type=token["owner_type"],
        owner_id=uuid.UUID(token["owner_id"]),
        lease_expires_at=datetime.fromisoformat(token["lease_expires_at"]),
        version=token["version"],
    )


def _reraise_classified(error: Exception) -> None:
    """Non-retryable Error는 재시도 없이 즉시 실패시키고 Retryable Error는 그대로 다시 던진다."""
    error_type = classify_error(error)
    if is_retryable(error):
        raise error
    raise AirflowFailException(f"{error_type}: {error}") from error


with DAG(
    dag_id="warehouse_pipeline_dag",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_TASK_ARGS,
) as dag:

    @task
    def initialize_run(**context) -> dict:
        """DagRun Logical Date로 표준 Batch ID를 만들어 이후 Task에 전달한다."""
        logical_date = context["logical_date"]
        if logical_date.utcoffset() is None:
            raise ValueError("logical_date must include a UTC offset")
        batch = BatchIdentity(dag_id=dag.dag_id, logical_date=logical_date.astimezone(UTC))
        return {"batch_id": batch.batch_id, "logical_date": batch.logical_date.isoformat()}

    @task
    def acquire_source_snapshot_lease(run_info: dict) -> dict:
        """WAREHOUSE 원천 데이터 동시성 잠금을 획득해 Lease Token만 반환한다."""
        settings = PostgresSettings.from_environment()
        owner_id = uuid.uuid4()
        try:
            lease = acquire_source_mutation_lease(
                settings, owner_type=WAREHOUSE_OWNER_TYPE, owner_id=owner_id
            )
        except Exception as error:
            _reraise_classified(error)
            raise
        return _lease_token(lease)

    @task(max_active_tis_per_dag=3)
    def extract_validate_load(source_table: str, run_info: dict, lease_token: dict) -> dict:
        """하나의 Source Table을 Lease Token으로 검증·적재해 작은 결과 증적만 반환한다."""
        settings = PostgresSettings.from_environment()
        storage = SeaweedFSSettings.from_environment()
        lease = _lease_from_token(lease_token)
        logical_date = datetime.fromisoformat(run_info["logical_date"])
        request = TableIngestionRequest.for_dag_run(
            source_table=source_table, dag_id=dag.dag_id, logical_date=logical_date
        )
        try:
            result = ingest_table(
                settings,
                storage,
                request,
                local_directory=LOCAL_DIRECTORY,
                source_lease=lease,
            )
        except Exception as error:
            _reraise_classified(error)
            raise
        return {
            "source_table": source_table,
            "status": result.status,
            "object_key": result.object_key,
            "row_count": result.row_count,
        }

    @task(trigger_rule="all_done")
    def release_source_snapshot_lease(lease_token: dict) -> None:
        """모든 Table Task 종료 뒤 같은 Token으로 원천 데이터 동시성 잠금을 해제한다."""
        settings = PostgresSettings.from_environment()
        lease = _lease_from_token(lease_token)
        try:
            release_source_mutation_lease(settings, lease)
        except (LeaseUnavailableError, LeaseOwnershipLostError):
            pass

    @task(trigger_rule="all_success")
    def verify_bronze_commit_task(run_info: dict, extract_results: list) -> dict:
        """Map Task 성공 응답만 신뢰하지 않고 Metadata·Manifest·Object를 재확인한다."""
        settings = PostgresSettings.from_environment()
        storage = SeaweedFSSettings.from_environment()
        logical_date = datetime.fromisoformat(run_info["logical_date"])
        batch = BatchIdentity(dag_id=dag.dag_id, logical_date=logical_date)
        try:
            verified = verify_bronze_commit(settings, storage, batch, SOURCE_TABLES)
        except Exception as error:
            _reraise_classified(error)
            raise
        return {"verified_table_count": len(verified)}

    @task(trigger_rule="all_success")
    def sync_bronze_catalog_task(verification: dict) -> dict:
        """검증을 통과한 뒤에만 COMMITTED Object를 DuckDB Catalog로 동기화한다."""
        settings = PostgresSettings.from_environment()
        try:
            entries = sync_bronze_catalog(settings, CATALOG_PATH)
        except Exception as error:
            _reraise_classified(error)
            raise
        return {"catalog_entry_count": len(entries)}

    @task(trigger_rule="all_done")
    def publish_run_summary(run_info: dict, verification: dict | None, catalog: dict | None) -> dict:
        """DagRun 결과를 작은 JSON Summary로 로그와 XCom에 남긴다."""
        summary = {
            "batch_id": run_info["batch_id"],
            "verified_table_count": verification["verified_table_count"] if verification else 0,
            "catalog_entry_count": catalog["catalog_entry_count"] if catalog else 0,
        }
        print(summary)
        return summary

    run_info = initialize_run()
    lease_token = acquire_source_snapshot_lease(run_info)
    extract_results = extract_validate_load.partial(run_info=run_info, lease_token=lease_token).expand(
        source_table=list(SOURCE_TABLES)
    )
    release_task = release_source_snapshot_lease(lease_token)
    verification = verify_bronze_commit_task(run_info, extract_results)
    catalog = sync_bronze_catalog_task(verification)

    extract_results >> release_task
    extract_results >> verification >> catalog
    publish_run_summary(run_info, verification, catalog)
