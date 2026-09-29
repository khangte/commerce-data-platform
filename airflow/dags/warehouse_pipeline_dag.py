"""Phase 3 Bronze 수집 API를 Task 경계로 나눠 호출하는 Warehouse Pipeline DAG."""

from __future__ import annotations

import os
import tempfile
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG, get_current_context, task
from airflow.sdk.exceptions import AirflowException, AirflowFailException

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
from src.ingestion.errors import classify_error, is_retryable
from src.ingestion.service import TableIngestionRequest, ingest_table
from src.ingestion.storage import SeaweedFSSettings
from src.ingestion.verification import verify_bronze_commit
from src.serving.export import ServingPaths, export_serving_mart
from src.serving.metabase_repoint import (
    metabase_repoint_summary_status,
    repoint_and_prune_serving,
)
from src.warehouse.publish import (
    DEFAULT_WAREHOUSE_ROOT,
    WarehousePaths,
    build_warehouse,
    prepare_warehouse_build,
    publish_build,
)
from src.warehouse.publish_metadata import PublishRun, get_publish_run

LOCAL_DIRECTORY = Path(tempfile.gettempdir()) / "commerce-data-platform" / "warehouse-pipeline-dag"
WAREHOUSE_PATHS = WarehousePaths.under(DEFAULT_WAREHOUSE_ROOT)
SERVING_PATHS = ServingPaths.under(PROJECT_ROOT / "data" / "serving")

DEFAULT_TASK_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(seconds=60),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=20),
}

SOURCE_TABLES = (
    "customers",
    "customer_subscriptions",
    "customer_membership_tiers",
    "subscription_payments",
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

    @task(task_id="prepare_warehouse_build", trigger_rule="all_success")
    def prepare_warehouse_build_task(run_info: dict, verification: dict) -> dict:
        """검증 뒤 Published 파일을 복사한 Build 파일을 만들고 Catalog를 동기화한다."""
        settings = PostgresSettings.from_environment()
        run = PublishRun(
            publish_run_id=uuid.uuid4(),
            pipeline_name=dag.dag_id,
            batch_id=run_info["batch_id"],
            dag_run_id=get_current_context()["run_id"],
        )
        try:
            build_path = prepare_warehouse_build(settings, WAREHOUSE_PATHS, run)
        except Exception as error:
            _reraise_classified(error)
            raise
        return {"publish_run_id": str(run.publish_run_id), "build_path": str(build_path)}

    @task(task_id="dbt_build", trigger_rule="all_success", retries=0)
    def dbt_build_task(build: dict) -> dict:
        """Build 파일에만 dbt build를 실행한다. 실패 Build는 격리되고 Published 파일은 그대로다."""
        settings = PostgresSettings.from_environment()
        try:
            result = build_warehouse(settings, WAREHOUSE_PATHS, uuid.UUID(build["publish_run_id"]))
        except Exception as error:
            _reraise_classified(error)
            raise
        return {"tests_passed": result.tests_passed, "tests_failed": result.tests_failed}

    @task(task_id="publish_mart", trigger_rule="all_success", retries=0)
    def publish_mart_task(build: dict, dbt_result: dict) -> dict:
        """dbt build가 성공한 Build 파일을 Published 경로로 원자 교체한다."""
        settings = PostgresSettings.from_environment()
        try:
            outcome = publish_build(settings, WAREHOUSE_PATHS, uuid.UUID(build["publish_run_id"]))
        except Exception as error:
            _reraise_classified(error)
            raise
        return {
            "publish_run_id": str(outcome.publish_run_id),
            "changed_relations": list(outcome.changed_relations),
            "mart_hashes": outcome.mart_hashes,
        }

    @task(task_id="export_serving_mart", trigger_rule="all_success", retries=0)
    def export_serving_mart_task(publish: dict) -> dict:
        """성공한 Published Mart만 별도 Serving 파일로 복사하고 실패를 Publish와 분리한다."""
        try:
            result = export_serving_mart(
                WAREHOUSE_PATHS.published,
                SERVING_PATHS,
                publish_run_id=uuid.UUID(publish["publish_run_id"]),
                mart_hashes=publish["mart_hashes"],
                now=datetime.now(UTC),
            )
        except Exception as error:
            raise AirflowFailException(f"SERVING_EXPORT_FAILED: {error}") from error
        return {
            "export_id": str(result.export_id),
            "row_counts": result.row_counts,
            "serving_path": str(result.serving_path),
        }

    @task(trigger_rule="all_done")
    def publish_run_summary(
        run_info: dict,
        verification: dict | None,
        build: dict | None,
        publish: dict | None,
        serving_export: dict | None,
        metabase_repoint: str | None,
    ) -> dict:
        """DagRun의 Bronze 검증과 Mart Publish 결과를 작은 JSON Summary로 남긴다."""
        publish_status = "SKIPPED"
        publish_error_type = None
        if build:
            record = get_publish_run(
                PostgresSettings.from_environment(), uuid.UUID(build["publish_run_id"])
            )
            publish_status = record.status if record else "UNKNOWN"
            publish_error_type = record.error_type if record else None
        serving_export_status = "NOT_REQUESTED"
        if publish:
            serving_export_status = "SUCCESS" if serving_export else "SERVING_EXPORT_FAILED"
        summary = {
            "batch_id": run_info["batch_id"],
            "verified_table_count": verification["verified_table_count"] if verification else 0,
            "publish_run_id": build["publish_run_id"] if build else None,
            "publish_status": publish_status,
            "publish_error_type": publish_error_type,
            "changed_relations": publish["changed_relations"] if publish else [],
            "serving_export_status": serving_export_status,
            "serving_export_id": serving_export["export_id"] if serving_export else None,
            "serving_row_counts": serving_export["row_counts"] if serving_export else {},
            "metabase_repoint_status": metabase_repoint_summary_status(
                serving_export, metabase_repoint
            ),
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
    build = prepare_warehouse_build_task(run_info, verification)
    dbt_result = dbt_build_task(build)
    publish = publish_mart_task(build, dbt_result)
    serving_export = export_serving_mart_task(publish)

    @task(task_id="repoint_metabase_serving", trigger_rule="all_success")
    def repoint_metabase_serving_task(serving_export: dict) -> str:
        """Metabase 연결을 새 Export 경로로 바꾸고 Manifest를 검증한다."""
        status = repoint_and_prune_serving(
            serving_export["export_id"],
            os.environ.get("METABASE_URL", ""),
            os.environ.get("METABASE_API_KEY", ""),
            os.environ.get("METABASE_SERVING_DATABASE_ID", ""),
            SERVING_PATHS.serving.parent / "exports",
        )
        if status == "FAILED":
            raise AirflowException("METABASE_REPOINT_FAILED: API or Manifest validation failed")
        return status

    metabase_repoint = repoint_metabase_serving_task(serving_export)

    extract_results >> release_task
    extract_results >> verification >> build >> dbt_result >> publish >> serving_export >> metabase_repoint
    publish_run_summary(run_info, verification, build, publish, serving_export, metabase_repoint)
