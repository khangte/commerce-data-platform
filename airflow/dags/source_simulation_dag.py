"""Phase 2 결정적 Generator를 Airflow Params로 호출하는 Source Simulation DAG."""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG, task
from airflow.sdk.definitions.param import Param

from src.common.database import PostgresSettings
from src.generator.config import EXECUTABLE_ANOMALY_PROFILES, GENERATOR_VERSION, GeneratorConfig
from src.generator.service import resolve_source_snapshot_id, run_generator

with DAG(
    dag_id="source_simulation_dag",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "seed": Param(0, type="integer", description="Deterministic random seed."),
        "logical_date": Param(
            None,
            type=["null", "string"],
            description="UTC logical date in ISO-8601 format. Defaults to the DAG run's logical date.",
        ),
        "orders": Param(1, type="integer", minimum=0, description="Number of orders to generate."),
        "anomaly_profile": Param(
            "default",
            type="string",
            enum=sorted(EXECUTABLE_ANOMALY_PROFILES),
            description="Deterministic profile executable by the generator.",
        ),
        "source_snapshot_id": Param(
            None,
            type=["null", "string"],
            description="Seed snapshot identifier. Defaults to the latest successful seed.",
        ),
    },
) as dag:

    @task
    def run_source_simulation(**context) -> dict:
        """Params를 검증해 Phase 2 Generator API를 실행하고 결과 증적만 반환한다."""
        params = context["params"]
        settings = PostgresSettings.from_environment()
        logical_date = params["logical_date"] or context["logical_date"].isoformat()
        source_snapshot_id = params["source_snapshot_id"] or resolve_source_snapshot_id(settings)

        config = GeneratorConfig.from_values(
            source_snapshot_id=source_snapshot_id,
            random_seed=params["seed"],
            logical_date=logical_date,
            order_count=params["orders"],
            anomaly_profile=params["anomaly_profile"],
            generator_version=GENERATOR_VERSION,
        )
        result = run_generator(config, settings)
        return {
            "generator_run_id": str(result.generator_run_id),
            "result_counts": result.result_counts,
            "logical_content_hash": result.logical_content_hash,
            "reused_successful_run": result.reused_successful_run,
        }

    run_source_simulation()
