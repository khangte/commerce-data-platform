"""DagRun별 운영 상태를 독립적으로 수집하고 실패를 격리해 저장한다."""

from __future__ import annotations

import json
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path

import duckdb
from psycopg.types.json import Jsonb

from src.common.database import PostgresSettings, apply_sql_file
from src.common.schedule import schedule_interval, source_dag_schedule
from src.serving.freshness import check_serving_freshness
from src.serving.metabase_repoint import SERVING_EXPORT_RETENTION
from src.warehouse.publish import FAILED_BUILD_RETENTION

RUN_LIMIT_SECONDS = 600
STALE_AFTER_INTERVALS = 2
ROW_COUNT_DROP_TOLERANCE = 0.01


def judge_run_report(report: dict) -> list[str]:
    """수집된 운영 값에서 실패 코드 목록을 결정한다."""
    failures = []
    for task in report.get("tasks", []):
        if task["state"] not in ("success", "not_terminal"):
            failures.append(f"TASK_NOT_SUCCESS:{task['task_id']}")
    if report.get("publish_status") != "PUBLISHED":
        failures.append(f"PUBLISH_{report.get('publish_status', 'UNKNOWN')}")
    if report.get("serving_export_status") != "SUCCESS":
        failures.append("SERVING_EXPORT_FAILED")
    if report.get("metabase_repoint_status") != "SUCCESS":
        failures.append(f"REPOINT_{report.get('metabase_repoint_status', 'UNKNOWN')}")
    if report.get("fresh") is False:
        failures.append("NOT_FRESH")
    lag, threshold = report.get("data_lag_s"), report.get("stale_after_s")
    if lag is not None and threshold is not None and lag > threshold:
        failures.append("DATA_STALE")
    previous, current = report.get("previous_mart_row_counts"), report.get("mart_row_counts")
    if not report.get("full_refresh", False) and previous is not None and current is not None:
        for relation in sorted(previous.keys() & current.keys()):
            if current[relation] < previous[relation] * (1 - ROW_COUNT_DROP_TOLERANCE):
                failures.append(f"ROW_COUNT_DROP:{relation}")
    if "exports_count" in report and report["exports_count"] != SERVING_EXPORT_RETENTION:
        failures.append(f"EXPORTS_COUNT_{report['exports_count']}")
    if report.get("failed_builds_count", 0) > FAILED_BUILD_RETENTION:
        failures.append("FAILED_BUILDS_OVER_RETENTION")
    if report.get("warehouse_build_empty") is False or report.get("serving_build_empty") is False:
        failures.append("BUILD_DIR_NOT_EMPTY")
    if report.get("held_table_leases") or report.get("source_lease_owner"):
        failures.append("LEASE_HELD")
    if (report.get("dag_elapsed_s") or 0) >= RUN_LIMIT_SECONDS:
        failures.append("SLOW_RUN")
    failures.extend(f"PROBE_ERROR:{probe}" for probe in report.get("probe_errors", {}))
    return list(dict.fromkeys(failures))


def collect_run_report(
    settings: PostgresSettings, summary: dict, context: dict, warehouse_paths, serving_paths,
    *, now: datetime | None = None,
) -> dict:
    """수집 단계별 예외를 격리하고 기존 요약에 운영 판정 결과를 더한다."""
    now = now or datetime.now(UTC)
    run = context.get("dag_run")
    logical_date = context.get("logical_date") or getattr(run, "logical_date", None)
    report = {
        **summary,
        "dag_run_id": getattr(run, "run_id", None),
        "logical_date": logical_date.isoformat() if logical_date else None,
        "recorded_at": now.isoformat(),
        "probe_errors": {},
    }
    for name, probe in (
        ("dag", _probe_dag), ("tasks", _probe_tasks), ("ingestion", _probe_ingestion),
        ("leases", _probe_leases), ("bronze", _probe_bronze), ("publish", _probe_publish),
        ("freshness", _probe_freshness), ("staleness", _probe_staleness),
        ("row_counts", _probe_row_counts), ("files", _probe_files),
    ):
        try:
            report.update(probe(settings, summary, context, warehouse_paths, serving_paths, now))
        except Exception as error:  # noqa: BLE001 - 수집·기록 장애를 DAG 밖으로 전파하지 않는다.
            report["probe_errors"][name] = str(error)
    report["failures"] = judge_run_report(report)
    report["ok"] = not report["failures"]
    return report


def _probe_dag(settings, summary, context, warehouse_paths, serving_paths, now) -> dict:
    """DagRun 식별자와 실제 시작 지연·경과 시간을 수집한다."""
    run = context["dag_run"]
    logical_date = run.logical_date
    run_after = run.run_after
    start = run.start_date
    return {
        "dag_run_id": run.run_id,
        "logical_date": logical_date.isoformat() if logical_date else None,
        "run_after": run_after.isoformat() if run_after else None,
        "start_lag_s": (start - run_after).total_seconds() if start and run_after else None,
        "dag_elapsed_s": (now - start).total_seconds() if start else None,
    }


def _probe_tasks(settings, summary, context, warehouse_paths, serving_paths, now) -> dict:
    """Execution API의 종료 작업과 아직 종료하지 않은 작업을 기록한다."""
    run = context["dag_run"]
    breadcrumbs = context["ti"].get_task_breadcrumbs(run.dag_id, run.run_id)
    tasks = [
        {key: task.get(key) for key in ("task_id", "map_index", "state", "duration")}
        for task in breadcrumbs
    ]
    seen = {task["task_id"] for task in tasks}
    for task_id in context["dag"].task_ids:
        if task_id not in seen:
            tasks.append({"task_id": task_id, "map_index": -1,
                          "state": "not_terminal", "duration": None})
    return {"tasks": tasks}


def _probe_ingestion(settings, summary, context, warehouse_paths, serving_paths, now) -> dict:
    """배치의 테이블별 최신 시도만 합산한다."""
    with settings.pipeline_connection() as connection:
        rows = connection.execute(
            """
            SELECT DISTINCT ON (source_table) source_table, status, rows_loaded,
                   rows_rejected, attempt_number
            FROM pipeline_runs WHERE batch_id = %s
            ORDER BY source_table, attempt_number DESC, started_at DESC, run_id DESC
            """, (summary["batch_id"],),
        ).fetchall()
    return {
        "tables": [row[0] for row in rows],
        "statuses": {row[0]: row[1] for row in rows},
        "rows_loaded": sum(row[2] for row in rows),
        "rows_rejected": sum(row[3] for row in rows),
        "no_data_tables": sum(row[1] == "SUCCESS_NO_DATA" for row in rows),
        "max_attempt": max((row[4] for row in rows), default=0),
    }


def _probe_leases(settings, summary, context, warehouse_paths, serving_paths, now) -> dict:
    """해당 파이프라인의 테이블 잠금과 원천 잠금 소유자를 확인한다."""
    with settings.pipeline_connection() as connection:
        held = connection.execute(
            """SELECT count(*) FROM watermarks WHERE lease_owner IS NOT NULL
               AND pipeline_name IN (SELECT pipeline_name FROM pipeline_runs WHERE batch_id = %s)
            """, (summary["batch_id"],),
        ).fetchone()[0]
        source = connection.execute(
            "SELECT owner_id FROM source_mutation_leases WHERE resource_name = 'commerce_source'"
        ).fetchone()
    return {"held_table_leases": held,
            "source_lease_owner": str(source[0]) if source and source[0] else None}


def _probe_bronze(settings, summary, context, warehouse_paths, serving_paths, now) -> dict:
    """배치의 확정된 원시 객체와 미확정 객체 수를 수집한다."""
    with settings.pipeline_connection() as connection:
        committed, uncommitted = connection.execute(
            """SELECT count(*) FILTER (WHERE status = 'COMMITTED'),
                      count(*) FILTER (WHERE status <> 'COMMITTED')
               FROM bronze_objects WHERE batch_id = %s""", (summary["batch_id"],),
        ).fetchone()
    return {"committed_objects": committed, "uncommitted_objects": uncommitted}


def _probe_publish(settings, summary, context, warehouse_paths, serving_paths, now) -> dict:
    """아직 진행 중인 게시 실행 수를 수집한다."""
    with settings.pipeline_connection() as connection:
        count = connection.execute(
            "SELECT count(*) FROM mart_publish_runs WHERE status IN ('BUILDING', 'PUBLISHING')"
        ).fetchone()[0]
    return {"active_publish_runs": count}


def _probe_freshness(settings, summary, context, warehouse_paths, serving_paths, now) -> dict:
    """설정이 있을 때 기존 검사로 실제 서빙 실행의 최신 여부를 확인한다."""
    url, key, database_id = (
        os.environ.get(name, "") for name in
        ("METABASE_URL", "METABASE_API_KEY", "METABASE_SERVING_DATABASE_ID")
    )
    if not all((url, key, database_id)):
        return {"fresh": None, "live_export_id": None}
    freshness = check_serving_freshness(settings, url, key, int(database_id))
    return {"fresh": freshness.fresh, "live_export_id": freshness.live_export_id}


def _probe_staleness(settings, summary, context, warehouse_paths, serving_paths, now) -> dict:
    """게시 파일의 최신 주문 구매 시각으로 데이터 지연과 허용 간격을 수집한다."""
    threshold = STALE_AFTER_INTERVALS * schedule_interval(source_dag_schedule())
    result = {"max_purchase_at": None, "data_lag_s": None,
              "stale_after_s": threshold.total_seconds()}
    if not warehouse_paths.published.exists():
        return result
    with duckdb.connect(str(warehouse_paths.published), read_only=True) as connection:
        epoch = connection.execute('SELECT epoch(max(purchase_at)) FROM facts.fct_order').fetchone()[0]
    if epoch is not None:
        result.update(max_purchase_at=datetime.fromtimestamp(epoch, UTC).isoformat(),
                      data_lag_s=now.timestamp() - epoch)
    return result


def _probe_row_counts(settings, summary, context, warehouse_paths, serving_paths, now) -> dict:
    """이번 게시와 이 실행이 참조하는 직전 게시의 행 수를 수집한다."""
    result = {"mart_row_counts": None, "previous_mart_row_counts": None,
              "full_refresh": bool(context.get("params", {}).get("full_refresh", False))}
    publish_run_id = summary.get("publish_run_id")
    if not publish_run_id:
        return result
    with settings.pipeline_connection() as connection:
        row = connection.execute(
            """SELECT cur.mart_row_counts, prev.mart_row_counts
               FROM mart_publish_runs cur
               LEFT JOIN mart_publish_runs prev
                 ON prev.publish_run_id = cur.previous_publish_run_id
               WHERE cur.publish_run_id = %s AND cur.status = 'PUBLISHED'""",
            (publish_run_id,),
        ).fetchone()
    if row:
        result.update(mart_row_counts=row[0], previous_mart_row_counts=row[1])
    return result


def _bytes(path: Path) -> int:
    """파일 또는 디렉터리 아래 파일의 논리 크기를 합산한다."""
    if path.is_file():
        return path.stat().st_size
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _probe_files(settings, summary, context, warehouse_paths, serving_paths, now) -> dict:
    """버전 보존·실패 파일·임시 디렉터리·디스크 여유 공간을 확인한다."""
    exports = serving_paths.serving.parent / "exports"
    return {
        "exports_count": len(list(exports.glob("*.duckdb"))),
        "failed_builds_count": len(list(warehouse_paths.failed_dir.glob("*.duckdb"))),
        "warehouse_build_empty": not any(warehouse_paths.build_dir.glob("*")),
        "serving_build_empty": not any(serving_paths.build_dir.glob("*")),
        "warehouse_bytes": _bytes(warehouse_paths.published),
        "serving_bytes": _bytes(serving_paths.serving),
        "exports_bytes": _bytes(exports), "failed_bytes": _bytes(warehouse_paths.failed_dir),
        "disk_free_bytes": shutil.disk_usage(warehouse_paths.published.parent).free,
    }


def record_run_report(settings: PostgresSettings, report: dict) -> None:
    """동일 DagRun의 기록을 갱신하고 저장 실패는 로그만 남긴다."""
    try:
        with settings.pipeline_connection() as connection:
            apply_sql_file(connection, "sql/metadata/006_create_warehouse_run_reports.sql")
            connection.execute(
                """
                INSERT INTO warehouse_run_reports
                    (dag_run_id, batch_id, logical_date, recorded_at, ok, failures, report)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (dag_run_id) DO UPDATE SET
                    batch_id = EXCLUDED.batch_id, logical_date = EXCLUDED.logical_date,
                    recorded_at = EXCLUDED.recorded_at, ok = EXCLUDED.ok,
                    failures = EXCLUDED.failures, report = EXCLUDED.report
                """,
                (report["dag_run_id"], report["batch_id"], report["logical_date"],
                 report["recorded_at"], report["ok"], Jsonb(report["failures"]),
                 Jsonb(json.loads(json.dumps(report, default=str)))),
            )
    except Exception as error:  # noqa: BLE001 - 수집·기록 장애를 DAG 밖으로 전파하지 않는다.
        print(f"RUN_REPORT_WRITE_FAILED: {error}")
