"""DAG Import/Parse Smoke Test(P4-04)를 검증한다.

Airflow Image를 빌드하고 Compose로 실행하므로 무겁다. `airflow` Marker로 분리해
기본 `pytest` 실행에서는 제외하고, `pytest -m airflow`로만 돈다.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]

pytestmark = [
    pytest.mark.airflow,
    pytest.mark.skipif(
        os.environ.get("RUN_AIRFLOW_SMOKE_TEST") != "1",
        reason="Set RUN_AIRFLOW_SMOKE_TEST=1 to build and run the airflow Compose profile.",
    ),
]


def _run_compose(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", "--profile", "airflow", *args],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def test_dags_import_without_errors() -> None:
    """두 DAG 모두 Import Error 없이 Parse된다."""
    build = _run_compose("build", "airflow-scheduler")
    assert build.returncode == 0, build.stderr

    up = _run_compose("up", "-d")
    assert up.returncode == 0, up.stderr

    try:
        result = _run_compose(
            "exec", "-T", "airflow-scheduler", "airflow", "dags", "list-import-errors"
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert (
            "No data found" in result.stdout or result.stdout.strip() == ""
        ), f"DAG Import Error 발견:\n{result.stdout}"

        listed = _run_compose("exec", "-T", "airflow-scheduler", "airflow", "dags", "list")
        assert listed.returncode == 0, listed.stderr
        assert "source_simulation_dag" in listed.stdout
        assert "warehouse_pipeline_dag" in listed.stdout
    finally:
        _run_compose("down")


def test_generator_success_triggers_warehouse_with_same_logical_date() -> None:
    """Generator DagRun 성공 뒤 같은 logical_date로 Warehouse DagRun이 생성되는지 검증한다."""
    build = _run_compose("build", "airflow-scheduler")
    assert build.returncode == 0, build.stderr

    up = _run_compose("up", "-d")
    assert up.returncode == 0, up.stderr

    try:
        # Airflow는 새로 Parse된 DAG를 기본적으로 paused 상태로 만든다(dags_are_paused_at_creation).
        # 이 테스트는 정상 트리거 경로를 검증하는 것이므로 대상 DAG를 먼저 unpause한다.
        unpause = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "unpause", "warehouse_pipeline_dag",
        )
        assert unpause.returncode == 0, unpause.stdout + unpause.stderr

        logical_date = "2026-09-11T00:00:00+00:00"
        trigger = _run_compose(
            "exec",
            "-T",
            "airflow-scheduler",
            "airflow",
            "dags",
            "test",
            "source_simulation_dag",
            logical_date,
            "--conf",
            '{"logical_date": "2026-09-11T00:00:00+00:00", "orders": 1}',
        )
        assert trigger.returncode == 0, trigger.stdout + trigger.stderr

        listed = _run_compose(
            "exec",
            "-T",
            "airflow-scheduler",
            "airflow",
            "dags",
            "list-runs",
            "warehouse_pipeline_dag",
        )
        assert listed.returncode == 0, listed.stderr
        assert logical_date in listed.stdout, (
            f"Warehouse DagRun with logical_date={logical_date} not found:\n{listed.stdout}"
        )
    finally:
        _run_compose("down")


def test_duplicate_generator_run_skips_second_warehouse_trigger() -> None:
    """같은 logical_date로 Generator를 두 번 실행하면 두 번째 Trigger Task는 skip되고 Warehouse DagRun은 1개로 유지되는지 검증한다."""
    build = _run_compose("build", "airflow-scheduler")
    assert build.returncode == 0, build.stderr

    up = _run_compose("up", "-d")
    assert up.returncode == 0, up.stderr

    try:
        # Airflow는 새로 Parse된 DAG를 기본적으로 paused 상태로 만든다(dags_are_paused_at_creation).
        # 이 테스트는 중복 트리거 경로를 검증하는 것이므로 대상 DAG를 먼저 unpause한다.
        unpause = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "unpause", "warehouse_pipeline_dag",
        )
        assert unpause.returncode == 0, unpause.stdout + unpause.stderr

        logical_date = "2026-09-11T03:00:00+00:00"
        conf = '{"logical_date": "2026-09-11T03:00:00+00:00", "orders": 1}'

        first = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "test", "source_simulation_dag", logical_date, "--conf", conf,
        )
        assert first.returncode == 0, first.stdout + first.stderr

        second = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "test", "source_simulation_dag", logical_date, "--conf", conf,
        )
        assert second.returncode == 0, second.stdout + second.stderr
        assert "trigger_warehouse_pipeline" in second.stdout

        listed = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "list-runs", "warehouse_pipeline_dag",
        )
        assert listed.returncode == 0, listed.stderr
        run_count = listed.stdout.count(logical_date)
        assert run_count == 1, (
            f"Expected exactly 1 warehouse_pipeline_dag run for {logical_date}, found {run_count}:\n{listed.stdout}"
        )
    finally:
        _run_compose("down")


def test_paused_warehouse_dag_fails_trigger_task() -> None:
    """warehouse_pipeline_dag가 paused 상태면 Trigger Task가 명시적으로 실패하는지 검증한다."""
    build = _run_compose("build", "airflow-scheduler")
    assert build.returncode == 0, build.stderr

    up = _run_compose("up", "-d")
    assert up.returncode == 0, up.stderr

    try:
        pause = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "pause", "warehouse_pipeline_dag",
        )
        assert pause.returncode == 0, pause.stdout + pause.stderr

        logical_date = "2026-09-11T06:00:00+00:00"
        conf = '{"logical_date": "2026-09-11T06:00:00+00:00", "orders": 1}'
        result = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "test", "source_simulation_dag", logical_date, "--conf", conf,
        )
        assert result.returncode != 0, (
            "warehouse_pipeline_dag가 paused일 때 trigger_warehouse_pipeline Task는 실패해야 한다"
        )
        assert "trigger_warehouse_pipeline" in result.stdout + result.stderr
    finally:
        unpause = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "unpause", "warehouse_pipeline_dag",
        )
        assert unpause.returncode == 0, unpause.stdout + unpause.stderr
        _run_compose("down")


def test_generator_failure_does_not_trigger_warehouse() -> None:
    """Generator 실행이 실제로 실패(AirflowFailException)하면 DagRun이 실패로 종료되어
    trigger_warehouse_pipeline Task가 실행되지 못하고(스케줄러 환경에서는 upstream_failed로
    건너뜀) Warehouse DagRun이 생성되지 않는지 검증한다.

    `airflow dags test`는 단일 프로세스로 Task를 순차 실행하는 로컬 테스트 러너라 첫 Task가
    실패하면 DagRun을 즉시 failed로 종료하고 이후 Task를 아예 실행하지 않으므로, stdout에
    "upstream_failed" 문자열 자체는 나타나지 않는다. 따라서 이 테스트는 (1) 실패 원인이
    Param 검증이 아닌 run_source_simulation Task 실행 중 AirflowFailException이라는 점과
    (2) 그 결과 DagRun이 failed로 종료되고 warehouse_pipeline_dag DagRun이 생성되지 않았다는
    점을 관측한다.
    """
    build = _run_compose("build", "airflow-scheduler")
    assert build.returncode == 0, build.stderr

    up = _run_compose("up", "-d")
    assert up.returncode == 0, up.stderr

    try:
        # orders=-1은 Param JSON-Schema 검증(minimum=0) 단계에서 Task 실행 전에 막혀
        # upstream_failed 전이를 관측할 수 없다. 대신 source_snapshot_id에 존재할 수
        # 없는 값을 줘서 run_generator() 내부에서 ValueError가 발생하게 하고,
        # 이를 source_simulation_dag.py가 AirflowFailException으로 재분류해
        # Task 실행 도중 실패하도록 유도한다.
        logical_date = "2026-09-11T12:00:00+00:00"
        nonexistent_snapshot_id = (
            "seed:nonexistent-00000000-0000-0000-0000-000000000000:2020-01-01T00:00:00+00:00"
        )
        failing_conf = (
            '{"logical_date": "2026-09-11T12:00:00+00:00", "orders": 1, '
            f'"source_snapshot_id": "{nonexistent_snapshot_id}"}}'
        )

        result = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "test", "source_simulation_dag", logical_date, "--conf", failing_conf,
        )
        assert result.returncode != 0, (
            "존재하지 않는 source_snapshot_id는 run_generator() 실행 중 "
            "AirflowFailException으로 실패해야 한다"
        )
        combined_output = result.stdout + result.stderr
        # Param 검증이 아니라 Task 실행 도중(run_generator 호출 이후) 발생한 실패인지 확인한다.
        assert "run_source_simulation" in combined_output, (
            f"run_source_simulation Task 실행 흔적을 찾지 못함(Param 검증 단계에서 막혔을 수 있음):\n{combined_output}"
        )
        assert "AirflowFailException" in combined_output, (
            f"AirflowFailException으로 실패한 흔적을 찾지 못함:\n{combined_output}"
        )
        # `airflow dags test`는 첫 Task 실패 시 DagRun을 즉시 failed로 종료하고
        # 스케줄러 없이 downstream Task를 실행하지 않으므로, "DagRun이 failed로
        # 종료되어 trigger_warehouse_pipeline이 실행되지 못했다"가 이 실행 모드에서
        # 관측 가능한 upstream_failed 전이의 대리 신호다.
        assert "DagRun failed" in combined_output, (
            f"DagRun이 failed로 종료된 흔적을 찾지 못함:\n{combined_output}"
        )

        listed = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "list-runs", "warehouse_pipeline_dag",
        )
        assert listed.returncode == 0, listed.stderr
        assert logical_date not in listed.stdout, (
            f"Generator 실패에도 warehouse_pipeline_dag DagRun이 생성됨:\n{listed.stdout}"
        )
    finally:
        _run_compose("down")
