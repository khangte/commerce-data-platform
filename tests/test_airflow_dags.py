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
