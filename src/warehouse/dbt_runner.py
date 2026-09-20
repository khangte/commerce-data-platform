"""Build Warehouse 파일에 dbt build를 실행하고 결과를 분류한다."""

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from src.common.database import PROJECT_ROOT, environment_values
from src.warehouse.errors import DBT_BUILD_ERROR, DBT_TEST_ERROR

DBT_PROJECT_DIR = PROJECT_ROOT / "dbt"
OUTPUT_LIMIT = 4000
_BUILD_NODE_PREFIXES = ("model.", "seed.", "snapshot.")
_TEST_NODE_PREFIXES = ("test.", "unit_test.")
_FAILED_STATUSES = frozenset({"error", "fail"})


@dataclass(frozen=True)
class DbtRunResult:
    """dbt build 한 번의 Exit Code, Test 수, 분류된 실패 정보를 담는다."""

    returncode: int
    invocation_id: str | None
    tests_passed: int
    tests_failed: int
    error_type: str | None
    failed_nodes: tuple[str, ...]
    output: str

    @property
    def succeeded(self) -> bool:
        """Exit Code가 0이고 분류된 실패가 없으면 True다."""
        return self.returncode == 0 and self.error_type is None


def run_dbt_build(
    build_path: Path,
    target_path: Path,
    *,
    dbt_executable: str | None = None,
    project_dir: Path = DBT_PROJECT_DIR,
    extra_args: tuple[str, ...] = (),
) -> DbtRunResult:
    """WAREHOUSE_PATH를 Build 파일로 지정해 dbt build를 실행하고 결과를 해석한다."""
    environment = {**environment_values(), "WAREHOUSE_PATH": str(build_path)}
    process = subprocess.run(
        [
            dbt_executable or _default_dbt_executable(),
            "build",
            "--project-dir",
            str(project_dir),
            "--profiles-dir",
            str(project_dir),
            "--target-path",
            str(target_path),
            *extra_args,
        ],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    output = f"{process.stdout}\n{process.stderr}"
    return parse_run_results(target_path / "run_results.json", process.returncode, output)


def parse_run_results(path: Path, returncode: int, output: str) -> DbtRunResult:
    """run_results.json과 Exit Code로 DbtRunResult를 만든다."""
    invocation_id: str | None = None
    results: list[dict] = []
    if path.is_file():
        payload = json.loads(path.read_text(encoding="utf-8"))
        invocation_id = payload.get("metadata", {}).get("invocation_id")
        results = payload.get("results", [])
    tests = [item for item in results if item["unique_id"].startswith(_TEST_NODE_PREFIXES)]
    tests_failed = sum(item["status"] in _FAILED_STATUSES for item in tests)
    failed_nodes = tuple(
        item["unique_id"] for item in results if item["status"] in _FAILED_STATUSES
    )
    failed = returncode != 0 or bool(failed_nodes)
    return DbtRunResult(
        returncode=returncode,
        invocation_id=invocation_id,
        tests_passed=len(tests) - tests_failed,
        tests_failed=tests_failed,
        error_type=classify_dbt_failure(results) if failed else None,
        failed_nodes=failed_nodes,
        output=output[-OUTPUT_LIMIT:],
    )


def classify_dbt_failure(results: list[dict]) -> str:
    """Model·Seed·Snapshot 오류를 Test 실패보다 먼저 보고, 근거가 없으면 Build 오류로 본다."""
    if any(
        item["unique_id"].startswith(_BUILD_NODE_PREFIXES) and item["status"] == "error"
        for item in results
    ):
        return DBT_BUILD_ERROR
    if any(
        item["unique_id"].startswith(_TEST_NODE_PREFIXES) and item["status"] in _FAILED_STATUSES
        for item in results
    ):
        return DBT_TEST_ERROR
    return DBT_BUILD_ERROR


def _default_dbt_executable() -> str:
    """현재 Python 옆의 dbt를 우선 쓰고, 없으면 PATH의 dbt를 쓴다."""
    sibling = Path(sys.executable).with_name("dbt")
    return str(sibling) if sibling.is_file() else "dbt"
