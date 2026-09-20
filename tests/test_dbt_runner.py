"""dbt run_results.json 해석과 실패 분류를 검증한다."""

from __future__ import annotations

import json
import stat
import sys
from pathlib import Path

import pytest

from src.warehouse.dbt_runner import classify_dbt_failure, parse_run_results, run_dbt_build
from src.warehouse.errors import DBT_BUILD_ERROR, DBT_TEST_ERROR


def _node(unique_id: str, status: str) -> dict[str, str]:
    """run_results.json의 Result 한 건을 만든다."""
    return {"unique_id": unique_id, "status": status}


def _write_results(path: Path, results: list[dict[str, str]]) -> Path:
    """Invocation ID와 Result 목록을 가진 run_results.json을 쓴다."""
    path.write_text(
        json.dumps({"metadata": {"invocation_id": "inv-1"}, "results": results}),
        encoding="utf-8",
    )
    return path


@pytest.mark.parametrize(
    ("results", "expected"),
    [
        ([_node("model.p.fct_order", "error"), _node("test.p.x", "fail")], DBT_BUILD_ERROR),
        ([_node("seed.p.s", "error")], DBT_BUILD_ERROR),
        ([_node("snapshot.p.s", "error")], DBT_BUILD_ERROR),
        ([_node("model.p.fct_order", "success"), _node("test.p.x", "fail")], DBT_TEST_ERROR),
        ([_node("unit_test.p.u", "error")], DBT_TEST_ERROR),
        ([_node("model.p.fct_order", "skipped")], DBT_BUILD_ERROR),
        ([], DBT_BUILD_ERROR),
    ],
)
def test_classify_dbt_failure(results: list[dict[str, str]], expected: str) -> None:
    """Model 오류가 Test 실패보다 우선하고, 근거가 없으면 Build 오류로 본다."""
    assert classify_dbt_failure(results) == expected


def test_parse_success_counts_tests(tmp_path: Path) -> None:
    """성공 Run은 Error Type 없이 Test 통과·실패 수를 센다."""
    path = _write_results(
        tmp_path / "run_results.json",
        [
            _node("model.p.fct_order", "success"),
            _node("test.p.a", "pass"),
            _node("unit_test.p.b", "pass"),
            _node("test.p.c", "warn"),
        ],
    )
    result = parse_run_results(path, 0, "ok")
    assert result.succeeded
    assert result.invocation_id == "inv-1"
    assert (result.tests_passed, result.tests_failed) == (3, 0)
    assert result.failed_nodes == ()


def test_parse_failure_lists_failed_nodes(tmp_path: Path) -> None:
    """실패 Run은 분류된 Error Type과 실패 Node 목록을 가진다."""
    path = _write_results(
        tmp_path / "run_results.json",
        [_node("model.p.fct_order", "success"), _node("test.p.a", "fail")],
    )
    result = parse_run_results(path, 1, "x" * 10_000)
    assert not result.succeeded
    assert result.error_type == DBT_TEST_ERROR
    assert result.failed_nodes == ("test.p.a",)
    assert result.tests_failed == 1
    assert len(result.output) == 4000


def test_parse_missing_file_with_nonzero_exit_is_build_error(tmp_path: Path) -> None:
    """run_results.json이 없고 Exit Code가 0이 아니면 Build 오류다."""
    result = parse_run_results(tmp_path / "missing.json", 2, "compile error")
    assert result.error_type == DBT_BUILD_ERROR
    assert result.invocation_id is None


def test_parse_nonzero_exit_without_failed_nodes_is_build_error(tmp_path: Path) -> None:
    """모든 Node가 성공이어도 Exit Code가 0이 아니면 성공으로 보지 않는다."""
    path = _write_results(tmp_path / "run_results.json", [_node("test.p.a", "pass")])
    assert parse_run_results(path, 1, "").error_type == DBT_BUILD_ERROR


def test_run_dbt_build_passes_build_path_and_target_path(tmp_path: Path) -> None:
    """Runner는 WAREHOUSE_PATH와 --target-path를 Build 경로로 넘긴다."""
    fake = tmp_path / "fake-dbt"
    fake.write_text(
        f"#!{sys.executable}\n"
        "import json, os, pathlib, sys\n"
        "target = pathlib.Path(sys.argv[sys.argv.index('--target-path') + 1])\n"
        "target.mkdir(parents=True, exist_ok=True)\n"
        "(target / 'run_results.json').write_text(json.dumps({\n"
        "    'metadata': {'invocation_id': os.environ['WAREHOUSE_PATH']},\n"
        "    'results': [{'unique_id': 'test.p.a', 'status': 'pass'}],\n"
        "}))\n",
        encoding="utf-8",
    )
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    build_path = tmp_path / "build" / "run.duckdb"
    result = run_dbt_build(build_path, tmp_path / "target", dbt_executable=str(fake))
    assert result.succeeded
    assert result.invocation_id == str(build_path)
    assert result.tests_passed == 1
