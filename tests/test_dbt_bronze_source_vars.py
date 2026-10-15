"""dbt Bronze 변수의 실행·파싱 경계와 동적 계약 사용을 검증한다."""

from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import Environment, StrictUndefined

MACRO_PATH = Path(__file__).resolve().parents[1] / "dbt/macros/bronze_source.sql"


def _compiler_error(message: str) -> None:
    """dbt 컴파일 오류를 격리된 테스트의 예외로 전달한다."""
    raise ValueError(message)


def _unexpected_query(_: str) -> None:
    """계약 검증 이전 또는 파싱 중 DB 조회가 발생하면 실패한다."""
    pytest.fail("No database query should run before contract validation")


def _macros(*, execute: bool, variables: dict, run_query=_unexpected_query):
    """실제 매크로를 명시적 dbt 실행 환경으로 렌더링한다."""
    return Environment(undefined=StrictUndefined, extensions=["jinja2.ext.do"]).from_string(
        MACRO_PATH.read_text(encoding="utf-8")
    ).make_module({
        "execute": execute,
        "var": lambda name, default=None: variables.get(name, default),
        "exceptions": SimpleNamespace(raise_compiler_error=_compiler_error),
        "run_query": run_query,
        "replay_boundary_predicate": lambda: "",
        "env_var": lambda _: "test-bucket",
        "log": lambda *args, **kwargs: None,
    })


def test_parse_without_vars_never_queries_or_checks_supported_tables() -> None:
    """파싱은 계약 변수와 DB 없이 무해한 빈 SQL을 반환한다."""
    macros = _macros(execute=False, variables={})
    assert "select 1 where false" in macros.bronze_source("not_in_contract")
    assert "select 1 where false" in macros.bronze_empty_relation("not_in_contract")
    assert macros.validate_bronze_catalog() == ""


@pytest.mark.parametrize("missing", ["bronze_schemas", "bronze_schema_versions"])
@pytest.mark.parametrize("operation", ["source", "validate"])
def test_execute_requires_contract_before_querying_catalog(missing: str, operation: str) -> None:
    """파일 유무를 조회하기 전에 누락된 계약 변수를 명확히 거절한다."""
    variables = {"bronze_schemas": {"orders": []}, "bronze_schema_versions": [3]}
    del variables[missing]
    macros = _macros(execute=True, variables=variables)
    with pytest.raises(ValueError, match="SOURCE_CONTRACT_ERROR: missing bronze_schemas"):
        if operation == "source":
            macros.bronze_source("orders")
        else:
            macros.validate_bronze_catalog()


def test_source_tables_and_versions_come_from_vars() -> None:
    """고정 목록에 없는 테이블·버전도 전달된 계약에 따라 판정한다."""
    variables = {"bronze_schemas": {"new_table": [["id", "integer"]]},
                 "bronze_schema_versions": [4]}
    macros = _macros(
        execute=True, variables=variables,
        run_query=lambda _: SimpleNamespace(rows=[("new.parquet", 4)]),
    )
    assert "s3://test-bucket/new.parquet" in macros.bronze_source("new_table")
    with pytest.raises(ValueError, match="unsupported source_table=orders"):
        macros.bronze_source("orders")
    old_version = _macros(
        execute=True, variables=variables,
        run_query=lambda _: SimpleNamespace(rows=[("old.parquet", 3)]),
    )
    with pytest.raises(ValueError, match="unsupported schema_version=3"):
        old_version.bronze_source("new_table")


def test_empty_relation_uses_provided_columns_and_order() -> None:
    """빈 원천의 컬럼·타입·순서를 SQL의 별도 목록 없이 전달된 정의로 만든다."""
    macros = _macros(execute=True, variables={
        "bronze_schemas": {"new_table": [["second", "boolean"], ["first", "integer"]]},
    })
    sql = macros.bronze_empty_relation("new_table")
    assert "cast(null as boolean) as second" in sql
    assert "cast(null as integer) as first" in sql
    assert sql.index("as second") < sql.index("as first")
    assert "where false" in sql


def test_catalog_validation_uses_provided_table_set_and_versions() -> None:
    """카탈로그 검증 쿼리와 순회 대상도 Python 계약 변수에서 가져온다."""
    queries = []

    def _query(sql: str):
        """실제 DB 대신 카탈로그 검증 SQL을 수집한다."""
        queries.append(sql)
        return SimpleNamespace(rows=[])

    macros = _macros(execute=True, variables={
        "bronze_schemas": {"new_table": [["id", "integer"]]},
        "bronze_schema_versions": [3, 4],
    }, run_query=_query)
    macros.validate_bronze_catalog()
    assert len(queries) == 2
    assert "schema_version not in (3, 4)" in queries[0]
    assert "source_table = 'new_table'" in queries[1]
