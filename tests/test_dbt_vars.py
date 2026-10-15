"""Python Bronze 계약에서 만드는 dbt 변수와 병합 경계를 검증한다."""

import json
import subprocess
import sys

import pyarrow as pa
import pytest

from src.ingestion.contract.schema import SUPPORTED_BRONZE_SCHEMA_VERSIONS
from src.ingestion.contract.tables import TABLE_CONFIGS
from src.warehouse.dbt_vars import _duckdb_type, dbt_vars_json


def test_vars_preserve_all_table_columns_order_and_technical_fields() -> None:
    """모든 테이블의 원천·기술 컬럼 순서와 지원 버전을 계약에서 가져온다."""
    variables = json.loads(dbt_vars_json())
    assert set(variables["bronze_schemas"]) == set(TABLE_CONFIGS)
    for name, config in TABLE_CONFIGS.items():
        assert variables["bronze_schemas"][name] == [
            [field.name, _duckdb_type(field.type)] for field in config.bronze_schema
        ]
    assert variables["bronze_schema_versions"] == sorted(SUPPORTED_BRONZE_SCHEMA_VERSIONS)


@pytest.mark.parametrize(
    ("arrow_type", "expected"),
    [
        (pa.string(), "varchar"), (pa.int32(), "integer"), (pa.bool_(), "boolean"),
        (pa.timestamp("us", tz="UTC"), "timestamptz"),
        (pa.decimal128(14, 2), "decimal(14, 2)"),
    ],
)
def test_arrow_type_mapping_is_explicit(arrow_type: pa.DataType, expected: str) -> None:
    """지원하는 Arrow 타입만 정해진 DuckDB 타입으로 변환한다."""
    assert _duckdb_type(arrow_type) == expected


@pytest.mark.parametrize(
    "arrow_type",
    [pa.int64(), pa.timestamp("ms", tz="UTC"), pa.timestamp("us"),
     pa.timestamp("us", tz="Asia/Seoul"), pa.decimal128(14, 3), pa.decimal128(15, 2)],
)
def test_unknown_arrow_type_fails(arrow_type: pa.DataType) -> None:
    """정밀도·시간 단위·시간대가 다른 타입을 조용히 허용하지 않는다."""
    with pytest.raises(ValueError, match="Unsupported Bronze Arrow type"):
        _duckdb_type(arrow_type)


def test_vars_merge_canary_and_replay_without_losing_bronze_contract() -> None:
    """Canary와 재처리 경계를 Bronze 스키마와 같은 변수 묶음에 보존한다."""
    extra = {"publish_gate_canary": True, "bronze_as_of": "2026-10-01T00:00:00Z"}
    merged = json.loads(dbt_vars_json(extra))
    assert all(merged[key] == value for key, value in extra.items())
    assert merged["bronze_schemas"] and merged["bronze_schema_versions"]
    assert extra == {"publish_gate_canary": True, "bronze_as_of": "2026-10-01T00:00:00Z"}


@pytest.mark.parametrize("key", ["bronze_schemas", "bronze_schema_versions"])
def test_reserved_vars_cannot_be_overridden(key: str) -> None:
    """호출자가 생성된 Bronze 계약 변수를 덮어쓰지 못한다."""
    with pytest.raises(ValueError, match="Reserved dbt vars"):
        dbt_vars_json({key: {}})


def test_module_command_prints_the_same_json() -> None:
    """직접 dbt 명령도 같은 Python 변수 생성 경로를 사용한다."""
    result = subprocess.run(
        [sys.executable, "-m", "src.warehouse.dbt_vars"],
        capture_output=True, text=True, check=True,
    )
    assert json.loads(result.stdout) == json.loads(dbt_vars_json())
