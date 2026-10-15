"""Python Bronze 계약과 호출자 변수를 하나의 dbt JSON으로 만든다."""

from __future__ import annotations

import json
from collections.abc import Mapping

import pyarrow as pa

from src.ingestion.contract.schema import SUPPORTED_BRONZE_SCHEMA_VERSIONS
from src.ingestion.contract.tables import TABLE_CONFIGS

_DUCKDB_TYPES = {
    pa.string(): "varchar",
    pa.int32(): "integer",
    pa.bool_(): "boolean",
    pa.timestamp("us", tz="UTC"): "timestamptz",
    pa.decimal128(14, 2): "decimal(14, 2)",
}
_RESERVED_VARS = frozenset({"bronze_schemas", "bronze_schema_versions"})


def _duckdb_type(arrow_type: pa.DataType) -> str:
    """정확히 지원하는 Arrow 타입만 DuckDB 타입 문자열로 변환한다."""
    try:
        return _DUCKDB_TYPES[arrow_type]
    except KeyError as error:
        raise ValueError(f"Unsupported Bronze Arrow type: {arrow_type}") from error


def dbt_vars_json(extra: Mapping[str, object] | None = None) -> str:
    """기술 컬럼을 포함한 Bronze 계약과 예약 키를 침범하지 않는 변수를 병합한다."""
    extra = {} if extra is None else extra
    reserved = _RESERVED_VARS.intersection(extra)
    if reserved:
        raise ValueError(f"Reserved dbt vars cannot be overridden: {', '.join(sorted(reserved))}")
    schemas = {
        name: [[field.name, _duckdb_type(field.type)] for field in config.bronze_schema]
        for name, config in TABLE_CONFIGS.items()
    }
    return json.dumps({
        "bronze_schemas": schemas,
        "bronze_schema_versions": sorted(SUPPORTED_BRONZE_SCHEMA_VERSIONS),
        **extra,
    })


if __name__ == "__main__":
    print(dbt_vars_json())
