"""- Bronze Schema Version 지원 범위와 Source Contract 오류를 정의한다."""

from __future__ import annotations

SUPPORTED_BRONZE_SCHEMA_VERSIONS = frozenset({1})


class SourceContractError(RuntimeError):
    """- 지원하지 않는 Source·Bronze 계약을 dbt 이전에 차단한다."""


def assert_supported_schema_version(schema_version: int) -> None:
    """- Catalog·Reader가 지원하는 Bronze Schema Version만 허용한다."""
    if schema_version not in SUPPORTED_BRONZE_SCHEMA_VERSIONS:
        raise SourceContractError(f"SOURCE_CONTRACT_ERROR: unsupported schema_version={schema_version}")
