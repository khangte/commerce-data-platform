"""Bronze Schema Version 지원 계약을 검증한다."""

from __future__ import annotations

import pytest

from src.ingestion.schema import SourceContractError, assert_supported_schema_version


def test_schema_version_one_is_supported_and_unknown_version_is_contract_error() -> None:
    """Version 1만 허용하고 그 외 Version은 SOURCE_CONTRACT_ERROR로 막는다."""
    assert_supported_schema_version(1)
    with pytest.raises(SourceContractError, match="SOURCE_CONTRACT_ERROR"):
        assert_supported_schema_version(2)
