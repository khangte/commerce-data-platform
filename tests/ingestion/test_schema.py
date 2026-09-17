"""Bronze Schema Version 지원 계약을 검증한다."""

from __future__ import annotations

import pytest

from src.ingestion.schema import SourceContractError, assert_supported_schema_version


def test_schema_version_three_is_supported_and_old_version_is_contract_error() -> None:
    """Version 3만 허용하고 이전 Version은 SOURCE_CONTRACT_ERROR로 막는다."""
    assert_supported_schema_version(3)
    with pytest.raises(SourceContractError, match="SOURCE_CONTRACT_ERROR"):
        assert_supported_schema_version(2)
