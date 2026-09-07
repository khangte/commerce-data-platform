"""SeaweedFS S3 API 연결 설정 계약을 검증한다."""

from __future__ import annotations

import pytest

from src.ingestion.storage import (
    BRONZE_PREFIX,
    QUARANTINE_PREFIX,
    STAGING_PREFIX,
    SeaweedFSSettings,
)


def test_object_storage_prefixes_match_the_bronze_layout_contract() -> None:
    """Object Storage는 Bronze·Quarantine과 선택적 Staging Prefix를 구분한다."""
    assert (STAGING_PREFIX, BRONZE_PREFIX, QUARANTINE_PREFIX) == ("_staging", "bronze", "quarantine")


def test_seaweedfs_settings_uses_default_local_host() -> None:
    """Host를 생략하면 로컬 S3 API Endpoint를 만든다."""
    settings = SeaweedFSSettings(
        host="localhost",
        port=8333,
        access_key="access",
        secret_key="secret",
        bucket="commerce-lake",
    )

    assert settings.endpoint_url == "http://localhost:8333"


def test_seaweedfs_settings_requires_all_credentials(monkeypatch) -> None:
    """필수 S3 Credential 또는 Bucket이 없으면 환경 설정을 거부한다."""
    monkeypatch.setattr("src.ingestion.storage.environment_values", dict)

    with pytest.raises(ValueError, match="SEAWEEDFS_S3_PORT"):
        SeaweedFSSettings.from_environment()
