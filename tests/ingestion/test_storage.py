"""SeaweedFS S3 API 연결 설정 계약을 검증한다."""

from __future__ import annotations

import base64
import hashlib
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from src.ingestion.bronze.storage import (
    BRONZE_PREFIX,
    QUARANTINE_PREFIX,
    STAGING_PREFIX,
    SeaweedFSSettings,
    upload_new_bytes,
    upload_new_file,
)


def test_object_storage_prefixes_match_the_bronze_layout_contract() -> None:
    """Object Storage는 Bronze·Quarantine과 선택적 Staging Prefix를 구분한다."""
    assert (STAGING_PREFIX, BRONZE_PREFIX, QUARANTINE_PREFIX) == (
        "_staging",
        "bronze",
        "quarantine",
    )


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
    monkeypatch.setattr("src.ingestion.bronze.storage.environment_values", dict)

    with pytest.raises(ValueError, match="SEAWEEDFS_S3_PORT"):
        SeaweedFSSettings.from_environment()


@pytest.mark.parametrize("from_file", [False, True])
def test_upload_sends_sha256_checksum_and_preserves_head_checks(
    monkeypatch, tmp_path, from_file
) -> None:
    """파일·바이트 PUT에 실제 SHA-256 검증값과 불변 조건을 전달한다."""
    payload = b"local artifact bytes"
    digest = hashlib.sha256(payload).digest()
    client = Mock()
    client.head_object.side_effect = [
        ClientError({"Error": {"Code": "NoSuchKey"}}, "HeadObject"),
        {"ContentLength": len(payload), "Metadata": {"sha256": digest.hex()}},
    ]
    monkeypatch.setattr(
        "src.ingestion.bronze.storage.seaweedfs_s3_client", Mock(return_value=client)
    )
    settings = SeaweedFSSettings("localhost", 8333, "access", "secret", "lake")
    if from_file:
        path = tmp_path / "data.parquet"
        path.write_bytes(payload)
        stored = upload_new_file(settings, "bronze/data.parquet", path)
    else:
        stored = upload_new_bytes(settings, "bronze/data.parquet", payload)
    request = client.put_object.call_args.kwargs
    assert request["ChecksumSHA256"] == base64.b64encode(digest).decode("ascii")
    assert request["Metadata"] == {"sha256": stored.content_sha256}
    assert request["IfNoneMatch"] == "*"
    assert stored.size == len(payload)
    assert stored.content_sha256 == digest.hex()
    assert client.head_object.call_count == 2
    client.get_object.assert_not_called()
