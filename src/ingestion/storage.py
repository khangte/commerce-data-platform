"""SeaweedFS S3 API의 Path-style 연결과 Bucket 준비를 제공한다."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import ClientError

from src.common.database import environment_values

STAGING_PREFIX = "_staging"
BRONZE_PREFIX = "bronze"
QUARANTINE_PREFIX = "quarantine"


@dataclass(frozen=True)
class StoredObject:
    """최종 Object의 Key·크기·SHA-256 검증 결과다."""

    key: str
    size: int
    content_sha256: str


@dataclass(frozen=True)
class VerifiedParquetObject(StoredObject):
    """최종 Parquet Object에서 다시 확인한 Row Count를 포함한다."""

    row_count: int


@dataclass(frozen=True)
class SeaweedFSSettings:
    """로컬 환경 변수로 제공되는 SeaweedFS S3 API 연결 정보다."""

    host: str
    port: int
    access_key: str
    secret_key: str
    bucket: str

    @classmethod
    def from_environment(cls) -> SeaweedFSSettings:
        """환경 변수와 로컬 `.env`에서 필수 SeaweedFS 연결 값을 읽는다."""
        values = environment_values()
        required = (
            "SEAWEEDFS_S3_PORT",
            "SEAWEEDFS_ACCESS_KEY",
            "SEAWEEDFS_SECRET_KEY",
            "SEAWEEDFS_BUCKET",
        )
        missing = [name for name in required if not values.get(name)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        return cls(
            host=values.get("SEAWEEDFS_HOST", "localhost"),
            port=int(values["SEAWEEDFS_S3_PORT"]),
            access_key=values["SEAWEEDFS_ACCESS_KEY"],
            secret_key=values["SEAWEEDFS_SECRET_KEY"],
            bucket=values["SEAWEEDFS_BUCKET"],
        )

    @property
    def endpoint_url(self) -> str:
        """TLS 없이 로컬 S3 API에 연결할 HTTP Endpoint URL을 반환한다."""
        return f"http://{self.host}:{self.port}"


def seaweedfs_s3_client(settings: SeaweedFSSettings):
    """Virtual-host 대신 Path-style 주소 지정을 강제한 S3 Client를 반환한다."""
    return boto3.client(
        "s3",
        endpoint_url=settings.endpoint_url,
        aws_access_key_id=settings.access_key,
        aws_secret_access_key=settings.secret_key,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def ensure_bucket(settings: SeaweedFSSettings) -> None:
    """Configured Bucket이 없으면 생성하고 이미 있으면 그대로 사용한다."""
    client = seaweedfs_s3_client(settings)
    try:
        client.head_bucket(Bucket=settings.bucket)
        return
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code")
        if error_code not in {"404", "NoSuchBucket", "NotFound"}:
            raise
    client.create_bucket(Bucket=settings.bucket)


def sha256_file(path: Path) -> str:
    """Local 파일 전체 Byte의 SHA-256 Hex를 스트리밍으로 계산한다."""
    digest = hashlib.sha256()
    with path.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def upload_new_file(settings: SeaweedFSSettings, key: str, path: Path) -> StoredObject:
    """새 Final Key에만 Local 파일을 올리고 HEAD의 크기·Hash를 검증한다."""
    content_sha256 = sha256_file(path)
    size = path.stat().st_size
    with path.open("rb") as file:
        return _upload_new(settings, key, file, size, content_sha256)


def upload_new_bytes(settings: SeaweedFSSettings, key: str, payload: bytes) -> StoredObject:
    """새 Final Key에만 Byte를 올리고 HEAD의 크기·Hash를 검증한다."""
    return _upload_new(settings, key, payload, len(payload), hashlib.sha256(payload).hexdigest())


def verify_parquet_object(
    settings: SeaweedFSSettings, object: StoredObject
) -> VerifiedParquetObject:
    """Final Object를 재수신해 Byte Hash와 Parquet Row Count를 함께 확인한다."""
    client = seaweedfs_s3_client(settings)
    payload = client.get_object(Bucket=settings.bucket, Key=object.key)["Body"].read()
    actual_hash = hashlib.sha256(payload).hexdigest()
    if actual_hash != object.content_sha256:
        raise RuntimeError(f"Object checksum differs from expected value: {object.key}")
    parquet_file = pq.ParquetFile(pa.BufferReader(payload))
    return VerifiedParquetObject(
        key=object.key,
        size=len(payload),
        content_sha256=actual_hash,
        row_count=parquet_file.metadata.num_rows,
    )


def read_object_bytes(settings: SeaweedFSSettings, key: str) -> bytes:
    """지정 Final Object의 전체 Byte를 검증·Manifest 처리용으로 읽는다."""
    return seaweedfs_s3_client(settings).get_object(Bucket=settings.bucket, Key=key)["Body"].read()


def list_object_keys(settings: SeaweedFSSettings, prefix: str) -> tuple[str, ...]:
    """지정 Prefix 아래의 Object Key를 페이지 처리해 정렬 반환한다."""
    client = seaweedfs_s3_client(settings)
    keys: list[str] = []
    continuation: str | None = None
    while True:
        request = {"Bucket": settings.bucket, "Prefix": prefix}
        if continuation is not None:
            request["ContinuationToken"] = continuation
        response = client.list_objects_v2(**request)
        keys.extend(item["Key"] for item in response.get("Contents", ()))
        if not response.get("IsTruncated"):
            return tuple(sorted(keys))
        continuation = response.get("NextContinuationToken")


def stored_object_from_head(settings: SeaweedFSSettings, key: str) -> StoredObject:
    """HEAD의 크기·SHA-256 Metadata를 검증 가능한 Object 증적으로 복원한다."""
    head = seaweedfs_s3_client(settings).head_object(Bucket=settings.bucket, Key=key)
    metadata = head.get("Metadata")
    checksum = metadata.get("sha256") if isinstance(metadata, Mapping) else None
    size = head.get("ContentLength")
    if not isinstance(checksum, str) or not isinstance(size, int):
        raise TypeError(f"Object HEAD lacks required checksum metadata: {key}")
    return StoredObject(key=key, size=size, content_sha256=checksum)


def _upload_new(
    settings: SeaweedFSSettings,
    key: str,
    body: object,
    expected_size: int,
    content_sha256: str,
) -> StoredObject:
    """기존 Final Key를 거부하고 조건부 PUT 뒤 HEAD Metadata를 검증한다."""
    if not key or key.startswith("/"):
        raise ValueError("Object key must be a non-empty relative path")
    client = seaweedfs_s3_client(settings)
    _reject_existing_object(client, settings.bucket, key)
    try:
        client.put_object(
            Bucket=settings.bucket,
            Key=key,
            Body=body,
            Metadata={"sha256": content_sha256},
            IfNoneMatch="*",
        )
    except ClientError as error:
        if error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 412:
            raise FileExistsError(f"Final object already exists: {key}") from error
        raise
    head = client.head_object(Bucket=settings.bucket, Key=key)
    _assert_object_head(key, head, expected_size, content_sha256)
    return StoredObject(key=key, size=expected_size, content_sha256=content_sha256)


def _reject_existing_object(client: Any, bucket: str, key: str) -> None:
    """명시적인 재실행 충돌을 조건부 PUT 이전에 이해하기 쉬운 오류로 바꾼다."""
    try:
        client.head_object(Bucket=bucket, Key=key)
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code")
        if error_code in {"404", "NoSuchKey", "NotFound"}:
            return
        raise
    raise FileExistsError(f"Final object already exists: {key}")


def _assert_object_head(
    key: str, head: Mapping[str, object], expected_size: int, expected_sha256: str
) -> None:
    """HEAD가 PUT 직후 기대한 크기와 사용자 Metadata Hash를 보존하는지 확인한다."""
    if head.get("ContentLength") != expected_size:
        raise RuntimeError(f"Object size differs from expected value: {key}")
    metadata = head.get("Metadata")
    if not isinstance(metadata, Mapping) or metadata.get("sha256") != expected_sha256:
        raise RuntimeError(f"Object checksum metadata differs from expected value: {key}")
