"""SeaweedFS S3 API의 Path-style 연결과 Bucket 준비를 제공한다."""

from __future__ import annotations

from dataclasses import dataclass

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from src.common.database import environment_values

STAGING_PREFIX = "_staging"
BRONZE_PREFIX = "bronze"
QUARANTINE_PREFIX = "quarantine"


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
