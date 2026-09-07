"""검증된 Bronze Object의 공개 증적 Manifest를 결정적으로 만든다."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

from src.ingestion.metadata import CursorPosition

MANIFEST_VERSION = 1
VERIFIED_OBJECT_STATE = "VERIFIED"


@dataclass(frozen=True)
class BronzeManifest:
    """Metadata Commit 전 검증을 마친 불변 Bronze Object의 공개 증적이다."""

    batch_id: str
    run_id: uuid.UUID
    source_table: str
    logical_date: datetime
    watermark_before: CursorPosition
    extract_upper_bound: CursorPosition
    object_key: str
    object_size: int
    content_sha256: str
    logical_hash: str
    row_count: int
    created_at: datetime
    schema_version: int
    manifest_version: int = MANIFEST_VERSION
    object_state: str = VERIFIED_OBJECT_STATE

    def __post_init__(self) -> None:
        """Manifest의 공개 식별자·UTC 시각·검증 값과 상태를 확인한다."""
        if (
            not self.batch_id.strip()
            or not self.source_table.strip()
            or not self.object_key.strip()
        ):
            raise ValueError("Manifest identifiers must not be empty")
        _assert_utc(self.logical_date, "logical_date")
        _assert_utc(self.created_at, "created_at")
        if self.object_size < 0 or self.row_count < 0:
            raise ValueError("Manifest size and row_count must be non-negative")
        if self.schema_version <= 0 or self.manifest_version != MANIFEST_VERSION:
            raise ValueError("Unsupported manifest or schema version")
        if self.object_state != VERIFIED_OBJECT_STATE:
            raise ValueError("Manifest object_state must be VERIFIED before metadata commit")
        _assert_sha256(self.content_sha256, "content_sha256")
        _assert_sha256(self.logical_hash, "logical_hash")

    def as_dict(self) -> dict[str, object]:
        """Credential·경로·Metadata Commit 상태 없는 안정적인 공개 JSON을 만든다."""
        return {
            "manifest_version": self.manifest_version,
            "schema_version": self.schema_version,
            "object_state": self.object_state,
            "batch_id": self.batch_id,
            "run_id": str(self.run_id),
            "source_table": self.source_table,
            "logical_date": self.logical_date.isoformat(),
            "watermark_before": self.watermark_before.as_metadata_json(),
            "extract_upper_bound": self.extract_upper_bound.as_metadata_json(),
            "object_key": self.object_key,
            "object_size": self.object_size,
            "content_sha256": self.content_sha256,
            "logical_hash": self.logical_hash,
            "row_count": self.row_count,
            "created_at": self.created_at.isoformat(),
        }

    def to_bytes(self) -> bytes:
        """Object Upload와 Hash에 쓸 UTF-8 Canonical JSON Byte를 반환한다."""
        return json.dumps(
            self.as_dict(), ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")


def _assert_utc(value: datetime, name: str) -> None:
    """Manifest의 모든 Timestamp가 UTC로 정규화됐는지 검증한다."""
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be normalized to UTC")


def _assert_sha256(value: str, name: str) -> None:
    """검증 증적 Hash가 소문자 SHA-256 Hex인지 확인한다."""
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a lowercase SHA-256 hex value")
