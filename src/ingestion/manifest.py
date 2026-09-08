"""검증된 Bronze Object의 공개 증적 Manifest를 결정적으로 만든다."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from src.ingestion.metadata import CursorPosition
from src.ingestion.tables import table_config

MANIFEST_VERSION = 1
VERIFIED_OBJECT_STATE = "VERIFIED"

_REQUIRED_BRONZE_MANIFEST_FIELDS = (
    "batch_id",
    "run_id",
    "source_table",
    "logical_date",
    "watermark_before",
    "extract_upper_bound",
    "object_key",
    "object_size",
    "content_sha256",
    "logical_hash",
    "row_count",
    "schema_version",
)


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


def parse_bronze_manifest_payload(manifest_bytes: bytes, *, expected_object_key: str) -> dict[str, object]:
    """Bronze Manifest JSON Byte를 재검증에 필요한 필드·Type 계약으로 파싱한다."""
    try:
        payload = json.loads(manifest_bytes)
        if not isinstance(payload, dict):
            raise TypeError("manifest must be a JSON object")
        if (
            payload["manifest_version"] != MANIFEST_VERSION
            or payload["object_state"] != VERIFIED_OBJECT_STATE
            or payload["object_key"] != expected_object_key
        ):
            raise ValueError("manifest object identity is invalid")
        uuid.UUID(payload["run_id"])
        if any(name not in payload for name in _REQUIRED_BRONZE_MANIFEST_FIELDS):
            raise ValueError("manifest fields are incomplete")
        _assert_bronze_manifest_types(payload)
        return payload
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError("Bronze manifest is invalid") from error


def _assert_bronze_manifest_types(payload: dict[str, object]) -> None:
    """재검증에 쓰는 Manifest 식별자·Count·Hash·Cursor의 Type과 범위를 확인한다."""
    text_fields = ("batch_id", "source_table", "logical_date", "object_key", "content_sha256", "logical_hash")
    if any(not isinstance(payload[name], str) or not payload[name].strip() for name in text_fields):
        raise ValueError("manifest identifiers must be non-empty strings")
    if any(
        not isinstance(payload[name], int) or isinstance(payload[name], bool) or payload[name] < 0
        for name in ("object_size", "row_count")
    ):
        raise ValueError("manifest size and row_count must be non-negative integers")
    if (
        not isinstance(payload["schema_version"], int)
        or isinstance(payload["schema_version"], bool)
        or payload["schema_version"] <= 0
    ):
        raise ValueError("manifest schema_version must be a positive integer")
    for name in ("content_sha256", "logical_hash"):
        value = payload[name]
        if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
            raise ValueError(f"manifest {name} must be a lowercase SHA-256 hex")
    table_config(payload["source_table"])
    _cursor_from_manifest_payload(payload["watermark_before"])
    _cursor_from_manifest_payload(payload["extract_upper_bound"])
    _utc_datetime_from_manifest(payload["logical_date"])


def _cursor_from_manifest_payload(value: object) -> CursorPosition:
    """Manifest JSON Cursor를 Metadata와 비교 가능한 엄격한 Cursor 계약으로 변환한다."""
    if not isinstance(value, dict) or set(value) != {"timestamp", "keys"}:
        raise ValueError("manifest cursor must contain timestamp and keys")
    timestamp = value["timestamp"]
    keys = value["keys"]
    if timestamp is not None and not isinstance(timestamp, str):
        raise ValueError("manifest cursor timestamp must be a UTC string or null")
    if not isinstance(keys, list):
        raise TypeError("manifest cursor keys must be an array")
    return CursorPosition(_utc_datetime_from_manifest(timestamp) if timestamp is not None else None, tuple(keys))


def _utc_datetime_from_manifest(value: str) -> datetime:
    """ISO-8601 문자열을 UTC Timestamp로 읽고 비 UTC 값은 거부한다."""
    result = datetime.fromisoformat(value)
    if result.tzinfo is None or result.utcoffset() != UTC.utcoffset(None):
        raise ValueError("manifest timestamps must be normalized to UTC")
    return result


def _assert_utc(value: datetime, name: str) -> None:
    """Manifest의 모든 Timestamp가 UTC로 정규화됐는지 검증한다."""
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be normalized to UTC")


def _assert_sha256(value: str, name: str) -> None:
    """검증 증적 Hash가 소문자 SHA-256 Hex인지 확인한다."""
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a lowercase SHA-256 hex value")


@dataclass(frozen=True)
class QuarantineManifest:
    """검증된 불변 Quarantine Object의 공개 증적을 표현한다."""

    batch_id: str
    run_id: uuid.UUID
    source_table: str
    object_key: str
    object_size: int
    content_sha256: str
    row_count: int
    error_counts: dict[str, int]
    created_at: datetime
    manifest_version: int = MANIFEST_VERSION
    object_state: str = VERIFIED_OBJECT_STATE

    def __post_init__(self) -> None:
        """공개 식별자·UTC 시각·검증 값·오류 집계를 확인한다."""
        if not self.batch_id.strip() or not self.source_table.strip() or not self.object_key.strip():
            raise ValueError("Quarantine manifest identifiers must not be empty")
        if self.object_size < 0 or self.row_count < 0:
            raise ValueError("Quarantine manifest size and row_count must be non-negative")
        if self.manifest_version != MANIFEST_VERSION or self.object_state != VERIFIED_OBJECT_STATE:
            raise ValueError("Quarantine manifest must be VERIFIED at the supported version")
        if any(not code or count < 0 for code, count in self.error_counts.items()):
            raise ValueError("Quarantine error_counts must contain non-negative named counts")
        _assert_utc(self.created_at, "created_at")
        _assert_sha256(self.content_sha256, "content_sha256")

    def as_dict(self) -> dict[str, object]:
        """Raw Payload 없이 검증에 필요한 Canonical 공개 JSON을 만든다."""
        return {
            "manifest_version": self.manifest_version,
            "object_state": self.object_state,
            "object_type": "QUARANTINE",
            "batch_id": self.batch_id,
            "run_id": str(self.run_id),
            "source_table": self.source_table,
            "object_key": self.object_key,
            "object_size": self.object_size,
            "content_sha256": self.content_sha256,
            "row_count": self.row_count,
            "error_counts": dict(sorted(self.error_counts.items())),
            "created_at": self.created_at.isoformat(),
        }

    def to_bytes(self) -> bytes:
        """Object Upload에 쓸 UTF-8 Canonical JSON Byte를 반환한다."""
        return json.dumps(
            self.as_dict(), ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
