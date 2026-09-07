"""Bronze Manifest의 공개 증적과 Commit 경계 계약을 검증한다."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

import pytest

from src.ingestion.manifest import VERIFIED_OBJECT_STATE, BronzeManifest
from src.ingestion.metadata import CursorPosition


def test_manifest_serializes_verified_object_evidence_without_commit_secrets_or_paths() -> None:
    """Manifest는 VERIFIED Object 증적만 남기며 Metadata Commit 상태를 포함하지 않는다."""
    manifest = BronzeManifest(
        batch_id="warehouse__20260907T000000Z",
        run_id=uuid.UUID("5f809cf8-92e8-4c29-a843-b771a288e5bb"),
        source_table="orders",
        logical_date=datetime(2026, 9, 7, tzinfo=UTC),
        watermark_before=CursorPosition(None),
        extract_upper_bound=CursorPosition(datetime(2026, 9, 7, 0, 0, 1, tzinfo=UTC), ("o-1",)),
        object_key="bronze/orders/ingestion_date=2026-09-07/batch_id=batch/data.parquet",
        object_size=123,
        content_sha256="a" * 64,
        logical_hash="b" * 64,
        row_count=2,
        created_at=datetime(2026, 9, 7, tzinfo=UTC),
        schema_version=1,
    )

    payload = json.loads(manifest.to_bytes())

    assert payload["object_state"] == VERIFIED_OBJECT_STATE
    assert payload["watermark_before"] == {"timestamp": None, "keys": []}
    assert payload["extract_upper_bound"]["keys"] == ["o-1"]
    assert "status" not in payload
    assert "local_path" not in payload
    assert "credential" not in payload


def test_manifest_rejects_any_object_state_other_than_verified() -> None:
    """VERIFIED는 Metadata COMMITTED와 별개이므로 다른 Manifest 상태를 허용하지 않는다."""
    with pytest.raises(ValueError, match="VERIFIED"):
        BronzeManifest(
            batch_id="batch",
            run_id=uuid.uuid4(),
            source_table="orders",
            logical_date=datetime(2026, 9, 7, tzinfo=UTC),
            watermark_before=CursorPosition(None),
            extract_upper_bound=CursorPosition(datetime(2026, 9, 7, tzinfo=UTC), ("o-1",)),
            object_key="bronze/orders/data.parquet",
            object_size=1,
            content_sha256="a" * 64,
            logical_hash="b" * 64,
            row_count=1,
            created_at=datetime(2026, 9, 7, tzinfo=UTC),
            schema_version=1,
            object_state="COMMITTED",
        )
