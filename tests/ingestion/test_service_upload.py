"""Bronze·격리 파일 게시에서 로컬 증적 사용과 재수신 생략을 검증한다."""

import hashlib
import json
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from src.ingestion import service
from src.ingestion.bronze.storage import SeaweedFSSettings, StoredObject
from src.ingestion.contract.tables import table_config
from src.ingestion.control.metadata import CursorPosition


@pytest.mark.parametrize("quarantine", [False, True])
def test_publish_uses_local_evidence_without_downloading(monkeypatch, tmp_path, quarantine) -> None:
    """체크섬 업로드 후 로컬 파일의 크기·해시·행 수를 매니페스트에 기록한다."""
    now = datetime(2026, 10, 15, tzinfo=UTC)
    request = service.TableIngestionRequest("orders", "warehouse__20261015T000000Z", now)
    cursor = CursorPosition(now, ("order-1",))
    run = SimpleNamespace(
        run_id=uuid.uuid4(), logical_date=now, watermark_before=cursor, extract_upper_bound=cursor
    )
    path = tmp_path / "data.parquet"
    payload = b"local artifact"
    path.write_bytes(payload)
    checksum = hashlib.sha256(payload).hexdigest()
    artifact = SimpleNamespace(path=path, row_count=7, error_counts={"INVALID_STATUS": 7})
    upload = Mock(return_value=StoredObject("unused", len(payload), checksum))
    manifest_upload = Mock()
    client = Mock()
    monkeypatch.setattr(service, "upload_new_file", upload)
    monkeypatch.setattr(service, "upload_new_bytes", manifest_upload)
    monkeypatch.setattr(service, "table_logical_hash", Mock(return_value="a" * 64))
    monkeypatch.setattr(
        "src.ingestion.bronze.storage.seaweedfs_s3_client", Mock(return_value=client)
    )
    storage = SeaweedFSSettings("localhost", 8333, "access", "secret", "lake")
    publish = service._publish_quarantine if quarantine else service._upload_and_verify_bronze
    result = publish(storage, request, run, table_config("orders"), artifact, now)
    manifest = json.loads(manifest_upload.call_args.args[2])
    assert manifest["object_size"] == len(payload)
    assert manifest["content_sha256"] == checksum
    assert manifest["row_count"] == artifact.row_count == result.row_count
    assert manifest["object_key"] == result.object_key
    upload.assert_called_once_with(storage, result.object_key, path)
    client.get_object.assert_not_called()
    if quarantine:
        assert manifest["error_counts"] == result.error_counts == artifact.error_counts
    else:
        assert result.content_sha256 == checksum
