"""신뢰성 시나리오에서 기존 함수 경계에 장애를 주입한다."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Literal

from botocore.exceptions import ClientError
from psycopg import OperationalError
from pytest import MonkeyPatch

from src.ingestion import extract, metadata, service
from src.ingestion import storage as storage_module
from src.ingestion.storage import SeaweedFSSettings, read_object_bytes, seaweedfs_s3_client

ManifestCorruptionKind = Literal["CHECKSUM", "ROW_RANGE", "SCHEMA_VERSION"]


class InjectedCrash(BaseException):
    """예외 처리 경로를 의도적으로 우회하는 테스트 전용 프로세스 중단이다."""


@contextmanager
def fail_object_upload(monkeypatch: MonkeyPatch, *, on_call: int = 1) -> Iterator[None]:
    """지정한 번째 Bronze Object 업로드에서 S3 ClientError를 발생시킨다."""
    if on_call <= 0:
        raise ValueError("on_call must be greater than zero")
    original_upload = service.upload_new_file
    calls = 0

    def _fail_on_requested_call(*args: object, **kwargs: object) -> object:
        """호출 횟수를 세고 목표 호출에서만 S3 업로드 실패를 재현한다."""
        nonlocal calls
        calls += 1
        if calls == on_call:
            raise ClientError(
                {
                    "Error": {"Code": "ServiceUnavailable", "Message": "Injected upload failure"},
                    "ResponseMetadata": {"HTTPStatusCode": 503},
                },
                "PutObject",
            )
        return original_upload(*args, **kwargs)

    with monkeypatch.context() as scoped:
        scoped.setattr(storage_module, "upload_new_file", _fail_on_requested_call)
        scoped.setattr(service, "upload_new_file", _fail_on_requested_call)
        yield


@contextmanager
def fail_metadata_commit(monkeypatch: MonkeyPatch) -> Iterator[None]:
    """검증된 Bronze·Manifest 게시 뒤 Metadata Commit 직전에 실패시킨다."""

    def _fail_commit(*_: object, **__: object) -> None:
        """Metadata Transaction 시작 전 예외를 발생시켜 Orphan 경로를 남긴다."""
        raise RuntimeError("Injected metadata commit failure")

    with monkeypatch.context() as scoped:
        scoped.setattr(metadata, "commit_table_run", _fail_commit)
        scoped.setattr(service, "commit_table_run", _fail_commit)
        yield


@contextmanager
def crash_metadata_commit(monkeypatch: MonkeyPatch) -> Iterator[None]:
    """Commit 직전 BaseException 중단으로 미처리 Crash를 재현한다."""

    def _crash(*_: object, **__: object) -> None:
        """예외 처리 경로를 우회하는 프로세스 중단을 발생시킨다."""
        raise InjectedCrash("Injected metadata commit crash")

    with monkeypatch.context() as scoped:
        scoped.setattr(service, "commit_table_run", _crash)
        yield


@contextmanager
def fail_source_connection(monkeypatch: MonkeyPatch) -> Iterator[None]:
    """Extractor가 Source Snapshot 연결을 열 때 psycopg 연결 오류를 발생시킨다."""

    def _fail_connection(*_: object, **__: object) -> object:
        """실제 Source 연결을 열기 전에 재시도 가능한 연결 오류를 낸다."""
        raise OperationalError("Injected source connection failure")

    with monkeypatch.context() as scoped:
        scoped.setattr(extract.PostgresSettings, "source_connection", _fail_connection)
        yield


def corrupt_manifest(
    storage: SeaweedFSSettings, key: str, *, kind: ManifestCorruptionKind
) -> None:
    """Manifest Key는 보존한 채 선택한 계약 필드를 변조해 다시 올린다."""
    if kind not in {"CHECKSUM", "ROW_RANGE", "SCHEMA_VERSION"}:
        raise ValueError(f"Unsupported manifest corruption kind: {kind}")
    payload = json.loads(read_object_bytes(storage, key))
    if not isinstance(payload, dict):
        raise TypeError("Manifest payload must be a JSON object")
    _corrupt_payload(payload, kind)
    mutated = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )
    seaweedfs_s3_client(storage).put_object(
        Bucket=storage.bucket,
        Key=key,
        Body=mutated,
        Metadata={"sha256": hashlib.sha256(mutated).hexdigest()},
    )


def _corrupt_payload(payload: dict[str, object], kind: ManifestCorruptionKind) -> None:
    """선택한 Manifest 계약만 깨뜨리고 나머지 식별 정보는 유지한다."""
    if kind == "CHECKSUM":
        checksum = payload.get("content_sha256")
        if not isinstance(checksum, str):
            raise TypeError("Manifest content_sha256 must be a string")
        payload["content_sha256"] = "0" * len(checksum)
        return
    if kind == "ROW_RANGE":
        upper_bound = payload.get("extract_upper_bound")
        if not isinstance(upper_bound, dict):
            raise TypeError("Manifest extract_upper_bound must be an object")
        payload["extract_upper_bound"] = {"timestamp": None, "keys": []}
        return
    schema_version = payload.get("schema_version")
    if not isinstance(schema_version, int):
        raise TypeError("Manifest schema_version must be an integer")
    payload["schema_version"] = schema_version + 1
