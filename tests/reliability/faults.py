"""신뢰성 시나리오에서 기존 함수 경계에 장애를 주입한다."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import duckdb
from botocore.exceptions import ClientError
from psycopg import OperationalError
from pytest import MonkeyPatch

from src.benchmark.duckdb_s3 import configure_s3
from src.ingestion import service
from src.ingestion.bronze import storage as storage_module
from src.ingestion.bronze.storage import SeaweedFSSettings, read_object_bytes, seaweedfs_s3_client
from src.ingestion.control import metadata
from src.ingestion.extraction import extract

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


@dataclass(frozen=True)
class BackdatedTier:
    """Build Catalog에 주입한 소급 등급 관측과 그 관측으로 사라지는 기존 고객 Key다."""

    object_key: str
    customer_id: str
    membership_tier: str
    replaced_customer_key: str


@contextmanager
def backdated_tier_observation(
    storage: SeaweedFSSettings,
) -> Iterator[Callable[[Path], BackdatedTier]]:
    """가입 시점 등급 관측이 늦게 도착한 Bronze 조건을 Build 파일 Catalog에만 주입한다.

    구독 결제가 참조하는 첫 고객 Version을 고른다. 같은 등급에 생성·수정 시각을 하루 앞당긴 관측을
    넣으면 첫 Version의 valid_from이 바뀌어 기존 customer_key가 사라진다. 등급은 내려갈 수 없어
    (dim_customer_membership_tier_non_decreasing) 등급은 그대로 둔다. Cursor 수집은 Watermark 이후
    관측만 가져오므로 이 조건을 만들지 못한다. 그래서 Object를 별도 Prefix에 올린다.
    Postgres Metadata는 건드리지 않고, 올린 Object는 종료 시 지운다.
    """
    uploaded: list[str] = []

    def inject(build_path: Path) -> BackdatedTier:
        """build_path의 Catalog에 소급 관측 1건을 추가하고 대상 고객 정보를 반환한다."""
        with duckdb.connect(str(build_path)) as connection:
            configure_s3(connection, storage)
            urls = [
                f"s3://{storage.bucket}/{key}"
                for (key,) in connection.execute(
                    "SELECT object_key FROM control.bronze_files"
                    " WHERE source_table = 'customer_membership_tiers' ORDER BY committed_at"
                ).fetchall()
            ]
            row = connection.execute(
                """
                SELECT d.customer_id, d.membership_tier, d.customer_key
                FROM facts.fct_subscription_payment AS p
                JOIN dimensions.dim_customer AS d USING (customer_key)
                WHERE d.effective_from = '-infinity'
                ORDER BY d.customer_id
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                raise LookupError("No subscription payment on a first customer version")
            customer_id, membership_tier, customer_key = row
            (max_batch,) = connection.execute(
                "SELECT max(batch_id) FROM control.bronze_files"
            ).fetchone()
            batch_id = f"{max_batch}_fault_backdated_tier"
            object_key = (
                "reliability-faults/customer_membership_tiers/"
                f"ingestion_date={datetime.now(UTC).date()}/batch_id={batch_id}/data.parquet"
            )
            connection.execute(
                f"""
                COPY (
                    SELECT * REPLACE (
                        created_at - INTERVAL 1 DAY AS created_at,
                        created_at - INTERVAL 1 DAY AS updated_at,
                        ? AS _batch_id, now() AS _ingested_at
                    )
                    FROM read_parquet(?, union_by_name = true, hive_partitioning = false)
                    WHERE customer_unique_id = ?
                    ORDER BY updated_at
                    LIMIT 1
                ) TO 's3://{storage.bucket}/{object_key}' (FORMAT parquet)
                """,
                [batch_id, urls, customer_id],
            )
            uploaded.append(object_key)
            schema_version = connection.execute(
                "SELECT max(schema_version) FROM control.bronze_files"
                " WHERE source_table = 'customer_membership_tiers'"
            ).fetchone()[0]
            connection.execute(
                "INSERT INTO control.bronze_files VALUES (?, ?, ?, ?, now(), 1, 'fault-injected')",
                ["customer_membership_tiers", object_key, schema_version, batch_id],
            )
        return BackdatedTier(object_key, customer_id, membership_tier, customer_key)

    try:
        yield inject
    finally:
        client = seaweedfs_s3_client(storage)
        for key in uploaded:
            client.delete_object(Bucket=storage.bucket, Key=key)
