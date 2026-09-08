"""Bronze Orphan 후보 탐지와 수동 처리 경계를 검증한다."""

from __future__ import annotations

from src.ingestion import orphan
from src.ingestion.orphan import OrphanCandidate, find_orphan_candidates


class _Connection:
    """Orphan 후보 조회에 필요한 COMMITTED Object 행만 반환한다."""

    def execute(self, *_: object) -> _Connection:
        """단순화한 Query 실행 결과를 현재 객체로 반환한다."""
        return self

    def fetchall(self) -> list[tuple[str]]:
        """Metadata에 이미 Commit된 Bronze Object Key를 반환한다."""
        return [("bronze/orders/committed/data.parquet",)]


class _ConnectionContext:
    """Pipeline Connection Context Manager를 대체한다."""

    def __enter__(self) -> _Connection:
        """고정된 Connection 대역을 연다."""
        return _Connection()

    def __exit__(self, *_: object) -> None:
        """테스트용 Connection 종료는 별도 동작이 없다."""


class _Settings:
    """Orphan 후보 탐지에 필요한 Pipeline Connection만 제공한다."""

    def pipeline_connection(self) -> _ConnectionContext:
        """COMMITTED Object 조회용 Connection Context를 반환한다."""
        return _ConnectionContext()


def test_find_orphan_candidates_includes_a_data_object_without_manifest(monkeypatch) -> None:
    """Manifest가 유실된 Final Parquet도 자동 복구가 아닌 수동 처리 후보로 보존한다."""
    monkeypatch.setattr(
        orphan,
        "list_object_keys",
        lambda *_: (
            "bronze/orders/committed/data.parquet",
            "bronze/orders/without-manifest/data.parquet",
            "bronze/orders/verified/data.parquet",
            "bronze/orders/verified/manifest.json",
        ),
    )

    candidates = find_orphan_candidates(_Settings(), object())

    assert candidates == (
        OrphanCandidate(
            "bronze/orders/verified/data.parquet", "bronze/orders/verified/manifest.json"
        ),
        OrphanCandidate("bronze/orders/without-manifest/data.parquet", None),
    )
