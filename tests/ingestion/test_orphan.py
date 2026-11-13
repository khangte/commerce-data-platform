"""Bronze Orphan 후보 탐지와 수동 처리 경계를 검증한다."""

from __future__ import annotations

from src.ingestion.bronze import orphan
from src.ingestion.bronze.orphan import OrphanCandidate, find_orphan_candidates


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


def _stub_cli_candidates(monkeypatch, candidates) -> None:
    """CLI의 환경 설정과 후보 조회를 외부 연결 없이 대체한다."""
    monkeypatch.setattr(orphan.PostgresSettings, "from_environment", lambda: object())
    monkeypatch.setattr(orphan.SeaweedFSSettings, "from_environment", lambda: object())
    monkeypatch.setattr(orphan, "find_orphan_candidates", lambda *_: candidates)


def test_cli_lists_candidates_without_reconciling(monkeypatch, capsys) -> None:
    """인자 없는 실행은 매니페스트 유무를 포함한 후보 JSON만 출력한다."""
    import json

    candidates = (OrphanCandidate("bronze/a/data.parquet", None),)
    _stub_cli_candidates(monkeypatch, candidates)
    calls = []
    monkeypatch.setattr(orphan, "reconcile_orphan", lambda *args: calls.append(args))

    assert orphan.main([]) == 0
    assert json.loads(capsys.readouterr().out) == [
        {"object_key": "bronze/a/data.parquet", "manifest_key": None}
    ]
    assert calls == []


def test_cli_rejects_unknown_key_without_reconciling(monkeypatch, capsys) -> None:
    """목록에 없는 키는 재조정하지 않고 0이 아닌 종료 코드를 반환한다."""
    _stub_cli_candidates(monkeypatch, ())
    calls = []
    monkeypatch.setattr(orphan, "reconcile_orphan", lambda *args: calls.append(args))

    assert orphan.main(["--reconcile", "bronze/missing/data.parquet"]) == 2
    assert "후보를 찾을 수 없습니다" in capsys.readouterr().err
    assert calls == []


def test_cli_returns_nonzero_for_refused_candidate(monkeypatch, capsys) -> None:
    """보수적 재조정이 거부되면 메시지를 stderr에 출력하고 실패한다."""
    candidate = OrphanCandidate("bronze/a/data.parquet", None)
    _stub_cli_candidates(monkeypatch, (candidate,))

    def refuse(*_):
        raise orphan.OrphanReconciliationError("requires manual handling")

    monkeypatch.setattr(orphan, "reconcile_orphan", refuse)
    assert orphan.main(["--reconcile", candidate.object_key]) == 1
    assert "requires manual handling" in capsys.readouterr().err


def test_cli_reconciles_only_selected_candidate(monkeypatch, capsys) -> None:
    """지정한 후보 한 건만 재조정하고 커밋 결과를 출력한다."""
    import json

    selected = OrphanCandidate("bronze/a/data.parquet", "bronze/a/manifest.json")
    other = OrphanCandidate("bronze/b/data.parquet", None)
    _stub_cli_candidates(monkeypatch, (selected, other))
    calls = []
    monkeypatch.setattr(orphan, "reconcile_orphan", lambda *args: calls.append(args[-1]))

    assert orphan.main(["--reconcile", selected.object_key]) == 0
    assert calls == [selected]
    assert json.loads(capsys.readouterr().out) == {
        "object_key": selected.object_key,
        "status": "COMMITTED",
    }
