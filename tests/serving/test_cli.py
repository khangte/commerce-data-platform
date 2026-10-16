"""호스트 서빙 CLI의 내보내기·재연결·상태 종료 코드를 검증한다."""

import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError

import pytest

from src.serving import cli, metabase_settings
from src.serving.freshness import ServingFreshness
from src.warehouse.duckdb_files import file_identity
from src.warehouse.publish_metadata import PUBLISHED, PublishRecord
from tests.serving.test_export import _create_published_mart


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch, tmp_path):
    """실제 설정이 서빙 CLI 테스트에 새지 않게 환경을 격리한다."""
    for name in ("METABASE_URL", "METABASE_API_KEY", "METABASE_SERVING_DATABASE_ID"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(metabase_settings, "ENV_PATH", tmp_path / ".env")
    monkeypatch.setattr(cli.PostgresSettings, "from_environment", lambda: object())


@pytest.fixture
def export_setup(monkeypatch, tmp_path):
    """실제 게시 파일과 이전 내보내기 세 개를 준비한다."""
    published = tmp_path / "warehouse.duckdb"
    _create_published_mart(published)
    record = PublishRecord(
        uuid.uuid4(), PUBLISHED, datetime.now(UTC), None, {}, None, file_identity(published)
    )
    monkeypatch.setattr(cli, "get_latest_publish_run", lambda _: record)
    root = tmp_path / "serving"
    versions = root / "exports"
    versions.mkdir(parents=True)
    for index in range(3):
        path = versions / f"old-{index}.duckdb"
        path.write_bytes(b"old")
        os.utime(path, (index + 1, index + 1))
    return published, root, record


def test_export_command_repoints_and_prunes(monkeypatch, export_setup, capsys) -> None:
    """내보내기 뒤 호스트 주소로 재지정하고 새 파일 포함 세 개만 보존한다."""
    published, root, record = export_setup
    monkeypatch.setenv("METABASE_API_KEY", "cli-test-api-secret")
    monkeypatch.setenv("METABASE_SERVING_DATABASE_ID", "2")
    captured = []

    def repoint(*args):
        """재연결 경계만 대체해 전달한 설정과 순서를 기록한다."""
        captured.append(args)
        assert (root / "exports" / f"{args[0]}.duckdb").is_file()
        return "SUCCESS"

    monkeypatch.setattr("src.serving.metabase_repoint.repoint_metabase_serving", repoint)
    assert cli.main(["export", "--published", str(published), "--serving-root", str(root)]) == 0
    output = capsys.readouterr()
    result = json.loads(output.out)
    assert result["metabase_repoint"] == "SUCCESS"
    assert result["publish_run_id"] == str(record.publish_run_id)
    assert captured == [(result["export_id"], "http://localhost:3000", "cli-test-api-secret", "2")]
    versions = list((root / "exports").glob("*.duckdb"))
    assert len(versions) == 3
    assert root / "exports" / f"{result['export_id']}.duckdb" in versions
    assert "cli-test-api-secret" not in output.out + output.err


def test_export_command_keeps_versions_and_fails_when_repoint_fails(
    monkeypatch, export_setup, capsys,
) -> None:
    """재지정 실패는 종료 1을 반환하고 이전·신규 파일을 모두 보존한다."""
    published, root, _ = export_setup
    monkeypatch.setattr(
        "src.serving.metabase_repoint.repoint_metabase_serving", lambda *args: "FAILED"
    )
    assert cli.main(["export", "--published", str(published), "--serving-root", str(root)]) == 1
    assert json.loads(capsys.readouterr().out)["metabase_repoint"] == "FAILED"
    assert len(list((root / "exports").glob("*.duckdb"))) == 4


def test_export_command_warns_and_prunes_when_repoint_is_skipped(export_setup, capsys) -> None:
    """선택 설정이 없으면 경고 후 종료 0으로 끝내고 최근 세 개를 남긴다."""
    published, root, _ = export_setup
    assert cli.main(["export", "--published", str(published), "--serving-root", str(root)]) == 0
    output = capsys.readouterr()
    assert json.loads(output.out)["metabase_repoint"] == "SKIPPED"
    assert "Metabase 재지정 건너뜀" in output.err
    assert "python -m src.serving status" in output.err
    assert len(list((root / "exports").glob("*.duckdb"))) == 3


@pytest.mark.parametrize("latest,expected", [("p1", 0), ("p2", 1)])
def test_status_exit_code_follows_freshness(monkeypatch, capsys, latest, expected) -> None:
    """상태 명령은 최신 실행 일치 여부를 JSON과 종료 코드로 전달한다."""
    monkeypatch.setenv("METABASE_API_KEY", "cli-test-api-secret")
    monkeypatch.setenv("METABASE_SERVING_DATABASE_ID", "2")
    monkeypatch.setattr(cli, "check_serving_freshness", lambda *args: ServingFreshness(
        2, "e1", "p1", latest,
    ))
    assert cli.main(["status"]) == expected
    output = capsys.readouterr()
    assert json.loads(output.out) == {
        "database_id": 2, "live_export_id": "e1", "live_publish_run_id": "p1",
        "latest_publish_run_id": latest, "fresh": expected == 0,
    }
    assert "cli-test-api-secret" not in output.out + output.err


def test_status_fails_without_database_id(capsys) -> None:
    """필수 DB 설정이 없으면 상태 확인을 실패로 명시한다."""
    assert cli.main(["status"]) == 1
    assert "METABASE_SERVING_DATABASE_ID가 필요합니다." in capsys.readouterr().err


@pytest.mark.parametrize("error,expected", [
    (HTTPError("http://mb.test", 403, "hidden-detail", {}, None), "HTTP 403"),
    (URLError("hidden-detail"), "URLError"),
    (ValueError("hidden-detail"), "ValueError"),
    (KeyError("hidden-detail"), "KeyError"),
    (OSError("hidden-detail"), "OSError"),
])
def test_status_failure_hides_remote_details(monkeypatch, capsys, error, expected) -> None:
    """조회 실패는 종료 1로 끝내고 응답 상세나 인증값을 노출하지 않는다."""
    monkeypatch.setenv("METABASE_API_KEY", "cli-test-api-secret")
    monkeypatch.setenv("METABASE_SERVING_DATABASE_ID", "2")

    def fail(*args):
        """외부 조회 경계에서 지정 오류를 재현한다."""
        raise error

    monkeypatch.setattr(cli, "check_serving_freshness", fail)
    assert cli.main(["status"]) == 1
    output = capsys.readouterr()
    assert expected in output.err
    assert "hidden-detail" not in output.out + output.err
    assert "cli-test-api-secret" not in output.out + output.err


def test_cli_requires_a_subcommand() -> None:
    """하위 명령이 없으면 암묵적 내보내기 대신 사용 오류로 끝낸다."""
    with pytest.raises(SystemExit) as raised:
        cli.main([])
    assert raised.value.code == 2


def test_cli_rejects_a_legacy_publish_without_file_identity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """기존 게시 기록에 식별값이 없으면 새 게시를 요구하고 파일을 만들지 않는다."""
    from src.warehouse.publish_metadata import PUBLISHED, PublishRecord

    record = PublishRecord(uuid.uuid4(), PUBLISHED, datetime.now(UTC), None, {}, None, None)
    monkeypatch.setattr(cli.PostgresSettings, "from_environment", lambda: object())
    monkeypatch.setattr(cli, "get_latest_publish_run", lambda _: record)
    root = tmp_path / "serving"
    with pytest.raises(ValueError, match="no recorded file identity; run a new publish"):
        cli.main(["export", "--serving-root", str(root)])
    assert not root.exists()


