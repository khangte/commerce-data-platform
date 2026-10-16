"""Warehouse Publish CLI의 dbt 실행 인자 전달을 검증한다."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.warehouse import publish
from src.warehouse.dbt_runner import DbtRunResult
from src.warehouse.duckdb_files import FileIdentity


def test_main_passes_full_refresh_and_dbt_vars_to_build_runner(monkeypatch, tmp_path: Path, capsys) -> None:
    """전체 새로고침과 dbt 변수는 같은 build 실행에 순서대로 전달한다."""
    captured: dict[str, object] = {}

    def _runner(
        _: Path, __: Path, *, extra_args: tuple[str, ...] = (), dbt_vars: dict | None = None
    ) -> DbtRunResult:
        """CLI가 구성한 dbt 추가 인자를 기록하는 성공 Runner다."""
        captured["extra_args"] = extra_args
        captured["dbt_vars"] = dbt_vars
        return DbtRunResult(0, "invocation", 0, 0, None, (), "")

    def _publish(*_: object, **kwargs: object) -> publish.PublishOutcome:
        """실제 DB 접근 없이 CLI가 만든 Runner를 한 번 실행한다."""
        runner = kwargs["dbt_runner"]
        assert callable(runner)
        runner(tmp_path / "build.duckdb", tmp_path / "target")
        return publish.PublishOutcome(
            publish_run_id=publish.uuid.uuid4(),
            previous_publish_run_id=None,
            mart_hashes={},
            mart_row_counts={},
            changed_relations=(),
            published_identity=FileIdentity(123, 456),
        )

    monkeypatch.setattr(publish.PostgresSettings, "from_environment", staticmethod(lambda: object()))
    monkeypatch.setattr(publish, "run_dbt_build", _runner)
    monkeypatch.setattr(publish, "publish_warehouse", _publish)

    assert (
        publish.main(
            [
                "--warehouse-root",
                str(tmp_path),
                "--full-refresh",
                "--dbt-vars",
                "{bronze_as_of: '2026-09-23T00:00:00Z'}",
            ]
        )
        == 0
    )
    assert "python -m src.serving export" in capsys.readouterr().err
    assert captured["extra_args"] == ("--full-refresh",)
    assert captured["dbt_vars"] == {"bronze_as_of": "2026-09-23T00:00:00Z"}


@pytest.mark.parametrize("value", ["[]", "null", "key: [", "{bronze_schemas: {}}"])
def test_invalid_dbt_vars_are_rejected_before_publish(monkeypatch, value: str) -> None:
    """잘못된 YAML·비매핑·예약 변수는 DB 연결과 게시 시작 전에 거절한다."""
    def _unexpected_settings() -> None:
        """잘못된 입력에서 DB 설정을 읽으면 검증 순서 오류로 처리한다."""
        pytest.fail("Invalid vars must fail before database access")

    monkeypatch.setattr(publish.PostgresSettings, "from_environment", _unexpected_settings)
    with pytest.raises(SystemExit) as raised:
        publish.main(["--dbt-vars", value])
    assert raised.value.code == 2


def test_failed_publish_does_not_print_serving_refresh_hint(monkeypatch, capsys) -> None:
    """게시가 실패했을 때 서빙 내보내기 안내를 성공처럼 출력하지 않는다."""
    monkeypatch.setattr(publish.PostgresSettings, "from_environment", lambda: object())

    def fail(*args, **kwargs):
        """외부 게시 경계에서 검증 실패를 재현한다."""
        raise publish.WarehouseBuildError("DBT_TEST_ERROR", "test failure")

    monkeypatch.setattr(publish, "publish_warehouse", fail)
    assert publish.main([]) == 1
    assert "python -m src.serving export" not in capsys.readouterr().err


def test_recover_only_does_not_print_serving_refresh_hint(monkeypatch, capsys) -> None:
    """복구 전용 실행은 게시 성공 뒤에만 필요한 안내를 출력하지 않는다."""
    monkeypatch.setattr(publish.PostgresSettings, "from_environment", lambda: object())
    monkeypatch.setattr(publish, "ensure_publish_metadata", lambda _: None)
    monkeypatch.setattr(publish, "recover_incomplete_publishes", lambda *args, **kwargs: ())
    assert publish.main(["--recover-only"]) == 0
    assert "python -m src.serving export" not in capsys.readouterr().err
