"""Warehouse Publish CLI의 dbt 실행 인자 전달을 검증한다."""

from __future__ import annotations

from pathlib import Path

from src.warehouse import publish
from src.warehouse.dbt_runner import DbtRunResult


def test_main_passes_full_refresh_and_dbt_vars_to_build_runner(monkeypatch, tmp_path: Path) -> None:
    """전체 새로고침과 dbt 변수는 같은 build 실행에 순서대로 전달한다."""
    captured: dict[str, tuple[str, ...]] = {}

    def _runner(_: Path, __: Path, *, extra_args: tuple[str, ...] = ()) -> DbtRunResult:
        """CLI가 구성한 dbt 추가 인자를 기록하는 성공 Runner다."""
        captured["extra_args"] = extra_args
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
    assert captured["extra_args"] == (
        "--full-refresh",
        "--vars",
        "{bronze_as_of: '2026-09-23T00:00:00Z'}",
    )
