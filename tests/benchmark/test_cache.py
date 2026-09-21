"""Cold/Warm Cache 초기화 절차의 대체 경로를 검증한다."""

from __future__ import annotations

from pathlib import Path

from src.benchmark import cache as cache_module
from src.benchmark.cache import reset_caches, warm_up


def test_unwritable_drop_caches_falls_back_to_process_restart_only(monkeypatch) -> None:
    """`drop_caches`를 쓸 수 없으면 실패 사실을 숨기지 않고 대체 방식으로 내려간다."""

    def _deny_write(self, *args, **kwargs):
        raise PermissionError("Operation not permitted")

    monkeypatch.setattr(Path, "write_text", _deny_write)
    monkeypatch.setattr(cache_module.subprocess, "run", lambda *a, **k: None)

    result = reset_caches()

    assert result.method == "process_restart_only"
    assert result.detail


def test_successful_drop_caches_reports_that_method(monkeypatch) -> None:
    """`sync`와 `drop_caches` 기록이 모두 성공하면 `drop_caches`로 보고한다."""
    monkeypatch.setattr(cache_module.subprocess, "run", lambda *a, **k: None)
    monkeypatch.setattr(Path, "write_text", lambda self, value: None)

    result = reset_caches()

    assert result.method == "drop_caches"


def test_fallback_with_services_notes_restart_in_detail(monkeypatch) -> None:
    """대체 경로에서 Service를 지정하면 재시작 사실이 상세 사유에 남는다."""

    def _deny_write(self, *args, **kwargs):
        raise PermissionError("Operation not permitted")

    monkeypatch.setattr(Path, "write_text", _deny_write)
    monkeypatch.setattr(cache_module.subprocess, "run", lambda *a, **k: None)

    result = reset_caches(services=("postgres",))

    assert result.method == "process_restart_only"
    assert "postgres" in result.detail


def test_warm_up_runs_the_callable_once() -> None:
    """Warm-up은 결과를 버리되 실제로 한 번 호출한다."""
    calls = []
    warm_up(lambda: calls.append(1))
    assert calls == [1]
