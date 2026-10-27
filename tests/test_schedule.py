"""원천 DAG 주기 기본값과 고정 간격 해석을 검증한다."""
from datetime import timedelta

import pytest

from src.common.schedule import schedule_interval, source_dag_schedule


@pytest.mark.parametrize(("expression", "expected"), [
    ("@hourly", timedelta(hours=1)), ("@daily", timedelta(days=1)),
    ("*/10 * * * *", timedelta(minutes=10)), ("*/1 * * * *", timedelta(minutes=1)),
    ("15 * * * *", timedelta(hours=1)), ("0 * * * *", timedelta(hours=1)),
    ("59 * * * *", timedelta(hours=1)), ("  @hourly  ", timedelta(hours=1)),
])
def test_schedule_interval(expression, expected):
    """지원하는 원천 DAG 주기를 고정 간격으로 바꾼다."""
    assert schedule_interval(expression) == expected


@pytest.mark.parametrize("expression", [
    "0 9 * * 1-5", "*/7 * * * *", "*/0 * * * *", "60 * * * *",
    "@weekly", "", "* * * *", "*/60 * * * *", "-1 * * * *", "x * * * *",
])
def test_unsupported_schedule_raises(expression):
    """간격이 일정하지 않거나 해석할 수 없는 주기는 오류로 드러낸다."""
    with pytest.raises(ValueError, match="unsupported SOURCE_DAG_SCHEDULE"):
        schedule_interval(expression)


@pytest.mark.parametrize(("configured", "expected"), [
    (None, "@hourly"), ("", "@hourly"), ("*/10 * * * *", "*/10 * * * *"),
])
def test_source_dag_schedule_from_environment(monkeypatch, configured, expected):
    """환경 변수가 없거나 비면 매시간 기본값을 쓴다."""
    if configured is None:
        monkeypatch.delenv("SOURCE_DAG_SCHEDULE", raising=False)
    else:
        monkeypatch.setenv("SOURCE_DAG_SCHEDULE", configured)
    assert source_dag_schedule() == expected
