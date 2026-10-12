"""Generator 성공 재사용 조회가 배율을 식별자에 포함하는지 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from src.generator import service
from src.generator.config import GENERATOR_VERSION, GeneratorConfig


def test_successful_result_query_matches_progress_time_scale() -> None:
    """같은 logical_date라도 배율이 다르면 성공 실행을 재사용하지 않는다."""
    config = GeneratorConfig("seed:abc123", 42, datetime(2026, 9, 4, tzinfo=UTC), 1, "default", GENERATOR_VERSION, 144)
    settings = MagicMock()
    connection = settings.pipeline_connection.return_value.__enter__.return_value
    connection.execute.return_value.fetchone.return_value = None

    assert service._successful_result(settings, config) is None

    query, parameters = connection.execute.call_args.args
    assert "progress_time_scale = %s" in query
    assert parameters[-1] == 144
