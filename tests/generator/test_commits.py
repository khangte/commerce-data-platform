"""Source 실행 마커가 배율을 실행 식별자로 쓰는지 검증한다."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock

from src.generator.core.commits import committed_result, record_source_commit
from src.generator.core.config import GENERATOR_VERSION, GeneratorConfig


def _config() -> GeneratorConfig:
    """배율 144의 고정 Config를 반환한다."""
    return GeneratorConfig("seed:abc123", 42, datetime(2026, 9, 4, tzinfo=UTC), 1, "default", GENERATOR_VERSION, 144)


def test_committed_result_query_matches_progress_time_scale() -> None:
    """같은 입력이라도 배율이 다르면 이전 커밋을 재사용하지 않는다."""
    settings = MagicMock()
    connection = settings.source_connection.return_value.__enter__.return_value
    connection.execute.return_value.fetchone.return_value = None

    assert committed_result(settings, _config()) is None

    query, parameters = connection.execute.call_args.args
    assert "progress_time_scale = %s" in query
    assert parameters[-1] == 144


def test_record_source_commit_stores_progress_time_scale() -> None:
    """Source 커밋 마커에 배율을 남긴다."""
    connection = MagicMock()
    record_source_commit(connection, uuid.uuid4(), _config(), {"orders_inserted": 1}, "0" * 64)
    query, parameters = connection.execute.call_args.args
    assert "progress_time_scale" in query
    assert 144 in parameters
