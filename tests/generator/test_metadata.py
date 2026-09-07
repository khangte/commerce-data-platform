"""Generator 실행 Metadata 기록 계약을 검증한다."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from src.generator.config import GENERATOR_VERSION, GeneratorConfig
from src.generator.metadata import record_finished_run, record_started_run


def _config() -> GeneratorConfig:
    """Metadata 기록 테스트에 사용할 유효한 실행 Config를 반환한다."""
    return GeneratorConfig(
        source_snapshot_id="seed:abc123",
        random_seed=42,
        logical_date=datetime(2026, 9, 4, tzinfo=UTC),
        order_count=1,
        anomaly_profile="default",
        generator_version=GENERATOR_VERSION,
    )


def test_record_started_run_stores_all_deterministic_inputs() -> None:
    """RUNNING 기록에 Snapshot과 모든 결정성 입력을 남긴다."""
    connection = MagicMock()
    generator_run_id = uuid.uuid4()

    record_started_run(connection, generator_run_id, _config())

    _, parameters = connection.execute.call_args.args
    assert parameters[:7] == (
        generator_run_id,
        "seed:abc123",
        42,
        datetime(2026, 9, 4, tzinfo=UTC),
        1,
        "default",
        GENERATOR_VERSION,
    )
    connection.commit.assert_called_once()


def test_successful_run_requires_reproducibility_evidence() -> None:
    """성공 상태에는 Entity Count와 Logical Hash가 반드시 필요하다."""
    with pytest.raises(ValueError, match="result_counts"):
        record_finished_run(MagicMock(), uuid.uuid4(), status="SUCCESS")
