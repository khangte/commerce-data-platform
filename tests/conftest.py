"""상태 변경 테스트가 공유 Source·Metadata·Bucket을 사용하지 못하게 한다."""

from __future__ import annotations

import pytest

from src.common.database import environment_values


def pytest_sessionstart(session: pytest.Session) -> None:
    """수집·Fixture 실행 전에 격리 대상 세 개를 모두 확인한다."""
    values = environment_values()
    if not any(
        values.get(flag) == "1"
        for flag in ("RUN_POSTGRES_INTEGRATION", "RUN_SEAWEEDFS_INTEGRATION")
    ):
        return
    required_suffixes = {
        "COMMERCE_SOURCE_DB": "_it",
        "PIPELINE_METADATA_DB": "_it",
        "SEAWEEDFS_BUCKET": "-it",
    }
    unsafe = [
        name for name, suffix in required_suffixes.items()
        if not values.get(name, "").endswith(suffix)
    ]
    if unsafe:
        raise pytest.UsageError(
            "공유 환경에서 상태 변경 테스트를 실행할 수 없습니다. "
            "scripts/setup_test_isolation.sh로 준비한 _it DB와 -it Bucket을 지정하세요: "
            + ", ".join(unsafe)
        )
