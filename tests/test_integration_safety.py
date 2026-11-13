"""격리 검사가 테스트 수집보다 먼저 공유 환경을 차단하는지 검증한다."""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

from src.common.database import PROJECT_ROOT


@pytest.mark.parametrize("flag", ["RUN_POSTGRES_INTEGRATION", "RUN_SEAWEEDFS_INTEGRATION"])
@pytest.mark.parametrize("unsafe_name", ["COMMERCE_SOURCE_DB", "PIPELINE_METADATA_DB", "SEAWEEDFS_BUCKET"])
def test_stateful_run_rejects_shared_resource_before_collection(flag: str, unsafe_name: str) -> None:
    environment = {
        **os.environ,
        "RUN_POSTGRES_INTEGRATION": "0",
        "RUN_SEAWEEDFS_INTEGRATION": "0",
        "COMMERCE_SOURCE_DB": "commerce_source_it",
        "PIPELINE_METADATA_DB": "pipeline_metadata_it",
        "SEAWEEDFS_BUCKET": "commerce-lake-it",
        flag: "1",
        unsafe_name: "shared_resource",
    }
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "tests/not_a_real_test.py"],
        cwd=PROJECT_ROOT,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == pytest.ExitCode.USAGE_ERROR
    assert "공유 환경에서 상태 변경 테스트를 실행할 수 없습니다" in result.stderr
    assert unsafe_name in result.stderr
    assert "file or directory not found" not in result.stderr


@pytest.mark.parametrize(
    "overrides",
    [
        {"TEST_SOURCE_DB": "commerce_source"},
        {"TEST_METADATA_DB": "pipeline_metadata"},
        {"TEST_BUCKET": "commerce-lake"},
        {"TEST_SOURCE_DB": "commerce_source_it", "COMMERCE_SOURCE_DB": "commerce_source_it"},
        {"TEST_BUCKET": "commerce-lake-it", "SEAWEEDFS_BUCKET": "commerce-lake-it"},
    ],
)
def test_setup_rejects_unsafe_targets_without_connecting(overrides: dict[str, str]) -> None:
    """잘못된 이름이나 원본과 같은 대상은 접속보다 먼저 거부한다."""
    environment = {
        **os.environ,
        "COMMERCE_SOURCE_DB": "commerce_source",
        "PIPELINE_METADATA_DB": "pipeline_metadata",
        "SEAWEEDFS_BUCKET": "commerce-lake",
        "TEST_SOURCE_DB": "commerce_source_it",
        "TEST_METADATA_DB": "pipeline_metadata_it",
        "TEST_BUCKET": "commerce-lake-it",
        # 원본 서비스 대신 접속 불가능한 포트를 지정해도 이름 검사부터 실패해야 한다.
        "POSTGRES_PORT": "0",
        **overrides,
    }
    result = subprocess.run(
        ["bash", "scripts/setup_test_isolation.sh"],
        cwd=PROJECT_ROOT,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode != 0
    assert "접속" not in result.stderr
    assert any(message in result.stderr for message in ("끝나야", "같습니다"))
    assert "Traceback" not in result.stderr
