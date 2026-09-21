"""Benchmark 환경 Metadata 수집이 예외 없이 동작하는지 검증한다."""

from __future__ import annotations

import hashlib
import subprocess

import pytest

from src.benchmark.metadata import UV_LOCK_PATH, collect_environment


def test_collect_environment_never_raises_when_subprocess_fails(monkeypatch) -> None:
    """모든 외부 명령이 실패해도 예외 없이 실패 사실을 필드에 남긴다."""

    def _boom(*args, **kwargs):
        raise FileNotFoundError("no such command")

    monkeypatch.setattr(subprocess, "run", _boom)

    metadata = collect_environment()

    assert metadata.git_commit is None
    assert metadata.docker_image_versions["_source"] == "compose_file"


def test_dependency_lock_hash_matches_uv_lock_sha256() -> None:
    """`dependency_lock_hash`는 `uv.lock` Byte의 SHA-256과 같다."""
    metadata = collect_environment()

    expected = hashlib.sha256(UV_LOCK_PATH.read_bytes()).hexdigest()
    assert metadata.dependency_lock_hash == expected


def test_docker_image_versions_parses_compose_file_services() -> None:
    """`compose.yaml`에 선언된 Service의 Image 값을 읽는다."""
    metadata = collect_environment()

    assert metadata.docker_image_versions.get("postgres") == "postgres:18.6"
    assert metadata.docker_image_versions.get("seaweedfs") == "chrislusf/seaweedfs:4.45"


def test_host_wsl_spec_has_every_declared_field() -> None:
    """읽지 못한 값은 None이더라도 필드 자체는 항상 존재한다."""
    metadata = collect_environment()

    assert set(metadata.host_wsl_spec) == {
        "cpu_model", "cpu_count", "mem_total", "kernel_release",
    }


@pytest.mark.parametrize("field", ["cpu_model", "cpu_count", "mem_total"])
def test_host_wsl_spec_is_null_when_proc_is_unreadable(monkeypatch, field: str) -> None:
    """`/proc` 파일을 읽지 못하면 해당 필드만 None이 되고 예외는 없다."""
    from pathlib import Path

    original_read_text = Path.read_text

    def _fail_proc_read(self, *args, **kwargs):
        if str(self).startswith("/proc/"):
            raise OSError("no such file")
        return original_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", _fail_proc_read)

    metadata = collect_environment()

    assert metadata.host_wsl_spec[field] is None
