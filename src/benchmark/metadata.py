"""Benchmark 실행 환경(Git·Python·의존성·Docker·Host)의 Metadata를 수집한다."""

from __future__ import annotations

import hashlib
import platform
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
UV_LOCK_PATH = PROJECT_ROOT / "uv.lock"
COMPOSE_PATH = PROJECT_ROOT / "compose.yaml"

_TOP_LEVEL_KEY_PATTERN = re.compile(r"^([\w.-]+):\s*(?:&\S+)?\s*$")
_SERVICE_KEY_PATTERN = re.compile(r"^  ([\w.-]+):\s*(?:&\S+)?\s*$")
_IMAGE_LINE_PATTERN = re.compile(r"^\s+image:\s*(\S+)\s*$")


@dataclass(frozen=True)
class EnvironmentMetadata:
    """한 Benchmark 실행이 수행된 환경을 재현 가능하게 기록한다."""

    git_commit: str | None
    python_version: str
    dependency_lock_hash: str | None
    docker_image_versions: dict[str, str]
    host_wsl_spec: dict[str, str | None]


def collect_environment() -> EnvironmentMetadata:
    """실행 환경 전체를 수집한다. 개별 항목이 실패해도 예외를 올리지 않는다."""
    return EnvironmentMetadata(
        git_commit=_collect_git_commit(),
        python_version=platform.python_version(),
        dependency_lock_hash=_collect_dependency_lock_hash(),
        docker_image_versions=_collect_docker_image_versions(),
        host_wsl_spec=_collect_host_wsl_spec(),
    )


def _run(*args: str) -> str | None:
    """외부 명령을 실행하고 표준 출력을 반환한다. 실패하면 예외 없이 None을 반환한다."""
    try:
        completed = subprocess.run(
            args, cwd=PROJECT_ROOT, capture_output=True, text=True, check=True, timeout=30
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return completed.stdout.strip()


def _collect_git_commit() -> str | None:
    """현재 Commit Hash를 구하고, 미커밋 변경이 있으면 `-dirty`를 붙인다."""
    commit = _run("git", "rev-parse", "HEAD")
    if commit is None:
        return None
    status = _run("git", "status", "--porcelain")
    if status:
        return f"{commit}-dirty"
    return commit


def _collect_dependency_lock_hash() -> str | None:
    """`uv.lock`의 SHA-256을 계산한다. 파일이 없으면 None을 반환한다."""
    try:
        payload = UV_LOCK_PATH.read_bytes()
    except OSError:
        return None
    return hashlib.sha256(payload).hexdigest()


def _collect_docker_image_versions() -> dict[str, str]:
    """`docker compose images`의 해석된 값을 우선하고, 실패하면 `compose.yaml` 값으로 대체한다."""
    file_images = _parse_compose_images()
    resolved = _run("docker", "compose", "images", "--format", "json")
    if resolved:
        return {"_source": "docker_compose_images", "_raw": resolved, **file_images}
    versions = dict(file_images)
    versions["_source"] = "compose_file"
    return versions


def _parse_compose_images() -> dict[str, str]:
    """`compose.yaml`을 정규식으로 훑어 Service/Anchor 이름별 Image 값을 모은다.

    `services:` 아래에서는 개별 Service 이름을 Key로 쓰고, 그 밖의 최상위 블록
    (`x-airflow-common:` 같은 Anchor)에서는 최상위 블록 이름 자체를 Key로 쓴다.
    """
    try:
        lines = COMPOSE_PATH.read_text(encoding="utf-8").splitlines()
    except OSError:
        return {}
    images: dict[str, str] = {}
    top_level_key: str | None = None
    service_key: str | None = None
    for line in lines:
        top_level_match = _TOP_LEVEL_KEY_PATTERN.match(line)
        if top_level_match:
            top_level_key = top_level_match.group(1)
            service_key = None
            continue
        if top_level_key == "services":
            service_match = _SERVICE_KEY_PATTERN.match(line)
            if service_match:
                service_key = service_match.group(1)
                continue
        image_match = _IMAGE_LINE_PATTERN.match(line)
        if image_match:
            key = service_key if top_level_key == "services" else top_level_key
            if key is not None:
                images[key] = image_match.group(1)
    return images


def _collect_host_wsl_spec() -> dict[str, str | None]:
    """CPU 모델·Core 수·전체 메모리·Kernel 버전을 읽는다. 읽지 못하면 해당 필드만 None."""
    return {
        "cpu_model": _read_cpuinfo_field("model name"),
        "cpu_count": _count_cpuinfo_processors(),
        "mem_total": _read_meminfo_field("MemTotal"),
        "kernel_release": platform.release() or None,
    }


def _read_cpuinfo_field(field_name: str) -> str | None:
    """`/proc/cpuinfo`에서 주어진 필드의 첫 값을 읽는다."""
    try:
        text = Path("/proc/cpuinfo").read_text(encoding="utf-8")
    except OSError:
        return None
    for line in text.splitlines():
        if line.startswith(field_name):
            _, _, value = line.partition(":")
            return value.strip()
    return None


def _count_cpuinfo_processors() -> str | None:
    """`/proc/cpuinfo`의 `processor` 항목 수를 Core 수로 센다."""
    try:
        text = Path("/proc/cpuinfo").read_text(encoding="utf-8")
    except OSError:
        return None
    count = sum(1 for line in text.splitlines() if line.startswith("processor"))
    return str(count) if count else None


def _read_meminfo_field(field_name: str) -> str | None:
    """`/proc/meminfo`에서 주어진 필드 값을 읽는다."""
    try:
        text = Path("/proc/meminfo").read_text(encoding="utf-8")
    except OSError:
        return None
    for line in text.splitlines():
        if line.startswith(field_name):
            _, _, value = line.partition(":")
            return value.strip()
    return None

