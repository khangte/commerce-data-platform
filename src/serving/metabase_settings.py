"""호스트용 Metabase 주소와 필수·선택 설정을 공통으로 읽는다."""

import os
from pathlib import Path

from src.common.database import PROJECT_ROOT

ENV_PATH = PROJECT_ROOT / ".env"
DEFAULT_HOST_METABASE_URL = "http://localhost:3000"


def metabase_setting(name: str, env_path: Path | None = None) -> str:
    """비어 있지 않은 환경 변수를 우선하고 없으면 루트 설정 파일에서 읽는다."""
    value = os.environ.get(name, "").strip()
    if value:
        return value
    path = env_path or ENV_PATH
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.startswith(f"{name}="):
                return line.partition("=")[2].strip()
    return ""


def require_metabase_setting(name: str, env_path: Path | None = None) -> str:
    """필수 설정이 비어 있으면 값 노출 없이 오류를 낸다."""
    value = metabase_setting(name, env_path)
    if not value:
        raise RuntimeError(f"{name}가 필요합니다.")
    return value


def host_metabase_url() -> str:
    """호스트 주소는 프로세스 환경 변수만 읽고 기본 localhost로 대체한다."""
    return os.environ.get("METABASE_URL", "").strip().rstrip("/") or DEFAULT_HOST_METABASE_URL
