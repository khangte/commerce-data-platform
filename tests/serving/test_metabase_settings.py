"""호스트 Metabase 설정의 우선순위와 컨테이너 주소 격리를 검증한다."""

import pytest

from src.serving import metabase_settings as settings


@pytest.fixture(autouse=True)
def isolated_settings(monkeypatch, tmp_path):
    """실제 키와 주소를 읽지 않도록 환경과 설정 파일을 격리한다."""
    for name in ("METABASE_API_KEY", "METABASE_URL", "METABASE_SERVING_DATABASE_ID"):
        monkeypatch.delenv(name, raising=False)
    path = tmp_path / ".env"
    monkeypatch.setattr(settings, "ENV_PATH", path)
    return path


def test_process_env_wins_over_dotenv(monkeypatch, isolated_settings) -> None:
    """비어 있지 않은 프로세스 설정을 파일보다 우선하고 공백을 제거한다."""
    isolated_settings.write_text("METABASE_API_KEY=file-key\n")
    monkeypatch.setenv("METABASE_API_KEY", " env-key ")
    assert settings.metabase_setting("METABASE_API_KEY") == "env-key"


def test_empty_process_env_falls_back_to_dotenv(monkeypatch, isolated_settings) -> None:
    """빈 환경 변수는 파일의 첫 번째 설정값으로 대체한다."""
    isolated_settings.write_text("METABASE_API_KEY= file-key \nMETABASE_API_KEY=ignored\n")
    monkeypatch.setenv("METABASE_API_KEY", " ")
    assert settings.metabase_setting("METABASE_API_KEY") == "file-key"


def test_empty_dotenv_value_is_missing(isolated_settings) -> None:
    """파일 값도 비어 있으면 필수 설정 오류로 처리한다."""
    isolated_settings.write_text("METABASE_API_KEY=\n")
    assert settings.metabase_setting("METABASE_API_KEY") == ""
    with pytest.raises(RuntimeError, match="METABASE_API_KEY가 필요합니다."):
        settings.require_metabase_setting("METABASE_API_KEY")


def test_missing_dotenv_file_returns_empty(isolated_settings) -> None:
    """설정 파일이 없으면 선택 설정은 빈 문자열이다."""
    assert settings.metabase_setting("METABASE_API_KEY") == ""


def test_host_url_ignores_dotenv_container_address(monkeypatch, isolated_settings) -> None:
    """호스트 주소는 파일의 컨테이너 주소를 읽지 않고 프로세스 설정만 따른다."""
    isolated_settings.write_text("METABASE_URL=http://metabase:3000\n")
    assert settings.host_metabase_url() == "http://localhost:3000"
    monkeypatch.setenv("METABASE_URL", " http://mb.test:3000/ ")
    assert settings.host_metabase_url() == "http://mb.test:3000"
