"""게시 생존 신호의 주기·오류·소유권 상실·종료를 검증한다."""

import threading
import time
import uuid
from datetime import timedelta

import psycopg
import pytest

from src.warehouse.publish_heartbeat import publish_heartbeat

INTERVAL = timedelta(seconds=0.05)


def test_heartbeat_touches_periodically_and_on_exit():
    """주기 갱신 뒤 정상 종료 시 호출 스레드에서 마지막으로 갱신한다."""
    calls = []
    def touch(*args, **kwargs):
        """갱신을 실행한 스레드를 기록한다."""
        calls.append(threading.current_thread())
        return True
    with publish_heartbeat(object(), uuid.uuid4(), interval=INTERVAL, touch=touch):
        time.sleep(0.2)
    assert len(calls) >= 3
    assert calls[-1] is threading.current_thread()
    assert all(t.daemon and not t.is_alive() for t in calls[:-1])


def test_heartbeat_survives_touch_errors(caplog):
    """일시 DB 오류 이후에도 갱신하며 예외 본문을 로그에 남기지 않는다."""
    calls = []
    resumed = threading.Event()
    def touch(*args, **kwargs):
        """첫 갱신은 실패하고 다음 갱신은 성공한다."""
        calls.append(1)
        if len(calls) == 1:
            raise psycopg.OperationalError("postgresql://user password=secret")
        resumed.set()
        return True
    with publish_heartbeat(object(), uuid.uuid4(), interval=INTERVAL, touch=touch):
        assert resumed.wait(2)
    assert len(calls) >= 3
    assert "OperationalError" in caplog.text
    assert "password=secret" not in caplog.text and "postgresql://" not in caplog.text


def test_heartbeat_stops_after_lost_lease(caplog):
    """갱신 0행이면 경고 후 중단하고 종료 때도 갱신하지 않는다."""
    calls = []
    lost = threading.Event()
    def touch(*args, **kwargs):
        """소유권을 잃은 첫 호출을 기록한다."""
        calls.append(threading.current_thread())
        lost.set()
        return False
    with publish_heartbeat(object(), uuid.uuid4(), interval=INTERVAL, touch=touch):
        assert lost.wait(2)
        time.sleep(0.15)
    assert len(calls) == 1
    assert not calls[0].is_alive()
    assert "소유권 상실" in caplog.text


def test_heartbeat_propagates_body_error_and_joins():
    """본문 예외를 보존하고 갱신 스레드를 종료한다."""
    threads = []
    touched = threading.Event()
    def touch(*args, **kwargs):
        """주기 갱신 스레드를 기록한다."""
        threads.append(threading.current_thread())
        touched.set()
        return True
    with pytest.raises(RuntimeError, match="body failure"), publish_heartbeat(
        object(), uuid.uuid4(), interval=INTERVAL, touch=touch,
    ):
        assert touched.wait(2)
        raise RuntimeError("body failure")
    assert len(threads) == 1 and not threads[0].is_alive()


def test_heartbeat_exit_does_not_wait_for_interval():
    """종료 이벤트는 긴 주기 대기도 즉시 깨운다."""
    started = time.monotonic()
    with publish_heartbeat(object(), uuid.uuid4(), interval=timedelta(seconds=30), touch=lambda *args, **kwargs: True):
        pass
    assert time.monotonic() - started < 1


def test_final_touch_error_is_sanitized(caplog):
    """마지막 갱신 실패도 본문 성공을 취소하거나 비밀값을 노출하지 않는다."""
    def touch(*args, **kwargs):
        """종료 갱신에서만 인증 문자열을 포함한 예외를 낸다."""
        raise OSError("password=secret")
    with publish_heartbeat(object(), uuid.uuid4(), interval=timedelta(seconds=30), touch=touch):
        pass
    assert "OSError" in caplog.text and "secret" not in caplog.text


def test_final_touch_false_does_not_warn(caplog):
    """본문이 행을 종료한 뒤 마지막 갱신 0행은 경고하지 않는다."""
    calls = []
    def touch(*args, **kwargs):
        """종료 상태인 행에 대한 마지막 갱신을 재현한다."""
        calls.append(threading.current_thread())
        return False
    with publish_heartbeat(object(), uuid.uuid4(), interval=timedelta(seconds=30), touch=touch):
        pass
    assert calls == [threading.current_thread()]
    assert not caplog.records
