"""게시 단계 실행 중 소유자의 생존 신호를 주기적으로 갱신한다."""

from __future__ import annotations

import logging
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

from src.common.database import PostgresSettings
from src.warehouse.publish_metadata import touch_publish_run

logger = logging.getLogger(__name__)


@contextmanager
def publish_heartbeat(
    settings: PostgresSettings, publish_run_id: uuid.UUID, *, interval: timedelta,
    touch=touch_publish_run,
) -> Iterator[None]:
    """단계 동안 갱신하고 정상 종료 때 마지막 생존 신호를 남긴다."""
    seconds = interval.total_seconds()
    if seconds <= 0:
        raise ValueError("생존 신호 주기는 0보다 커야 합니다.")
    stop = threading.Event()
    lost = threading.Event()

    def pulse() -> bool | None:
        """갱신 결과를 반환하고 예외는 종류만 경고한다."""
        try:
            return touch(settings, publish_run_id, now=datetime.now(UTC))
        except Exception as error:  # noqa: BLE001 - 갱신 콜백 오류를 격리하고 비밀값 없이 재시도한다.
            logger.warning("Publish 생존 신호 갱신 실패: %s", type(error).__name__)
            return None

    def beat() -> None:
        """종료 요청 또는 소유권 상실까지 일정 주기로 갱신한다."""
        while not stop.wait(seconds):
            if pulse() is False:
                logger.warning("Publish 생존 신호 소유권 상실: %s", publish_run_id)
                lost.set()
                return

    thread = threading.Thread(target=beat, daemon=True, name=f"publish-heartbeat-{publish_run_id}")
    thread.start()
    succeeded = False
    try:
        yield
        succeeded = True
    finally:
        stop.set()
        thread.join(seconds)
        if succeeded and not lost.is_set():
            pulse()
