"""원천 DAG 주기 설정과 지원하는 고정 간격을 정의한다."""

import os
import re
from datetime import timedelta

SOURCE_DAG_SCHEDULE_DEFAULT = "@hourly"


def source_dag_schedule() -> str:
    """환경 변수 또는 기본값으로 원천 DAG 주기를 반환한다."""
    return os.environ.get("SOURCE_DAG_SCHEDULE") or SOURCE_DAG_SCHEDULE_DEFAULT


def schedule_interval(expression: str) -> timedelta:
    """지원하는 별칭과 일정한 분 단위 cron을 고정 간격으로 바꾼다."""
    value = expression.strip()
    if value == "@hourly":
        return timedelta(hours=1)
    if value == "@daily":
        return timedelta(days=1)
    match = re.fullmatch(r"(\*/)?([0-9]+) \* \* \* \*", value)
    if match:
        minute = int(match[2])
        if match[1] and 1 <= minute <= 59 and 60 % minute == 0:
            return timedelta(minutes=minute)
        if not match[1] and 0 <= minute <= 59:
            return timedelta(hours=1)
    raise ValueError(f"unsupported SOURCE_DAG_SCHEDULE: {expression!r}")
