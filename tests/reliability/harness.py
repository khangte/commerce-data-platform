"""신뢰성 시나리오의 읽기 전용 상태 수집과 증적 저장을 제공한다."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from uuid import UUID

from src.common.database import PostgresSettings
from src.ingestion.orphan import find_orphan_candidates
from src.ingestion.storage import SeaweedFSSettings, list_object_keys
from src.warehouse.mart_hash import mart_logical_hashes, mart_row_counts, target_for
from src.warehouse.publish_metadata import PublishRecord, get_publish_run

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OBSERVABILITY_QUERY_PATH = PROJECT_ROOT / "sql" / "validation" / "observability_run_status.sql"


@dataclass(frozen=True)
class PlatformState:
    """한 신뢰성 시나리오 전후 비교에 필요한 플랫폼 상태 스냅샷이다."""

    run_rows: tuple[dict[str, object], ...]
    watermarks: dict[str, str]
    object_keys: tuple[str, ...]
    orphan_candidates: tuple[str, ...]
    publish_runs: tuple[dict[str, object], ...]
    mart_hashes: dict[str, str]
    mart_row_counts: dict[str, int]


def collect_state(
    postgres: PostgresSettings,
    storage: SeaweedFSSettings,
    *,
    warehouse_path: Path | None,
    marts: Sequence[str],
    publish_run_ids: Sequence[UUID] = (),
    object_prefix: str = "",
) -> PlatformState:
    """기존 조회 자산만 조합해 변경 없이 플랫폼 상태를 수집한다."""
    run_rows, watermarks = _collect_postgres_state(postgres)
    publish_runs = _collect_publish_runs(postgres, run_rows, publish_run_ids)
    targets = tuple(target_for(relation) for relation in marts)
    if warehouse_path is None or not warehouse_path.is_file():
        mart_hashes: dict[str, str] = {}
        row_counts: dict[str, int] = {}
    else:
        mart_hashes = mart_logical_hashes(warehouse_path, targets)
        row_counts = mart_row_counts(warehouse_path, targets)
    return PlatformState(
        run_rows=run_rows,
        watermarks=watermarks,
        object_keys=list_object_keys(storage, object_prefix),
        orphan_candidates=tuple(
            candidate.object_key for candidate in find_orphan_candidates(postgres, storage)
        ),
        publish_runs=publish_runs,
        mart_hashes=mart_hashes,
        mart_row_counts=row_counts,
    )


def assert_unchanged(before: PlatformState, after: PlatformState, fields: Sequence[str]) -> None:
    """지정한 상태 필드가 시나리오 전후에 동일한지 단언한다."""
    changed = diff_state(before, after)
    selected = {field: changed[field] for field in fields if field in changed}
    assert not selected, f"Unexpected state changes: {selected}"


def diff_state(before: PlatformState, after: PlatformState) -> dict[str, tuple[object, object]]:
    """두 스냅샷에서 달라진 필드와 전후 값을 반환한다."""
    return {
        field: (before_value, after_value)
        for field, before_value, after_value in zip(
            PlatformState.__dataclass_fields__,
            (getattr(before, field) for field in PlatformState.__dataclass_fields__),
            (getattr(after, field) for field in PlatformState.__dataclass_fields__),
            strict=True,
        )
        if before_value != after_value
    }


def write_evidence(scenario_id: str, payload: Mapping[str, object]) -> Path:
    """시나리오 증적을 UTC 시각 이름의 JSON 파일로 저장하고 경로를 반환한다."""
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    directory = PROJECT_ROOT / "data" / "reliability" / scenario_id
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{timestamp}.json"
    path.write_text(
        json.dumps(payload, default=_json_default, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _collect_postgres_state(
    postgres: PostgresSettings,
) -> tuple[tuple[dict[str, object], ...], dict[str, str]]:
    """관측 SQL 결과와 Watermark를 하나의 읽기 전용 연결에서 수집한다."""
    observability_sql = OBSERVABILITY_QUERY_PATH.read_text(encoding="utf-8")
    with postgres.pipeline_connection() as connection:
        cursor = connection.execute(observability_sql)
        columns = tuple(description.name for description in cursor.description)
        run_rows = tuple(dict(zip(columns, row, strict=True)) for row in cursor.fetchall())
        watermark_rows = connection.execute(
            """
            SELECT pipeline_name, source_table, watermark_timestamp, watermark_keys
            FROM watermarks
            ORDER BY pipeline_name, source_table
            """
        ).fetchall()
    watermarks = {
        f"{pipeline_name}:{source_table}": _watermark_value(timestamp, keys)
        for pipeline_name, source_table, timestamp, keys in watermark_rows
    }
    return run_rows, watermarks


def _collect_publish_runs(
    postgres: PostgresSettings,
    run_rows: Sequence[dict[str, object]],
    publish_run_ids: Sequence[UUID],
) -> tuple[dict[str, object], ...]:
    """관측 SQL과 명시 식별자가 가리키는 Publish Run을 기존 조회 함수로 읽는다."""
    records: list[dict[str, object]] = []
    seen = set(publish_run_ids)
    for row in run_rows:
        publish_run_id = row.get("publish_run_id")
        if isinstance(publish_run_id, UUID):
            seen.add(publish_run_id)
    for publish_run_id in sorted(seen, key=str):
        record = get_publish_run(postgres, publish_run_id)
        if record is not None:
            records.append(_publish_record_dict(record))
    return tuple(records)


def _publish_record_dict(record: PublishRecord) -> dict[str, object]:
    """Publish Record를 증적과 비교에 안전한 사전으로 변환한다."""
    return cast(dict[str, object], asdict(record))


def _watermark_value(timestamp: object, keys: object) -> str:
    """Watermark 시각과 동률 Key를 비교 가능한 결정적 문자열로 직렬화한다."""
    timestamp_value = timestamp.isoformat() if isinstance(timestamp, datetime) else ""
    serialized_keys = json.dumps(keys, ensure_ascii=False, sort_keys=True)
    return f"{timestamp_value}|{serialized_keys}"


def _json_default(value: object) -> object:
    """증적 JSON의 날짜·UUID·Dataclass 값을 안전한 기본 표현으로 바꾼다."""
    if isinstance(value, (datetime, UUID)):
        return str(value)
    if is_dataclass(value):
        return asdict(value)
    raise TypeError(f"Object is not JSON serializable: {type(value).__name__}")
