"""실험 A — Full Extract와 Incremental Extract가 같은 T1 상태에 도달하는 비용을 비교한다.

Generator 호출은 `prepare_extract_fixture`가 반복 시작 전 딱 한 번만 수행해 원천을
T1에서 동결한다. 반복마다(`run_extract_experiment`) 하는 일은 Watermark를 목표 지점에
있게 만들어(`_ensure_watermark_at`) 두 Arm을 가르고(Full은 전량, Incremental은 T1
경계 이후만) Ingestion 자체만 측정하는 것뿐이다. "되감기 실행"이 아니라 "목표 지점
보장"이 불변식이다 — 회차 1은 `prepare_extract_fixture`의 T0 선적재가 이미 그
지점에 세워 둬 되감기 자체가 무효한 요청이 되기 때문이다. Full Arm은
`src/ingestion/reprocess.py`의 기존 되감기만 재사용하고 새 Full-refresh 경로를
만들지 않는다.
"""

from __future__ import annotations

import dataclasses
import hashlib
import tempfile
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

from src.benchmark.config import BenchmarkScenario, RunConfig
from src.benchmark.duckdb_s3 import configure_s3
from src.benchmark.experiments import EXPERIMENTS, SCENARIOS
from src.benchmark.measure import RowCounts, measure
from src.benchmark.runner import ArmResult
from src.common.database import PostgresSettings
from src.common.row_hash import hash_cursor_rows
from src.generator.config import GENERATOR_VERSION, GeneratorConfig
from src.generator.service import resolve_source_snapshot_id, run_generator
from src.ingestion.extract import cursor_before_timestamp
from src.ingestion.metadata import CursorPosition, get_or_create_watermark
from src.ingestion.reprocess import RewindOutcome, rewind_tables
from src.ingestion.service import TableIngestionRequest, TableIngestionResult, ingest_table
from src.ingestion.storage import SeaweedFSSettings, stored_object_from_head
from src.ingestion.tables import TABLE_CONFIGS, table_config

EXTRACT_SCENARIO = BenchmarkScenario(
    scenario="extract",
    experiment="A",
    arms=("full", "incremental"),
    cold=False,
    description="Full Extract와 Incremental Extract가 같은 T1 상태에 도달하는 시간을 비교한다",
)
SCENARIOS[EXTRACT_SCENARIO.scenario] = EXTRACT_SCENARIO

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def prepare_extract_fixture(config: RunConfig) -> RunConfig:
    """반복 시작 전 딱 한 번 원천을 T1까지 만들고, 실측 `change_rate`·경계 시각을 심는다.

    Incremental·Full 두 Arm 모두 T0 상태를 한 번 적재해 Watermark를 전진시켜 둔다 —
    빈 Watermark는 되감을 수 없기 때문이다(`_cursor_is_earlier`는 현재 Cursor가
    `None`이면 항상 거부한다). Incremental Arm의 T0 Ingest 결과는 버리지 않고
    반환값에 실어, 반복마다 그 회차의 Delta와 합쳐 T1 전체 Catalog Hash를 만드는 데
    다시 쓴다. Full Arm은 매 반복 전량을 되감아 재적재하므로 T0 결과가 따로 필요
    없어 버린다. Delta Row 수는 추정이 아니라 실제 Postgres 조회로 잰다. 이후 5회
    반복 동안 `run_generator()`는 다시 부르지 않는다 — 원천은 T1에서 동결.
    """
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    change_rate_target = config.parameters["change_rate"]
    order_count_t0 = config.scale.order_count
    # T1 Generator 호출의 order_count는 총량이 아니라 이번 호출이 새로 넣을 건수다.
    # 결정적 ID가 (order_count, logical_date)를 Hash 입력에 포함하므로, 총량을 넘기면
    # T0와 겹치는 Ordinal 구간도 전부 다른 ID로 재생성되어 T0와 무관한 두 배 가까운
    # 집합이 생긴다(T0 100%와 T1이 서로소라 델타가 아니라 거의 전량이 됨).
    delta_order_count = max(1, round(order_count_t0 * change_rate_target))
    t0, t_boundary, t1 = _anchor_times(postgres)
    _assert_no_rows_at_or_after(postgres, t0)
    source_snapshot_id = resolve_source_snapshot_id(postgres)

    pipeline_incremental = f"bench-extract-{config.benchmark_id}-incremental"
    pipeline_full = f"bench-extract-{config.benchmark_id}-full"

    run_generator(
        _generator_config(source_snapshot_id, config.scale.random_seed, t0, order_count_t0),
        postgres,
    )

    with tempfile.TemporaryDirectory(prefix="bench-extract-setup-") as tmp:
        tmp_path = Path(tmp)
        t0_incremental = _ingest_all_tables(
            postgres,
            storage,
            pipeline_incremental,
            f"{pipeline_incremental}-setup",
            t_boundary,
            tmp_path,
        )
        _ingest_all_tables(
            postgres, storage, pipeline_full, f"{pipeline_full}-setup", t_boundary, tmp_path
        )

    run_generator(
        _generator_config(source_snapshot_id, config.scale.random_seed, t1, delta_order_count),
        postgres,
    )

    change_stats = _measure_change_stats(postgres, t_boundary)
    orders_stats = next(stats for stats in change_stats if stats.source_table == "orders")
    measured_change_rate = orders_stats.delta_row_count / orders_stats.t1_row_count

    return dataclasses.replace(
        config,
        parameters={
            **config.parameters,
            "change_rate": measured_change_rate,
            "t0": t0,
            "t_boundary": t_boundary,
            "t1": t1,
            "t0_incremental_results": tuple(t0_incremental),
            "change_stats": change_stats,
        },
    )


def run_extract_experiment(config: RunConfig) -> Mapping[str, ArmResult]:
    """고정된 T1 원천 위에서, 매 반복 Watermark만 되감아 Full/Incremental Ingest 비용을 잰다.

    `prepare_extract_fixture`가 반복 밖에서 Generator와 T0 선적재를 마쳤다고 가정한다.
    반복마다: Arm별 Watermark가 목표 지점에 있음을 보장하고(측정 밖, `_ensure_watermark_at`)
    → Ingest만 측정하고(measure 안) →
    Catalog Hash를 만든다(측정 밖). `config.run_number`를 DAG ID에 섞어 반복마다 다른
    Batch Identity를 만든다 — 같으면 멱등 재사용 경로로 빠져 실제 작업이 측정되지 않는다.
    """
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    t_boundary: datetime = config.parameters["t_boundary"]
    t0_incremental: tuple[TableIngestionResult, ...] = config.parameters["t0_incremental_results"]

    pipeline_incremental = f"bench-extract-{config.benchmark_id}-incremental"
    pipeline_full = f"bench-extract-{config.benchmark_id}-full"
    dag_incremental = f"{pipeline_incremental}-run{config.run_number}"
    dag_full = f"{pipeline_full}-run{config.run_number}"

    with tempfile.TemporaryDirectory(prefix="bench-extract-") as tmp:
        tmp_path = Path(tmp)

        incremental_rewind = tuple(
            _ensure_watermark_at(postgres, pipeline_incremental, source_table, t_boundary)
            for source_table in TABLE_CONFIGS
        )
        _assert_watermark_at_target(postgres, pipeline_incremental, t_boundary, config.run_number)
        full_rewind = tuple(
            _ensure_watermark_at(postgres, pipeline_full, source_table, _EPOCH)
            for source_table in TABLE_CONFIGS
        )
        _assert_watermark_at_target(postgres, pipeline_full, _EPOCH, config.run_number)

        with measure() as collector:
            incremental_t1 = _ingest_all_tables(
                postgres, storage, pipeline_incremental, dag_incremental, t_boundary, tmp_path
            )
        incremental_measurement = collector.result()

        with measure() as collector:
            full_t1 = _ingest_all_tables(
                postgres, storage, pipeline_full, dag_full, t_boundary, tmp_path
            )
        full_measurement = collector.result()

        incremental_hash, _ = _catalog_hash(
            tmp_path / "incremental-catalog.duckdb", list(t0_incremental) + incremental_t1
        )
        # Full Arm은 이번 반복에서 이미 빈 상태부터 전체를 다시 추출했으므로
        # T1 결과만으로 전체 상태가 완성된다. T0 결과를 더하면 중복 집계된다.
        full_hash, _ = _catalog_hash(tmp_path / "full-catalog.duckdb", full_t1)

    return {
        "incremental": ArmResult(
            "incremental",
            incremental_measurement,
            _row_counts(incremental_t1, storage),
            incremental_hash,
            cursor_range=_cursor_range_for_orders(incremental_rewind, incremental_t1),
        ),
        "full": ArmResult(
            "full",
            full_measurement,
            _row_counts(full_t1, storage),
            full_hash,
            cursor_range=_cursor_range_for_orders(full_rewind, full_t1),
        ),
    }


EXPERIMENTS[EXTRACT_SCENARIO.scenario] = run_extract_experiment


def _ensure_watermark_at(
    postgres: PostgresSettings, pipeline_name: str, source_table: str, target_boundary: datetime
) -> RewindOutcome:
    """Watermark가 `target_boundary` 직전 실제 Cursor에 있음을 보장한다(되감기 자체가 목적이 아니다).

    회차 1은 `prepare_extract_fixture`의 T0 선적재가 이미 그 지점에 세워 둔 상태라
    되감기가 무효(엄격 부등호 위반)한 요청이 된다 — 그래서 먼저 현재 위치를 확인하고,
    이미 목표면 아무것도 하지 않는다. 그 외에는 `rewind_tables` 하나로 실제로 되감는다.
    목표가 현재보다 미래인 진짜 이상 상태는 `rewind_tables`가 예외를 그대로 올려 드러낸다.
    """
    config = table_config(source_table)
    current = get_or_create_watermark(postgres, pipeline_name, source_table)
    target = cursor_before_timestamp(postgres, config, target_boundary)
    if current.cursor == target:
        return RewindOutcome(
            source_table=source_table,
            pipeline_name=pipeline_name,
            cursor_before=current.cursor,
            cursor_after=current.cursor,
            version_before=current.version,
            version_after=current.version,
        )
    (outcome,) = rewind_tables(
        postgres,
        source_tables=(source_table,),
        boundary=target_boundary,
        pipeline_name=pipeline_name,
    )
    return outcome


def _assert_watermark_at_target(
    postgres: PostgresSettings, pipeline_name: str, target_boundary: datetime, run_number: int
) -> None:
    """`_ensure_watermark_at` 호출 뒤 Watermark가 실제로 목표 지점에 있는지 확인한다.

    034의 정본이다 — `_ensure_watermark_at`의 불변식은 "되감기 실행 여부"가 아니라
    "목표 지점 보장"이었다(033 §4는 이 부분을 잘못 적어 철회됨). run1은 T0 선적재
    덕분에 구조적으로 무동작이고 run2부터는 되감기가 정상적으로 일어난다 — 그 자체는
    실패 조건이 아니다. 검사는 되감기 후 Cursor가 목표와 같은지만 본다.
    """
    for source_table in TABLE_CONFIGS:
        config = table_config(source_table)
        current = get_or_create_watermark(postgres, pipeline_name, source_table)
        target = cursor_before_timestamp(postgres, config, target_boundary)
        if current.cursor != target:
            raise RuntimeError(
                f"run{run_number} {pipeline_name}/{source_table}: Watermark가 목표 지점에 "
                f"없다(cursor={current.cursor}, target={target}). 034 사후 조건 위반."
            )
    print(
        f"run{run_number} {pipeline_name} watermark at target confirmed for {len(TABLE_CONFIGS)} tables",
        flush=True,
    )


def _anchor_times(postgres: PostgresSettings) -> tuple[datetime, datetime, datetime]:
    """9개 Source Table의 실측 최대 Cursor 시각 기준으로 T0·경계·T1 세 논리 시각을 만든다.

    035의 정본이다 — 벽시계(Benchmark ID 시각)로 Anchor를 뽑으면 원천이 영구 누적되는
    한(032) 직전 실행이 남긴 미래 시각 행보다 이번 T0가 과거가 되는 충돌이 재발한다.
    9개 Table 실측 최대값(`max_existing`)에서 뽑으면 `t0 > max_existing`이 가정이
    아니라 계산으로 보장되고, 이번 `t1`이 다음 실행의 `max_existing`이 되어 Anchor가
    단조 증가해 충돌이 구조적으로 사라진다. 간격 1분은 실험에 필요한 `t0 < t_boundary
    < t1` 순서만 지키면 되고 크기 자체는 의미가 없어 최소로 잡는다.
    """
    max_existing = _EPOCH
    for source_table in TABLE_CONFIGS:
        config = table_config(source_table)
        with postgres.source_connection() as connection, connection.transaction():
            connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            (table_max,) = connection.execute(
                f"SELECT MAX({config.cursor_timestamp_column}) FROM {config.source_table}"
            ).fetchone()
        if table_max is not None and table_max > max_existing:
            max_existing = table_max
    t0 = max_existing + timedelta(minutes=1)
    t_boundary = t0 + timedelta(minutes=1)
    t1 = t_boundary + timedelta(minutes=1)
    return t0, t_boundary, t1


def _assert_no_rows_at_or_after(postgres: PostgresSettings, t0: datetime) -> None:
    """Anchor 산출 직후, 9개 Table에 `t0` 이상인 행이 없는지 확인한다.

    035의 추가 요구다 — `max_existing` 계산이 실제로 `t0`보다 앞선다는 전제를
    Setup(수십 분)을 태우기 전에 확인해, 어긋나면 그 자리에서 멈춘다.
    """
    for source_table in TABLE_CONFIGS:
        config = table_config(source_table)
        with postgres.source_connection() as connection, connection.transaction():
            connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            (count,) = connection.execute(
                f"SELECT COUNT(*) FROM {config.source_table} "
                f"WHERE {config.cursor_timestamp_column} >= %s",
                (t0,),
            ).fetchone()
        if count > 0:
            raise RuntimeError(
                f"{source_table}: t0({t0.isoformat()}) 이상인 행이 {count}건 있다. "
                "035 Anchor 성립 조건 위반."
            )


def _generator_config(
    source_snapshot_id: str, random_seed: int, logical_date: datetime, order_count: int
) -> GeneratorConfig:
    """Benchmark 전용 결정적 Generator 입력을 만든다."""
    return GeneratorConfig(
        source_snapshot_id=source_snapshot_id,
        random_seed=random_seed,
        logical_date=logical_date,
        order_count=order_count,
        anomaly_profile="default",
        generator_version=GENERATOR_VERSION,
    )


@dataclasses.dataclass(frozen=True)
class TableChangeStats:
    """Table 하나의 T0·Delta·T1 전체 Row 수를 담는다(추정이 아니라 실측)."""

    source_table: str
    t0_row_count: int
    delta_row_count: int
    t1_row_count: int


def _measure_change_stats(
    postgres: PostgresSettings, t_boundary: datetime
) -> tuple[TableChangeStats, ...]:
    """9개 Table 각각의 T0(경계 이하)·Delta(경계 초과)·T1 전체 Row 수를 실제로 센다.

    경계 시각 자체의 Row는 T0에 속한다 — T0 Generator 호출이 `updated_at`/`created_at`을
    `t_boundary`로 채우므로, `<=`/`>` 경계로 갈라야 T0 자신이 Delta로 이중 집계되지 않는다.
    """
    stats = []
    for source_table in TABLE_CONFIGS:
        config = table_config(source_table)
        with postgres.source_connection() as connection, connection.transaction():
            connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            row = connection.execute(
                f"""
                SELECT
                    COUNT(*) FILTER (WHERE {config.cursor_timestamp_column} <= %s),
                    COUNT(*) FILTER (WHERE {config.cursor_timestamp_column} > %s),
                    COUNT(*)
                FROM {config.source_table}
                """,
                (t_boundary, t_boundary),
            ).fetchone()
        stats.append(TableChangeStats(source_table, row[0], row[1], row[2]))
    return tuple(stats)


def _ingest_all_tables(
    postgres: PostgresSettings,
    storage: SeaweedFSSettings,
    pipeline_name: str,
    dag_id: str,
    logical_date: datetime,
    tmp_path: Path,
) -> list[TableIngestionResult]:
    """등록된 9개 Table을 지정 Pipeline·DAG ID·Logical Date로 순서대로 수집한다."""
    results = []
    for source_table in TABLE_CONFIGS:
        request = TableIngestionRequest.for_dag_run(
            source_table=source_table,
            dag_id=dag_id,
            logical_date=logical_date,
            pipeline_name=pipeline_name,
        )
        result = ingest_table(postgres, storage, request, local_directory=tmp_path / "bronze")
        if result.status not in ("SUCCESS", "SUCCESS_NO_DATA"):
            raise RuntimeError(f"{source_table} ingest failed: {result.status}")
        results.append(result)
    return results


def _row_counts(results: list[TableIngestionResult], storage: SeaweedFSSettings) -> RowCounts:
    """측정 구간 Ingest 결과의 Row 수·Object 크기 합계를 집계한다.

    Postgres에서 읽은 Byte 수는 깨끗하게 잴 방법이 없어 `input_bytes`는 항상 None이다.
    """
    rows = sum(result.row_count for result in results)
    output_bytes = sum(
        stored_object_from_head(storage, result.object_key).size
        for result in results
        if result.object_key is not None
    )
    return RowCounts(
        rows_scanned=rows, rows_changed=rows, input_bytes=None, output_bytes=output_bytes
    )


def _format_cursor(cursor: CursorPosition | None) -> str:
    """CursorPosition을 Run Metadata 문자열용 짧은 표현으로 만든다."""
    if cursor is None or cursor.timestamp is None:
        return "none"
    return f"{cursor.timestamp.isoformat()}|{cursor.keys}"


def _cursor_range_for_orders(
    rewind_outcomes: tuple[RewindOutcome, ...], results: list[TableIngestionResult]
) -> str:
    """`orders` 기준 '되감기 전 -> 수집 후' Cursor Range를 만든다.

    Run Metadata의 `cursor_range`는 Arm당 문자열 하나뿐이라 9개 Table 전부를 담을 수
    없다. `change_rate` 산출 기준과 같은 `orders`를 대표로 삼는다.
    """
    rewind = next(outcome for outcome in rewind_outcomes if outcome.source_table == "orders")
    result = next(result for result in results if result.run.source_table == "orders")
    return f"{_format_cursor(rewind.cursor_before)} -> {_format_cursor(result.run.extract_upper_bound)}"


def _catalog_hash(catalog_path: Path, results: list[TableIngestionResult]) -> tuple[str, int]:
    """Ingest 결과로 격리 Catalog를 만들고 9개 Table의 결합 Hash를 계산한다."""
    _write_bronze_catalog(catalog_path, results)
    per_table = {table: bronze_logical_hash(catalog_path, table) for table in TABLE_CONFIGS}
    digest = hashlib.sha256()
    total_rows = 0
    for table in sorted(per_table):
        table_hash, row_count = per_table[table]
        digest.update(f"{table}:{table_hash}:{row_count}\n".encode())
        total_rows += row_count
    return digest.hexdigest(), total_rows


def _write_bronze_catalog(catalog_path: Path, results: list[TableIngestionResult]) -> None:
    """Ingest 결과의 Object Key만으로 격리된 Bronze Catalog DuckDB 파일을 만든다."""
    with duckdb.connect(str(catalog_path)) as connection:
        connection.execute("CREATE SCHEMA control")
        connection.execute(
            "CREATE TABLE control.bronze_files (source_table VARCHAR, object_key VARCHAR)"
        )
        rows = [
            (result.run.source_table, result.object_key)
            for result in results
            if result.object_key is not None
        ]
        connection.executemany("INSERT INTO control.bronze_files VALUES (?, ?)", rows)


def bronze_logical_hash(catalog_path: Path, table: str) -> tuple[str, int]:
    """Catalog에 등록된 한 Table의 커밋된 Bronze Row를 PK 순서로 Canonical Hash한다."""
    config = table_config(table)
    storage = SeaweedFSSettings.from_environment()
    column_list = ", ".join(f'"{column}"' for column in config.source_column_names)
    order_by = ", ".join(f'"{column}"' for column in config.primary_key_columns)
    with duckdb.connect(str(catalog_path)) as connection:
        object_keys = [
            row[0]
            for row in connection.execute(
                "SELECT object_key FROM control.bronze_files "
                "WHERE source_table = ? ORDER BY object_key",
                [table],
            ).fetchall()
        ]
        if not object_keys:
            return hashlib.sha256(b"").hexdigest(), 0
        configure_s3(connection, storage)
        paths = ", ".join(f"'s3://{storage.bucket}/{key}'" for key in object_keys)
        cursor = connection.execute(
            f"SELECT {column_list} FROM read_parquet([{paths}], union_by_name=true) "
            f"ORDER BY {order_by}"
        )
        return hash_cursor_rows(cursor)
