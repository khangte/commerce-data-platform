"""실험 C — Full Scan과 Filtered Scan이 같은 집계 결과에 도달하는 비용을 비교한다.

두 Arm은 완전히 같은 SQL을 실행한다. Full Scan Arm은 DuckDB Optimizer 중
Predicate/Projection Pushdown 관련 Rule만 꺼서 Pushdown이 없던 상태를 재현하고,
Filtered Scan Arm은 기본 설정 그대로 Pushdown 혜택을 받는다. Row·Byte 수는
`PRAGMA enable_profiling`을 측정 구간 밖 별도 Pass에서 실행해 얻는다. Postgres나
SeaweedFS에 의존하지 않는, 로컬 Parquet Fixture만 쓰는 실험이다.
"""

from __future__ import annotations

import hashlib
import json
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from src.benchmark.config import BenchmarkScenario, RunConfig
from src.benchmark.experiments import EXPERIMENTS, SCENARIOS
from src.benchmark.measure import RowCounts, measure
from src.benchmark.runner import ArmResult
from src.common.row_hash import canonical_row_json

_NO_PUSHDOWN_OPTIMIZERS = "filter_pushdown,row_group_pruner,unused_columns,column_lifetime"

SCAN_SCENARIO = BenchmarkScenario(
    scenario="scan",
    experiment="C",
    arms=("full_scan", "filtered_scan"),
    cold=False,
    description="Full Scan과 Filtered Scan이 같은 집계에 도달하는 비용을 비교한다",
)
SCENARIOS[SCAN_SCENARIO.scenario] = SCAN_SCENARIO


def run_scan_experiment(config: RunConfig) -> Mapping[str, ArmResult]:
    """오래된 Bucket과 최신 Bucket으로 나뉜 고정 Fixture에서 두 Scan 방식을 비교한다.

    두 Arm은 완전히 같은 SQL로 같은 집계를 낸다. Full Scan Arm만 Pushdown 관련
    Optimizer를 꺼서 Predicate/Projection Pushdown이 없던 경우를 재현한다.
    """
    current_rows = config.scale.order_count
    old_rows = current_rows * 20

    with tempfile.TemporaryDirectory(prefix="bench-scan-") as tmp:
        old_path, current_path = _write_fixture(Path(tmp), old_rows, current_rows)
        sql = _aggregate_sql(old_path, current_path)

        filtered_rows, filtered_bytes = profile_scan(_connect(), sql)
        full_rows, full_bytes = profile_scan(_connect(_NO_PUSHDOWN_OPTIMIZERS), sql)

        with measure() as collector:
            filtered_hash, filtered_output_bytes = _run_and_hash(_connect(), sql)
        filtered_measurement = collector.result()

        with measure() as collector:
            full_hash, full_output_bytes = _run_and_hash(_connect(_NO_PUSHDOWN_OPTIMIZERS), sql)
        full_measurement = collector.result()

    return {
        "filtered_scan": ArmResult(
            "filtered_scan",
            filtered_measurement,
            RowCounts(
                rows_scanned=filtered_rows,
                rows_changed=None,
                input_bytes=filtered_bytes,
                output_bytes=filtered_output_bytes,
            ),
            filtered_hash,
        ),
        "full_scan": ArmResult(
            "full_scan",
            full_measurement,
            RowCounts(
                rows_scanned=full_rows,
                rows_changed=None,
                input_bytes=full_bytes,
                output_bytes=full_output_bytes,
            ),
            full_hash,
        ),
    }


EXPERIMENTS[SCAN_SCENARIO.scenario] = run_scan_experiment


def run_filtered_scan_workload(current_rows: int) -> tuple[str, int]:
    """Filtered Scan Arm과 동일한 Query를 한 번 실행해 Result Hash·Payload Byte 수를 낸다.

    실험 D(Cache 효과)가 Full Scan Arm 없이 이 Workload 하나만 재사용할 수 있게
    공개해 둔 진입점이다.
    """
    old_rows = current_rows * 20
    with tempfile.TemporaryDirectory(prefix="bench-scan-workload-") as tmp:
        old_path, current_path = _write_fixture(Path(tmp), old_rows, current_rows)
        sql = _aggregate_sql(old_path, current_path)
        return _run_and_hash(_connect(), sql)


def profile_scan(
    connection: duckdb.DuckDBPyConnection, sql: str, params: Sequence[object] = ()
) -> tuple[int, int]:
    """Profiling을 켜고 한 번 실행해 Scan 단계의 Row 수·Byte 수를 얻은 뒤 끈다.

    측정 구간에는 포함하지 않는 별도 Pass다. Scan Operator의 Cardinality를
    Row 수로 쓴다: Pushdown이 없으면 Scan이 전체 Row를 그대로 내보내고,
    Pushdown이 있으면 이미 걸러진 Row만 내보내기 때문에 두 경우가 갈린다.
    """
    with tempfile.TemporaryDirectory(prefix="bench-scan-profile-") as tmp:
        profile_path = Path(tmp) / "profile.json"
        connection.execute("PRAGMA enable_profiling='json'")
        connection.execute(f"PRAGMA profiling_output='{profile_path}'")
        if params:
            connection.execute(sql, list(params)).fetchall()
        else:
            connection.execute(sql).fetchall()
        connection.execute("PRAGMA disable_profiling")
        data = json.loads(profile_path.read_text())
    connection.close()
    return _scanned_rows(data), int(data["total_bytes_read"])


def _scanned_rows(node: dict) -> int:
    """Plan Tree에서 Table Scan Operator들의 Cardinality 합을 구한다."""
    total = node.get("operator_cardinality", 0) if node.get("operator_type") == "TABLE_SCAN" else 0
    for child in node.get("children", []):
        total += _scanned_rows(child)
    return total


def _connect(disabled_optimizers: str = "") -> duckdb.DuckDBPyConnection:
    """필요하면 Pushdown 관련 Optimizer를 끈 새 DuckDB Connection을 만든다."""
    connection = duckdb.connect()
    if disabled_optimizers:
        connection.execute(f"SET disabled_optimizers='{disabled_optimizers}'")
    return connection


def _run_and_hash(connection: duckdb.DuckDBPyConnection, sql: str) -> tuple[str, int]:
    """같은 Query를 실행해 결과 Row를 Canonical Hash하고 Connection을 닫는다."""
    cursor = connection.execute(sql)
    column_names = [descriptor[0] for descriptor in cursor.description]
    rows = cursor.fetchall()
    connection.close()
    digest = hashlib.sha256()
    payload_bytes = 0
    for row in rows:
        payload = canonical_row_json(dict(zip(column_names, row, strict=True))).encode("utf-8")
        digest.update(payload)
        digest.update(b"\n")
        payload_bytes += len(payload)
    return digest.hexdigest(), payload_bytes


def _write_fixture(tmp_path: Path, old_rows: int, current_rows: int) -> tuple[Path, Path]:
    """오래된 Bucket과 최신 Bucket, 두 Parquet 파일을 결정적으로 만든다."""
    old_path = tmp_path / "old.parquet"
    current_path = tmp_path / "current.parquet"
    pq.write_table(
        pa.table(
            {
                "id": list(range(old_rows)),
                "bucket": ["old"] * old_rows,
                "amount": [float(index) for index in range(old_rows)],
            }
        ),
        old_path,
    )
    pq.write_table(
        pa.table(
            {
                "id": list(range(old_rows, old_rows + current_rows)),
                "bucket": ["current"] * current_rows,
                "amount": [float(index) for index in range(old_rows, old_rows + current_rows)],
            }
        ),
        current_path,
    )
    return old_path, current_path


def _aggregate_sql(old_path: Path, current_path: Path) -> str:
    """두 Arm이 완전히 똑같이 실행할 집계 Query를 만든다."""
    return (
        "SELECT count(*) AS row_count, sum(amount) AS total "
        f"FROM read_parquet(['{old_path}', '{current_path}']) "
        "WHERE bucket = 'current'"
    )
