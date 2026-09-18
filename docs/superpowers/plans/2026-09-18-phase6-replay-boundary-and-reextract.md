# Bronze Replay 경계와 Re-extract 되감기 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bronze 입력을 `bronze_as_of` 시점으로 잘라 Mart를 결정적으로 재현하고, Watermark Cursor를 지정 시각으로 되감아 명시 범위 Re-extract를 열어준다.

**Architecture:** dbt var `bronze_as_of` 하나가 `control.bronze_files` 조회에 `committed_at <=` 상한을 붙인다. 경계 Build는 Full Refresh만 허용하고, 재계산 Watermark를 전진시키지 않으며, `control.dbt_replay_boundary`에 입력 증거를 남긴다. Re-extract는 Table Lease 아래에서 Metadata Watermark Cursor를 과거의 실재 Cursor로 되감는 CLI로 연다. 되감기와 재추출은 분리한다.

**Tech Stack:** dbt-core 1.12.3, dbt-duckdb 1.11.0, DuckDB 1.5.5, psycopg 3, Python 3.12, pytest.

**Spec:** `docs/architecture/08-bronze-replay-and-reextract-boundary.md`

## Global Constraints

- ruff `line-length = 100`, `target-version = "py312"`. Format·Lint는 `ruff format`과 `ruff check`로 확인한다.
- 모든 Class·Function 선언 바로 아래에 한국어 Docstring을 쓴다 (`CLAUDE.md`).
- Bronze 경계 var 이름은 `bronze_as_of` 하나다. 값은 ISO-8601 UTC(`Z` 또는 `+00:00`)만 허용한다.
- 경계 위반 오류 접두사는 `REPLAY_BOUNDARY_ERROR:` 고정이다.
- 경계를 주지 않은 Build의 동작은 지금과 완전히 같아야 한다.
- `sql/metadata/004_create_ingestion_metadata.sql`의 Schema는 바꾸지 않는다. `bronze_objects`에 Column을 추가하지 않는다.
- 통합 Test는 `pytest.mark.integration`과 기존 환경 Flag(`RUN_POSTGRES_INTEGRATION`, `RUN_SEAWEEDFS_INTEGRATION`) 규약을 따른다.
- 되감기는 Metadata만 바꾼다. 되감기 CLI는 Source를 추출하지 않는다.

---

## File Structure

| 파일 | 책임 |
| --- | --- |
| `dbt/macros/replay_boundary.sql` (신규) | `bronze_as_of` 해석·검증, Catalog 조회 술어 생성, Build 모드 Guard, 경계 증거 기록 |
| `dbt/macros/bronze_source.sql` (수정) | Catalog 조회에 경계 술어 적용 |
| `dbt/macros/processed_batch_watermark.sql` (수정) | 경계 Build에서 Watermark 전진 Skip |
| `dbt/dbt_project.yml` (수정) | `on-run-start`에 Guard·증거 Hook 추가 |
| `src/ingestion/metadata.py` (수정) | `rewind_watermark()`와 `WatermarkRewindError` |
| `src/ingestion/extract.py` (수정) | `cursor_before_timestamp()` — 되감기 대상 Cursor를 Source에서 읽는다 |
| `src/ingestion/reprocess.py` (신규) | 되감기 CLI. Lease 획득 → 되감기 → 해제 |
| `tests/test_dbt_replay_boundary_macro.py` (신규) | 경계 해석·술어·Guard 단위 Test |
| `tests/test_dbt_watermark_macro.py` (수정) | 경계 Build Skip 계약 Test |
| `tests/integration/test_reprocess_rewind_integration.py` (신규) | 되감기 Metadata·CLI 통합 Test |
| `tests/integration/test_replay_boundary_hash_integration.py` (신규) | 시점 재현성 Hash Test (AC-07) |

---

### Task 1: 경계 해석과 Catalog 술어

**Files:**
- Create: `dbt/macros/replay_boundary.sql`
- Modify: `dbt/macros/bronze_source.sql`
- Test: `tests/test_dbt_replay_boundary_macro.py`

**Interfaces:**
- Consumes: 없음.
- Produces: Macro `replay_boundary()` → `none` 또는 `'YYYY-MM-DD HH:MM:SS[.ffffff]+00:00'` 문자열. Macro `replay_boundary_predicate(column_name='committed_at')` → `''` 또는 `" and committed_at <= timestamptz '...'"`.

- [ ] **Step 1: 실패하는 Test를 쓴다**

`tests/test_dbt_replay_boundary_macro.py`:

```python
"""Bronze Replay 경계 Macro의 값 검증과 Catalog 절단을 확인한다."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_boundary_excludes_objects_committed_after_the_as_of_timestamp(tmp_path: Path) -> None:
    """경계보다 늦게 Commit된 Object는 Bronze 입력 목록에서 빠진다."""
    warehouse_path = tmp_path / "warehouse.duckdb"
    _create_catalog(warehouse_path)

    result = _run_validate_catalog(
        warehouse_path, bronze_as_of="2026-09-10T00:00:00Z"
    )

    assert result.returncode == 0, _combined_output(result)
    assert "bronze/orders/early.parquet" in result.stdout
    assert "bronze/orders/late.parquet" not in result.stdout


def test_boundary_absent_keeps_every_committed_object(tmp_path: Path) -> None:
    """경계를 주지 않으면 지금과 같이 Commit된 Object를 모두 읽는다."""
    warehouse_path = tmp_path / "warehouse.duckdb"
    _create_catalog(warehouse_path)

    result = _run_validate_catalog(warehouse_path)

    assert result.returncode == 0, _combined_output(result)
    assert "bronze/orders/early.parquet" in result.stdout
    assert "bronze/orders/late.parquet" in result.stdout


def test_boundary_rejects_a_timestamp_without_utc_offset(tmp_path: Path) -> None:
    """UTC Offset이 없는 값은 Build 전에 차단한다."""
    warehouse_path = tmp_path / "warehouse.duckdb"
    _create_catalog(warehouse_path)

    result = _run_validate_catalog(warehouse_path, bronze_as_of="2026-09-10 00:00:00")

    assert result.returncode != 0
    assert "REPLAY_BOUNDARY_ERROR" in _combined_output(result)


def test_boundary_rejects_a_non_timestamp_value(tmp_path: Path) -> None:
    """Timestamp가 아닌 값은 Build 전에 차단한다."""
    warehouse_path = tmp_path / "warehouse.duckdb"
    _create_catalog(warehouse_path)

    result = _run_validate_catalog(warehouse_path, bronze_as_of="yesterday")

    assert result.returncode != 0
    assert "REPLAY_BOUNDARY_ERROR" in _combined_output(result)


def _create_catalog(warehouse_path: Path) -> None:
    """경계 앞뒤로 하나씩 Commit된 최소 Bronze Catalog를 만든다."""
    with duckdb.connect(str(warehouse_path)) as connection:
        connection.execute("CREATE SCHEMA control")
        connection.execute(
            """
            CREATE TABLE control.bronze_files (
                source_table VARCHAR NOT NULL,
                object_key VARCHAR PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                batch_id VARCHAR NOT NULL,
                committed_at TIMESTAMPTZ NOT NULL,
                row_count BIGINT NOT NULL,
                logical_hash VARCHAR NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO control.bronze_files VALUES
            ('orders', 'bronze/orders/early.parquet', 3, 'batch-1',
             timestamptz '2026-09-09 00:00:00+00', 1, 'a'),
            ('orders', 'bronze/orders/late.parquet', 3, 'batch-2',
             timestamptz '2026-09-11 00:00:00+00', 1, 'b')
            """
        )


def _run_validate_catalog(
    warehouse_path: Path, *, bronze_as_of: str | None = None
) -> subprocess.CompletedProcess[str]:
    """선택한 경계로 dbt Catalog 사전 검증 Operation을 실행한다."""
    environment = {
        **os.environ,
        "WAREHOUSE_PATH": str(warehouse_path),
        "SEAWEEDFS_BUCKET": "test-bucket",
        "SEAWEEDFS_ACCESS_KEY": "test-access-key",
        "SEAWEEDFS_SECRET_KEY": "test-secret-key",
    }
    command = [
        str(Path(sys.executable).with_name("dbt")),
        "run-operation",
        "validate_bronze_catalog",
        "--project-dir",
        "dbt",
        "--profiles-dir",
        "dbt",
    ]
    if bronze_as_of is not None:
        command += ["--vars", json.dumps({"bronze_as_of": bronze_as_of})]
    return subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


def _combined_output(result: subprocess.CompletedProcess[str]) -> str:
    """dbt 버전에 따라 달라지는 표준 출력·오류 출력을 함께 비교한다."""
    return f"{result.stdout}\n{result.stderr}"
```

- [ ] **Step 2: Test가 실패하는지 확인한다**

Run: `.venv/bin/pytest tests/test_dbt_replay_boundary_macro.py -v`
Expected: FAIL. 경계를 준 실행이 `late.parquet`을 여전히 포함하고, 잘못된 값도 `returncode == 0`이다.

- [ ] **Step 3: 경계 Macro를 만든다**

`dbt/macros/replay_boundary.sql`:

```jinja
{% macro replay_boundary() -%}
    {#- bronze_as_of var를 검증해 DuckDB Literal용 UTC 문자열 또는 none을 돌려준다. -#}
    {%- set raw = var('bronze_as_of', none) -%}
    {%- if raw is none -%}
        {{ return(none) }}
    {%- endif -%}
    {%- set value = raw | string | trim -%}
    {%- if value == '' -%}
        {{ return(none) }}
    {%- endif -%}
    {%- set pattern = '^\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,6})?(Z|\\+00:00)$' -%}
    {%- if modules.re.match(pattern, value) is none -%}
        {{ exceptions.raise_compiler_error(
            'REPLAY_BOUNDARY_ERROR: bronze_as_of must be an ISO-8601 UTC timestamp, got ' ~ value
        ) }}
    {%- endif -%}
    {{ return(value | replace('Z', '+00:00') | replace('T', ' ')) }}
{%- endmacro %}


{% macro replay_boundary_predicate(column_name='committed_at') -%}
    {#- 경계가 있으면 Catalog 조회에 붙일 AND 절을, 없으면 빈 문자열을 돌려준다. -#}
    {%- set boundary = replay_boundary() -%}
    {%- if boundary is none -%}
        {{ return('') }}
    {%- endif -%}
    {{ return(' and ' ~ column_name ~ " <= timestamptz '" ~ boundary ~ "'") }}
{%- endmacro %}
```

- [ ] **Step 4: `bronze_source()`에 경계를 적용한다**

`dbt/macros/bronze_source.sql`에서 Catalog 조회 두 곳을 고친다.

기존:

```jinja
        {%- set catalog_query -%}
            select object_key, schema_version
            from control.bronze_files
            where source_table = '{{ source_table }}'
            order by committed_at, object_key
        {%- endset -%}
```

수정:

```jinja
        {%- set catalog_query -%}
            select object_key, schema_version
            from control.bronze_files
            where source_table = '{{ source_table }}'
            {{ replay_boundary_predicate() }}
            order by committed_at, object_key
        {%- endset -%}
```

기존:

```jinja
        {%- set invalid_versions_query -%}
            select distinct schema_version
            from control.bronze_files
            where schema_version != 3
            order by schema_version
        {%- endset -%}
```

수정:

```jinja
        {%- set invalid_versions_query -%}
            select distinct schema_version
            from control.bronze_files
            where schema_version != 3
            {{ replay_boundary_predicate() }}
            order by schema_version
        {%- endset -%}
```

경계 밖 Object의 Schema Version이 경계 안 Build를 막으면 안 되므로 검증 조회에도 같은 경계를 쓴다.

- [ ] **Step 5: Test가 통과하는지 확인한다**

Run: `.venv/bin/pytest tests/test_dbt_replay_boundary_macro.py tests/test_dbt_catalog_macro.py -v`
Expected: PASS. 기존 Catalog Macro Test도 함께 통과해야 한다(무경계 동작 불변).

- [ ] **Step 6: Commit**

```bash
git add dbt/macros/replay_boundary.sql dbt/macros/bronze_source.sql tests/test_dbt_replay_boundary_macro.py
git commit -m "feat: bound bronze input by an as-of timestamp"
```

---

### Task 2: Build 모드 Guard와 경계 증거

**Files:**
- Modify: `dbt/macros/replay_boundary.sql`
- Modify: `dbt/dbt_project.yml`
- Test: `tests/test_dbt_replay_boundary_macro.py`

**Interfaces:**
- Consumes: `replay_boundary()`, `replay_boundary_predicate()` (Task 1).
- Produces: Macro `assert_replay_boundary_mode()`, Macro `record_replay_boundary()`. Table `control.dbt_replay_boundary(invocation_id, bronze_as_of, object_count, max_committed_at, full_refresh, recorded_at)`.

- [ ] **Step 1: 실패하는 Test를 쓴다**

`tests/test_dbt_replay_boundary_macro.py`에 이어 붙인다.

```python
def test_boundary_records_the_input_evidence_row(tmp_path: Path) -> None:
    """경계 Build는 읽은 Object 수와 경계를 증거 Table에 남긴다."""
    warehouse_path = tmp_path / "warehouse.duckdb"
    _create_catalog(warehouse_path)

    result = _run_operation(
        warehouse_path, "record_replay_boundary", bronze_as_of="2026-09-10T00:00:00Z"
    )

    assert result.returncode == 0, _combined_output(result)
    with duckdb.connect(str(warehouse_path)) as connection:
        rows = connection.execute(
            "SELECT bronze_as_of, object_count, full_refresh FROM control.dbt_replay_boundary"
        ).fetchall()

    assert len(rows) == 1
    assert rows[0][1] == 1
    assert rows[0][0] is not None


def test_boundary_evidence_records_unbounded_builds_as_null(tmp_path: Path) -> None:
    """무경계 Build도 증거를 남기고 경계 Column은 NULL이다."""
    warehouse_path = tmp_path / "warehouse.duckdb"
    _create_catalog(warehouse_path)

    result = _run_operation(warehouse_path, "record_replay_boundary")

    assert result.returncode == 0, _combined_output(result)
    with duckdb.connect(str(warehouse_path)) as connection:
        rows = connection.execute(
            "SELECT bronze_as_of, object_count FROM control.dbt_replay_boundary"
        ).fetchall()

    assert rows == [(None, 2)]


def test_boundary_evidence_skips_when_the_catalog_is_absent(tmp_path: Path) -> None:
    """Bronze Catalog가 아직 없으면 증거 기록은 Build를 막지 않는다."""
    warehouse_path = tmp_path / "warehouse.duckdb"

    result = _run_operation(warehouse_path, "record_replay_boundary")

    assert result.returncode == 0, _combined_output(result)


def test_boundary_requires_full_refresh() -> None:
    """경계 Build는 Full Refresh만 허용한다는 계약을 Macro가 담는다."""
    macro = (PROJECT_ROOT / "dbt/macros/replay_boundary.sql").read_text(encoding="utf-8")

    assert "flags.FULL_REFRESH" in macro
    assert "REPLAY_BOUNDARY_ERROR: bronze_as_of requires --full-refresh" in macro


def _run_operation(
    warehouse_path: Path, operation: str, *, bronze_as_of: str | None = None
) -> subprocess.CompletedProcess[str]:
    """격리된 Warehouse에 dbt Operation 하나를 선택한 경계로 실행한다."""
    environment = {
        **os.environ,
        "WAREHOUSE_PATH": str(warehouse_path),
        "SEAWEEDFS_BUCKET": "test-bucket",
        "SEAWEEDFS_ACCESS_KEY": "test-access-key",
        "SEAWEEDFS_SECRET_KEY": "test-secret-key",
    }
    command = [
        str(Path(sys.executable).with_name("dbt")),
        "run-operation",
        operation,
        "--project-dir",
        "dbt",
        "--profiles-dir",
        "dbt",
    ]
    if bronze_as_of is not None:
        command += ["--vars", json.dumps({"bronze_as_of": bronze_as_of})]
    return subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
```

- [ ] **Step 2: Test가 실패하는지 확인한다**

Run: `.venv/bin/pytest tests/test_dbt_replay_boundary_macro.py -v`
Expected: FAIL. `record_replay_boundary` Macro가 없어 `run-operation`이 실패한다.

- [ ] **Step 3: Guard와 증거 Macro를 만든다**

`dbt/macros/replay_boundary.sql` 끝에 붙인다.

```jinja
{% macro assert_replay_boundary_mode() -%}
    {#- 경계 Build는 Full Refresh만 허용한다. Incremental 혼합 결과를 막는다. -#}
    {%- if execute -%}
        {%- if replay_boundary() is not none and not flags.FULL_REFRESH -%}
            {{ exceptions.raise_compiler_error(
                'REPLAY_BOUNDARY_ERROR: bronze_as_of requires --full-refresh'
            ) }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}


{% macro record_replay_boundary() -%}
    {#- 이번 Build가 어떤 경계로 몇 개 Object를 읽었는지 증거로 남긴다. -#}
    {%- if execute -%}
        {%- do run_query("CREATE SCHEMA IF NOT EXISTS control") -%}
        {%- do run_query("
            CREATE TABLE IF NOT EXISTS control.dbt_replay_boundary (
                invocation_id VARCHAR NOT NULL,
                bronze_as_of TIMESTAMPTZ,
                object_count BIGINT NOT NULL,
                max_committed_at TIMESTAMPTZ,
                full_refresh BOOLEAN NOT NULL,
                recorded_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
            )
        ") -%}
        {%- set catalog_exists = run_query("
            select count(*) as table_count
            from information_schema.tables
            where table_schema = 'control' and table_name = 'bronze_files'
        ") -%}
        {%- if catalog_exists.rows[0][0] == 0 -%}
            {%- do log('Skipping replay boundary evidence: control.bronze_files is absent', info=true) -%}
        {%- else -%}
            {%- set boundary = replay_boundary() -%}
            {%- if boundary is none -%}
                {%- set boundary_literal = 'cast(null as timestamptz)' -%}
            {%- else -%}
                {%- set boundary_literal = "timestamptz '" ~ boundary ~ "'" -%}
            {%- endif -%}
            {%- set record_sql -%}
                insert into control.dbt_replay_boundary
                    (invocation_id, bronze_as_of, object_count, max_committed_at, full_refresh)
                select
                    '{{ invocation_id }}',
                    {{ boundary_literal }},
                    count(*),
                    max(committed_at),
                    {{ 'true' if flags.FULL_REFRESH else 'false' }}
                from control.bronze_files
                where true {{ replay_boundary_predicate() }}
            {%- endset -%}
            {%- do run_query(record_sql) -%}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
```

- [ ] **Step 4: Hook을 등록한다**

`dbt/dbt_project.yml`의 `on-run-start`를 바꾼다.

기존:

```yaml
on-run-start:
  - "{{ ensure_processed_batch_watermark() }}"
```

수정:

```yaml
on-run-start:
  - "{{ assert_replay_boundary_mode() }}"
  - "{{ ensure_processed_batch_watermark() }}"
  - "{{ record_replay_boundary() }}"
```

Guard를 먼저 둔다. 모드가 틀렸으면 아무것도 기록하지 않고 멈춰야 한다.

- [ ] **Step 5: Test가 통과하는지 확인한다**

Run: `.venv/bin/pytest tests/test_dbt_replay_boundary_macro.py -v`
Expected: PASS (7건).

- [ ] **Step 6: Commit**

```bash
git add dbt/macros/replay_boundary.sql dbt/dbt_project.yml tests/test_dbt_replay_boundary_macro.py
git commit -m "feat: guard and record replay boundary builds"
```

---

### Task 3: 경계 Build의 Watermark 미전진

**Files:**
- Modify: `dbt/macros/processed_batch_watermark.sql`
- Test: `tests/test_dbt_watermark_macro.py`

**Interfaces:**
- Consumes: `replay_boundary()` (Task 1).
- Produces: 없음. `advance_processed_batch_watermark()`의 동작만 바뀐다.

- [ ] **Step 1: 실패하는 Test를 쓴다**

`tests/test_dbt_watermark_macro.py`에 붙인다.

```python
def test_watermark_advance_skips_boundary_replay_builds() -> None:
    """bronze_as_of 경계 Build는 재계산 경계를 전진시키지 않는다."""
    macro = (PROJECT_ROOT / "dbt/macros/processed_batch_watermark.sql").read_text(
        encoding="utf-8"
    )

    assert "execute and replay_boundary() is none" in macro
    assert "Skipping watermark advance: bronze_as_of replay boundary build" in macro
```

- [ ] **Step 2: Test가 실패하는지 확인한다**

Run: `.venv/bin/pytest tests/test_dbt_watermark_macro.py -v`
Expected: FAIL. 새 Test가 두 문자열을 찾지 못한다.

- [ ] **Step 3: Macro에 Guard를 넣는다**

`dbt/macros/processed_batch_watermark.sql`의 `advance_processed_batch_watermark()`에서 두 곳만 고친다.

기존(진입부):

```jinja
    {%- if execute -%}
        {%- set required_facts = [
```

수정:

```jinja
    {%- if execute and replay_boundary() is none -%}
        {%- set required_facts = [
```

기존(말미):

```jinja
            {%- endif -%}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
```

수정:

```jinja
            {%- endif -%}
        {%- endif -%}
    {%- elif execute -%}
        {%- do log("Skipping watermark advance: bronze_as_of replay boundary build", info=true) -%}
    {%- endif -%}
{%- endmacro %}
```

본문 들여쓰기는 건드리지 않는다.

- [ ] **Step 4: Test가 통과하는지 확인한다**

Run: `.venv/bin/pytest tests/test_dbt_watermark_macro.py -v`
Expected: PASS (4건).

- [ ] **Step 5: Commit**

```bash
git add dbt/macros/processed_batch_watermark.sql tests/test_dbt_watermark_macro.py
git commit -m "fix: hold the watermark during boundary replay builds"
```

---

### Task 4: Watermark 되감기 Metadata 함수

**Files:**
- Modify: `src/ingestion/metadata.py`
- Test: `tests/integration/test_reprocess_rewind_integration.py`

**Interfaces:**
- Consumes: 기존 `CursorPosition`, `Watermark`, `_get_or_create_watermark`, `_utc_now`.
- Produces:
  - `class WatermarkRewindError(RuntimeError)`
  - `def rewind_watermark(settings: PostgresSettings, *, pipeline_name: str, source_table: str, owner_id: uuid.UUID, expected_version: int, cursor: CursorPosition, now: datetime | None = None) -> Watermark`

`lease.py`가 `metadata.py`를 import하므로 반대 방향 import를 만들지 않는다. Lease Snapshot 대신 `owner_id`와 `expected_version`을 값으로 받는다.

- [ ] **Step 1: 실패하는 Test를 쓴다**

`tests/integration/test_reprocess_rewind_integration.py`:

```python
"""Watermark 되감기가 Lease·방향·CAS 조건을 지키는지 검증한다."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from src.common.database import PostgresSettings
from src.ingestion.lease import (
    TableLeaseOwnershipLostError,
    acquire_table_lease,
    release_table_lease,
)
from src.ingestion.metadata import (
    CursorPosition,
    WatermarkRewindError,
    get_or_create_watermark,
    rewind_watermark,
)

pytestmark = pytest.mark.integration

REWIND_SKIP = pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the PostgreSQL container.",
)

BASE_TIME = datetime(2026, 9, 10, 0, 0, tzinfo=UTC)


@REWIND_SKIP
def test_rewind_moves_the_cursor_back_and_bumps_the_version() -> None:
    """Lease 소유자는 Cursor를 과거로 내리고 Version을 올린다."""
    postgres = PostgresSettings.from_environment()
    pipeline_name = f"test_rewind_{uuid.uuid4().hex}"
    _seed_cursor(postgres, pipeline_name, CursorPosition(BASE_TIME, ("customer-9",)))
    lease = acquire_table_lease(
        postgres,
        pipeline_name=pipeline_name,
        source_table="customers",
        owner_id=uuid.uuid4(),
    )
    try:
        rewound = rewind_watermark(
            postgres,
            pipeline_name=pipeline_name,
            source_table="customers",
            owner_id=lease.owner_id,
            expected_version=lease.watermark_version,
            cursor=CursorPosition(BASE_TIME - timedelta(days=1), ("customer-1",)),
        )

        assert rewound.cursor.timestamp == BASE_TIME - timedelta(days=1)
        assert rewound.cursor.keys == ("customer-1",)
        assert rewound.version == lease.watermark_version + 1
    finally:
        _cleanup(postgres, pipeline_name)


@REWIND_SKIP
def test_rewind_rejects_a_forward_target() -> None:
    """현재보다 이후 Cursor로의 이동은 되감기가 아니므로 거부한다."""
    postgres = PostgresSettings.from_environment()
    pipeline_name = f"test_rewind_{uuid.uuid4().hex}"
    _seed_cursor(postgres, pipeline_name, CursorPosition(BASE_TIME, ("customer-9",)))
    lease = acquire_table_lease(
        postgres,
        pipeline_name=pipeline_name,
        source_table="customers",
        owner_id=uuid.uuid4(),
    )
    try:
        with pytest.raises(WatermarkRewindError):
            rewind_watermark(
                postgres,
                pipeline_name=pipeline_name,
                source_table="customers",
                owner_id=lease.owner_id,
                expected_version=lease.watermark_version,
                cursor=CursorPosition(BASE_TIME + timedelta(days=1), ("customer-9",)),
            )
    finally:
        _cleanup(postgres, pipeline_name)


@REWIND_SKIP
def test_rewind_requires_the_table_lease() -> None:
    """Lease를 쥐지 않은 호출자는 되감을 수 없다."""
    postgres = PostgresSettings.from_environment()
    pipeline_name = f"test_rewind_{uuid.uuid4().hex}"
    _seed_cursor(postgres, pipeline_name, CursorPosition(BASE_TIME, ("customer-9",)))
    watermark = get_or_create_watermark(postgres, pipeline_name, "customers")
    try:
        with pytest.raises(TableLeaseOwnershipLostError):
            rewind_watermark(
                postgres,
                pipeline_name=pipeline_name,
                source_table="customers",
                owner_id=uuid.uuid4(),
                expected_version=watermark.version,
                cursor=CursorPosition(BASE_TIME - timedelta(days=1), ("customer-1",)),
            )
    finally:
        _cleanup(postgres, pipeline_name)


def _seed_cursor(
    postgres: PostgresSettings, pipeline_name: str, cursor: CursorPosition
) -> None:
    """되감기 대상 Watermark를 원하는 Cursor로 준비한다."""
    from psycopg.types.json import Jsonb

    get_or_create_watermark(postgres, pipeline_name, "customers")
    with postgres.pipeline_connection() as connection:
        connection.execute(
            """
            UPDATE watermarks
            SET watermark_timestamp = %s, watermark_keys = %s, updated_at = now()
            WHERE pipeline_name = %s AND source_table = %s
            """,
            (cursor.timestamp, Jsonb(cursor.as_json()), pipeline_name, "customers"),
        )
        connection.commit()


def _cleanup(postgres: PostgresSettings, pipeline_name: str) -> None:
    """Test가 만든 Watermark 행만 지운다."""
    with postgres.pipeline_connection() as connection:
        connection.execute(
            "DELETE FROM watermarks WHERE pipeline_name = %s", (pipeline_name,)
        )
        connection.commit()
```

`release_table_lease`는 Test에서 쓰지 않는다. 되감기가 Version을 올리면 Lease Snapshot이 낡기 때문이며, `_cleanup`이 행 자체를 지운다.

- [ ] **Step 2: Test가 실패하는지 확인한다**

Run: `RUN_POSTGRES_INTEGRATION=1 .venv/bin/pytest tests/integration/test_reprocess_rewind_integration.py -v`
Expected: FAIL with `ImportError: cannot import name 'rewind_watermark'`.

- [ ] **Step 3: 되감기 함수를 만든다**

`src/ingestion/metadata.py`의 `WatermarkConflictError` 아래에 Error를 추가한다.

```python
class WatermarkRewindError(RuntimeError):
    """되감기 대상 Cursor가 현재 Cursor보다 과거가 아닐 때 발생한다."""
```

`get_or_create_watermark()` 아래에 함수를 추가한다.

```python
def rewind_watermark(
    settings: PostgresSettings,
    *,
    pipeline_name: str,
    source_table: str,
    owner_id: uuid.UUID,
    expected_version: int,
    cursor: CursorPosition,
    now: datetime | None = None,
) -> Watermark:
    """Table Lease 소유자만 Watermark Cursor를 과거로 되감고 새 Snapshot을 돌려준다."""
    current_time = _utc_now(now)
    with settings.pipeline_connection() as connection, connection.transaction():
        current = _get_or_create_watermark(
            connection, pipeline_name, source_table, current_time
        )
        if current.version != expected_version:
            raise WatermarkConflictError("Watermark changed before rewind")
        if not _cursor_is_earlier(cursor, current.cursor):
            raise WatermarkRewindError("Rewind target must be earlier than the current cursor")
        updated = connection.execute(
            """
            UPDATE watermarks
            SET watermark_timestamp = %s,
                watermark_keys = %s,
                version = version + 1,
                updated_at = %s
            WHERE pipeline_name = %s
              AND source_table = %s
              AND version = %s
              AND lease_owner = %s
              AND lease_expires_at > %s
            """,
            (
                cursor.timestamp,
                Jsonb(cursor.as_json()),
                current_time,
                pipeline_name,
                source_table,
                expected_version,
                owner_id,
                current_time,
            ),
        )
        if updated.rowcount != 1:
            raise TableLeaseOwnershipLostError("Table lease ownership was lost before rewind")
    return Watermark(
        pipeline_name=pipeline_name,
        source_table=source_table,
        cursor=cursor,
        version=expected_version + 1,
    )


def _cursor_is_earlier(candidate: CursorPosition, current: CursorPosition) -> bool:
    """되감기 대상 Cursor가 현재 Cursor보다 과거인지 판단한다."""
    if current.timestamp is None:
        return False
    if candidate.timestamp is None:
        return True
    return (candidate.timestamp, candidate.keys) < (current.timestamp, current.keys)
```

`TableLeaseOwnershipLostError`는 `lease.py`에 있고 `lease.py`가 `metadata.py`를 import하므로, 같은 Error를 `metadata.py`에서 import하면 순환한다. `metadata.py`에 Error를 정의하고 `lease.py`가 그것을 다시 export하도록 옮긴다.

`src/ingestion/metadata.py`에 정의를 옮긴다.

```python
class TableLeaseOwnershipLostError(RuntimeError):
    """Lease 소유권을 잃은 상태에서 Metadata를 쓰려 할 때 발생한다."""
```

`src/ingestion/lease.py`의 기존 정의를 지우고 import로 바꾼다.

기존:

```python
class TableLeaseOwnershipLostError(RuntimeError):
```

수정: 해당 Class 선언과 Docstring을 지우고, 파일 상단 `from src.ingestion.metadata import (...)` 목록에 `TableLeaseOwnershipLostError`를 추가한다. `lease.py`를 통해 import하던 기존 호출부는 그대로 동작한다.

- [ ] **Step 4: Test가 통과하는지 확인한다**

Run: `RUN_POSTGRES_INTEGRATION=1 .venv/bin/pytest tests/integration/test_reprocess_rewind_integration.py -v`
Expected: PASS (3건).

- [ ] **Step 5: 회귀를 확인한다**

Run: `.venv/bin/pytest tests -m "not integration" -q`
Expected: 기존 건수 유지, 실패 0건. Lease Error 이동이 기존 import를 깨지 않았는지 확인한다.

- [ ] **Step 6: Commit**

```bash
git add src/ingestion/metadata.py src/ingestion/lease.py tests/integration/test_reprocess_rewind_integration.py
git commit -m "feat: rewind the ingestion watermark under a table lease"
```

---

### Task 5: 되감기 CLI

**Files:**
- Modify: `src/ingestion/extract.py`
- Create: `src/ingestion/reprocess.py`
- Test: `tests/integration/test_reprocess_rewind_integration.py`

**Interfaces:**
- Consumes: `rewind_watermark()` (Task 4), `acquire_table_lease()`, `release_table_lease()`, `TABLE_CONFIGS`, `table_config()`.
- Produces:
  - `def cursor_before_timestamp(settings: PostgresSettings, config: TableConfig, boundary: datetime) -> CursorPosition`
  - `@dataclass(frozen=True) class RewindOutcome` — `source_table`, `pipeline_name`, `cursor_before`, `cursor_after`, `version_before`, `version_after`
  - `def rewind_tables(settings: PostgresSettings, *, source_tables: Sequence[str], boundary: datetime, pipeline_name: str | None = None, dry_run: bool = False) -> tuple[RewindOutcome, ...]`
  - `def main(argv: list[str] | None = None) -> int`

- [ ] **Step 1: 실패하는 Test를 쓴다**

`tests/integration/test_reprocess_rewind_integration.py`에 붙인다.

```python
@REWIND_SKIP
def test_cursor_before_timestamp_returns_the_last_row_before_the_boundary() -> None:
    """되감기 대상 Cursor는 경계 직전에 실재하는 행에서 나온다."""
    postgres = PostgresSettings.from_environment()
    unique_id = uuid.uuid4().hex
    _seed_customers(postgres, unique_id)
    try:
        cursor = cursor_before_timestamp(
            postgres, table_config("customers"), BASE_TIME
        )

        assert cursor.timestamp is not None
        assert cursor.timestamp < BASE_TIME
    finally:
        _cleanup_customers(postgres, unique_id)


@REWIND_SKIP
def test_cli_rewinds_every_requested_table_and_reports_json(capsys) -> None:
    """CLI는 Table별 되감기 결과를 JSON으로 보고하고 0을 돌려준다."""
    postgres = PostgresSettings.from_environment()
    unique_id = uuid.uuid4().hex
    pipeline_name = f"test_rewind_{uuid.uuid4().hex}"
    _seed_customers(postgres, unique_id)
    _seed_cursor(postgres, pipeline_name, CursorPosition(BASE_TIME, ("customer-9",)))
    try:
        exit_code = main(
            [
                "--tables",
                "customers",
                "--reprocess-from",
                BASE_TIME.isoformat().replace("+00:00", "Z"),
                "--pipeline-name",
                pipeline_name,
            ]
        )

        assert exit_code == 0
        report = json.loads(capsys.readouterr().out)
        assert report[0]["source_table"] == "customers"
        assert report[0]["version_after"] == report[0]["version_before"] + 1
        assert report[0]["cursor_after"]["timestamp"] < BASE_TIME.isoformat()
    finally:
        _cleanup(postgres, pipeline_name)
        _cleanup_customers(postgres, unique_id)


def _seed_customers(postgres: PostgresSettings, unique_id: str) -> None:
    """경계 앞뒤로 한 행씩 Source Customer를 넣는다."""
    with postgres.source_connection() as connection:
        connection.execute(
            """
            INSERT INTO customers (
                customer_id, customer_unique_id, customer_city, customer_state, created_at
            )
            VALUES (%s, %s, 'sao paulo', 'SP', %s), (%s, %s, 'sao paulo', 'SP', %s)
            """,
            (
                f"rewind-early-{unique_id}",
                f"rewind-unique-early-{unique_id}",
                BASE_TIME - timedelta(days=2),
                f"rewind-late-{unique_id}",
                f"rewind-unique-late-{unique_id}",
                BASE_TIME + timedelta(days=2),
            ),
        )
        connection.commit()


def _cleanup_customers(postgres: PostgresSettings, unique_id: str) -> None:
    """Test가 넣은 Source 행만 지운다."""
    with postgres.source_connection() as connection:
        connection.execute(
            "DELETE FROM customers WHERE customer_id LIKE %s", (f"rewind-%-{unique_id}",)
        )
        connection.commit()
```

Import 문을 파일 상단에 더한다.

```python
import json

from src.ingestion.extract import cursor_before_timestamp
from src.ingestion.reprocess import main
from src.ingestion.tables import table_config
```

- [ ] **Step 2: Test가 실패하는지 확인한다**

Run: `RUN_POSTGRES_INTEGRATION=1 .venv/bin/pytest tests/integration/test_reprocess_rewind_integration.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.ingestion.reprocess'`.

- [ ] **Step 3: Source Cursor 조회 함수를 만든다**

`src/ingestion/extract.py`의 `open_table_snapshot()` 아래에 추가한다.

```python
def cursor_before_timestamp(
    settings: PostgresSettings, config: TableConfig, boundary: datetime
) -> CursorPosition:
    """경계 시각 직전에 실재하는 마지막 Composite Cursor를 돌려준다."""
    _assert_supported_config(config)
    _assert_utc(boundary, "boundary")
    with settings.source_connection() as connection, connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        row = connection.execute(
            f"""
            SELECT {config.cursor_timestamp_column}, {", ".join(config.cursor_key_columns)}
            FROM {config.source_table}
            WHERE {config.cursor_timestamp_column} < %s
            ORDER BY {_order_by(config, descending=True)}
            LIMIT 1
            """,
            (boundary,),
        ).fetchone()
    if row is None:
        return CursorPosition(None, ())
    return CursorPosition(
        row[0], _cursor_key_values(config, dict(zip(config.cursor_key_columns, row[1:])))
    )
```

Timestamp만으로 자르는 것이 핵심이다. Composite 비교로 자르면 경계 시각에 걸친 행이 남아 재추출에서 빠진다.

`_assert_utc`가 `extract.py`에 없으면 `from src.ingestion.metadata import _assert_utc` 대신 파일 안에 작은 검증을 둔다.

```python
def _assert_boundary_is_utc(boundary: datetime) -> None:
    """되감기 경계가 UTC Timestamp인지 확인한다."""
    if boundary.tzinfo is None or boundary.utcoffset() != timedelta(0):
        raise ValueError("boundary must be a UTC datetime")
```

그리고 `cursor_before_timestamp()`에서 `_assert_utc(boundary, "boundary")` 대신 `_assert_boundary_is_utc(boundary)`를 부른다. 어느 쪽이든 `datetime`, `timedelta` import를 확인한다.

- [ ] **Step 4: CLI를 만든다**

`src/ingestion/reprocess.py`:

```python
"""명시 시각으로 Watermark를 되감아 Re-extract 입력 경계를 연다."""

from __future__ import annotations

import argparse
import json
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from src.common.database import PostgresSettings
from src.ingestion.extract import cursor_before_timestamp
from src.ingestion.lease import acquire_table_lease, release_table_lease
from src.ingestion.metadata import CursorPosition, get_or_create_watermark, rewind_watermark
from src.ingestion.tables import TABLE_CONFIGS, table_config


@dataclass(frozen=True)
class RewindOutcome:
    """한 Source Table의 되감기 전후 Cursor와 Version이다."""

    source_table: str
    pipeline_name: str
    cursor_before: CursorPosition
    cursor_after: CursorPosition
    version_before: int
    version_after: int

    def as_json(self) -> dict[str, object]:
        """CLI 표준 출력에 실을 JSON 표현을 돌려준다."""
        return {
            "source_table": self.source_table,
            "pipeline_name": self.pipeline_name,
            "cursor_before": self.cursor_before.as_metadata_json(),
            "cursor_after": self.cursor_after.as_metadata_json(),
            "version_before": self.version_before,
            "version_after": self.version_after,
        }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """되감기 대상 Table과 UTC 경계 시각을 CLI 인자로 읽는다."""
    parser = argparse.ArgumentParser(
        description="Rewind ingestion watermarks so a bounded re-extract can run."
    )
    parser.add_argument(
        "--tables",
        required=True,
        nargs="+",
        choices=sorted(TABLE_CONFIGS),
        help="Source tables to rewind.",
    )
    parser.add_argument(
        "--reprocess-from",
        required=True,
        help="UTC timestamp in ISO-8601 format. Rows at or after it are re-extracted.",
    )
    parser.add_argument(
        "--pipeline-name",
        default=None,
        help="Pipeline name override. Defaults to <table>_bronze.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve the rewind target without writing metadata.",
    )
    return parser.parse_args(argv)


def rewind_tables(
    settings: PostgresSettings,
    *,
    source_tables: Sequence[str],
    boundary: datetime,
    pipeline_name: str | None = None,
    dry_run: bool = False,
) -> tuple[RewindOutcome, ...]:
    """Table마다 Lease를 잡고 경계 직전 Cursor로 Watermark를 되감는다."""
    outcomes: list[RewindOutcome] = []
    for source_table in source_tables:
        config = table_config(source_table)
        resolved_pipeline = pipeline_name or f"{source_table}_bronze"
        target = cursor_before_timestamp(settings, config, boundary)
        current = get_or_create_watermark(settings, resolved_pipeline, source_table)
        if dry_run:
            outcomes.append(
                RewindOutcome(
                    source_table=source_table,
                    pipeline_name=resolved_pipeline,
                    cursor_before=current.cursor,
                    cursor_after=target,
                    version_before=current.version,
                    version_after=current.version,
                )
            )
            continue
        lease = acquire_table_lease(
            settings,
            pipeline_name=resolved_pipeline,
            source_table=source_table,
            owner_id=uuid.uuid4(),
        )
        try:
            rewound = rewind_watermark(
                settings,
                pipeline_name=resolved_pipeline,
                source_table=source_table,
                owner_id=lease.owner_id,
                expected_version=lease.watermark_version,
                cursor=target,
            )
        finally:
            release_table_lease(settings, lease)
        outcomes.append(
            RewindOutcome(
                source_table=source_table,
                pipeline_name=resolved_pipeline,
                cursor_before=current.cursor,
                cursor_after=rewound.cursor,
                version_before=current.version,
                version_after=rewound.version,
            )
        )
    return tuple(outcomes)


def main(argv: list[str] | None = None) -> int:
    """되감기를 실행하고 Table별 결과를 JSON으로 보고한다."""
    args = parse_args(argv)
    boundary = datetime.fromisoformat(args.reprocess_from.replace("Z", "+00:00"))
    settings = PostgresSettings.from_environment()
    outcomes = rewind_tables(
        settings,
        source_tables=args.tables,
        boundary=boundary,
        pipeline_name=args.pipeline_name,
        dry_run=args.dry_run,
    )
    print(json.dumps([outcome.as_json() for outcome in outcomes], ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

되감기가 `version`을 올려 Lease Snapshot이 낡지만, `release_table_lease()`는 소유자만 보고 해제하므로 정상 동작한다.

- [ ] **Step 5: Test가 통과하는지 확인한다**

Run: `RUN_POSTGRES_INTEGRATION=1 .venv/bin/pytest tests/integration/test_reprocess_rewind_integration.py -v`
Expected: PASS (5건).

- [ ] **Step 6: Lint**

Run: `.venv/bin/ruff format src/ingestion/reprocess.py src/ingestion/extract.py src/ingestion/metadata.py && .venv/bin/ruff check src tests`
Expected: 오류 0건.

- [ ] **Step 7: Commit**

```bash
git add src/ingestion/reprocess.py src/ingestion/extract.py tests/integration/test_reprocess_rewind_integration.py
git commit -m "feat: add a watermark rewind cli for bounded re-extract"
```

---

### Task 6: 시점 재현성 Hash 통합 Test

**Files:**
- Create: `tests/integration/test_replay_boundary_hash_integration.py`

**Interfaces:**
- Consumes: Task 1~3의 경계 동작, `src/warehouse/mart_hash.py`의 `mart_logical_hashes`·`describe_mart_difference`, `tests/integration/test_subscription_payment_temporal_join_integration`의 Fixture Helper.
- Produces: 없음.

이 Test가 AC-07의 Warehouse 쪽 증거다. 기존 Fixture Helper를 import해서 쓰고, 그 파일을 수정하지 않는다.

- [ ] **Step 1: Test를 쓴다**

`tests/integration/test_replay_boundary_hash_integration.py`:

```python
"""as-of 경계 Replay가 그 시점 Build를 Hash까지 재현하는지 검증한다."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from src.common.database import PostgresSettings
from src.ingestion.storage import SeaweedFSSettings
from src.ingestion.service import TableIngestionResult
from src.warehouse.mart_hash import describe_mart_difference, mart_logical_hashes, target_for
from tests.integration.test_subscription_payment_temporal_join_integration import (
    FIXTURE_START,
    _append_fixture_catalog,
    _cleanup,
    _combined_output,
    _create_fixture_catalog,
    _ingest,
    _set_watermark,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
def test_as_of_replay_reproduces_the_build_of_that_moment(tmp_path: Path) -> None:
    """둘째 Batch 수집 뒤에도 첫 Batch 시점 경계 Replay는 첫 Build와 Hash가 같다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    pipeline_name = f"test_replay_boundary_{uuid.uuid4().hex}"
    ingested_at = datetime.now(UTC)
    results: list[TableIngestionResult] = []
    customer_unique_id = ""
    try:
        first_results, customer_unique_id = _ingest_first_batch(
            postgres, storage, pipeline_name, tmp_path, ingested_at
        )
        results.extend(first_results)

        first_warehouse = tmp_path / "first.duckdb"
        _create_fixture_catalog(postgres, first_warehouse, results)
        first_build = _run_build(first_warehouse, storage, tmp_path, "first")
        assert first_build.returncode == 0, _combined_output(first_build)
        first_hashes = mart_logical_hashes(first_warehouse)
        boundary = _max_committed_at(first_warehouse)

        second_results = _ingest_second_batch(
            postgres, storage, pipeline_name, tmp_path, ingested_at
        )
        results.extend(second_results)

        replay_warehouse = tmp_path / "replay.duckdb"
        shutil.copyfile(first_warehouse, replay_warehouse)
        _append_fixture_catalog(postgres, replay_warehouse, second_results)
        replay_build = _run_build(
            replay_warehouse,
            storage,
            tmp_path,
            "replay",
            bronze_as_of=boundary,
        )
        assert replay_build.returncode == 0, _combined_output(replay_build)
        replay_hashes = mart_logical_hashes(replay_warehouse)

        mismatched = [name for name, value in first_hashes.items() if replay_hashes[name] != value]
        report = "\n".join(
            describe_mart_difference(first_warehouse, replay_warehouse, target_for(name))
            for name in mismatched
        )
        assert mismatched == [], report

        with duckdb.connect(str(replay_warehouse)) as connection:
            boundary_rows = connection.execute(
                "SELECT bronze_as_of, object_count FROM control.dbt_replay_boundary"
                " WHERE bronze_as_of IS NOT NULL"
            ).fetchall()
            watermark_rows = connection.execute(
                "SELECT count(*) FROM control.dbt_processed_batch"
            ).fetchone()

        assert len(boundary_rows) == 1
        assert boundary_rows[0][1] == len(first_results)
        assert watermark_rows[0] == 1
    finally:
        _cleanup(postgres, storage, pipeline_name, results, customer_unique_id)


def test_boundary_build_without_full_refresh_is_rejected(tmp_path: Path) -> None:
    """경계를 준 Incremental Build는 Model 실행 전에 막힌다."""
    warehouse_path = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(warehouse_path)) as connection:
        connection.execute("CREATE SCHEMA control")

    result = _run_build(
        warehouse_path,
        None,
        tmp_path,
        "guard",
        bronze_as_of="2026-09-10 00:00:00+00:00",
        full_refresh=False,
    )

    assert result.returncode != 0
    assert "REPLAY_BOUNDARY_ERROR" in _combined_output(result)


def _max_committed_at(warehouse_path: Path) -> str:
    """Catalog에 실린 Object의 최대 Commit 시각을 경계 값으로 돌려준다."""
    with duckdb.connect(str(warehouse_path)) as connection:
        row = connection.execute(
            "SELECT max(committed_at) FROM control.bronze_files"
        ).fetchone()
    return row[0].astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f+00:00")


def _run_build(
    warehouse_path: Path,
    storage: SeaweedFSSettings | None,
    tmp_path: Path,
    label: str,
    *,
    bronze_as_of: str | None = None,
    full_refresh: bool = True,
) -> subprocess.CompletedProcess[str]:
    """지정한 Warehouse에만 경계 Build를 실행한다."""
    environment = {
        **os.environ,
        "WAREHOUSE_PATH": str(warehouse_path),
        "SEAWEEDFS_BUCKET": storage.bucket if storage else "test-bucket",
        "SEAWEEDFS_ACCESS_KEY": storage.access_key if storage else "test-access-key",
        "SEAWEEDFS_SECRET_KEY": storage.secret_key if storage else "test-secret-key",
    }
    if storage is not None:
        environment["SEAWEEDFS_HOST"] = storage.host
        environment["SEAWEEDFS_S3_PORT"] = str(storage.port)
    command = [
        str(Path(sys.executable).with_name("dbt")),
        "build",
        "--project-dir",
        "dbt",
        "--profiles-dir",
        "dbt",
        "--target-path",
        str(tmp_path / f"dbt-target-{label}"),
    ]
    if full_refresh:
        command.insert(2, "--full-refresh")
    if bronze_as_of is not None:
        command += ["--vars", json.dumps({"bronze_as_of": bronze_as_of})]
    return subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
```

- [ ] **Step 2: Fixture 두 Batch Helper를 쓴다**

같은 파일에 붙인다. `tests/integration/test_incremental_full_refresh_hash_integration.py`의 Fixture 구성을 따르되, 첫 Batch와 둘째 Batch를 분리해 돌려준다.

```python
def _ingest_first_batch(
    postgres: PostgresSettings,
    storage: SeaweedFSSettings,
    pipeline_name: str,
    tmp_path: Path,
    ingested_at: datetime,
) -> tuple[list[TableIngestionResult], str]:
    """기준 Batch를 Source에 넣고 Bronze까지 수집한다."""
    raise NotImplementedError


def _ingest_second_batch(
    postgres: PostgresSettings,
    storage: SeaweedFSSettings,
    pipeline_name: str,
    tmp_path: Path,
    ingested_at: datetime,
) -> list[TableIngestionResult]:
    """첫 Batch 이후에 도착한 지연 Batch를 수집한다."""
    raise NotImplementedError
```

두 Helper의 본문은 `tests/integration/test_incremental_full_refresh_hash_integration.py:55` 이후의 Fixture 구성 절차를 그대로 옮겨 적는다. 그 Test는 한 함수 안에서 Source 행 생성 → `_set_watermark` → `_ingest` 순으로 두 Batch를 만든다. 첫 Batch에 해당하는 구간을 `_ingest_first_batch`에, 지연 Payment 구간을 `_ingest_second_batch`에 넣고, 첫 Helper는 `customer_unique_id`를 함께 돌려준다. `FIXTURE_START`와 `timedelta` 기준 시각은 원본과 같은 값을 쓴다.

- [ ] **Step 3: Docker를 띄우고 Test를 실행한다**

Run:

```bash
docker compose up -d
RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 \
  .venv/bin/pytest tests/integration/test_replay_boundary_hash_integration.py -v
```

Expected: PASS (2건). 실패 시 `describe_mart_difference` 보고가 어떤 Mart의 어떤 Key가 다른지 출력한다.

- [ ] **Step 4: 전체 회귀**

Run:

```bash
RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 .venv/bin/pytest tests -q
```

Expected: 기존 통과 건수 + 신규 건수, 실패 0건.

- [ ] **Step 5: Commit**

```bash
git add tests/integration/test_replay_boundary_hash_integration.py
git commit -m "test: reproduce a past build with an as-of replay boundary"
```

---

## 완료 보고

모든 Task를 마치면 architect에게 보고한다.

```bash
say architect "[developer] P6-24 Replay 경계·되감기 구현 완료. 커밋: <hash 목록>. 단위 N건, 통합 M건 PASS."
```

설계와 다르게 구현해야 할 이유를 발견하면 구현하기 전에 architect에게 판단을 요청한다.
