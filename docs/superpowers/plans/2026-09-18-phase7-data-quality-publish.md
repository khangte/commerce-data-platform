# Phase 7 Data Quality & Publish Gate Implementation Plan

> **For agentic workers:** Implement this plan one task at a time, in order. Steps use checkbox (`- [ ]`) syntax for tracking. After each task, report the result to architect. Do not use subagent or worktree execution.

**Goal:** Publish the warehouse only after dbt build and dbt test pass, using build-then-swap. Record and reproduce ingestion quality rules and corruption scenarios. Show pipeline and publish status in one query. Prove the whole Phase 0–7 pipeline from a clean clone.

**Architecture:**
- Each warehouse run copies the published DuckDB file to `build/{publish_run_id}.duckdb`.
- The run syncs the bronze catalog and runs `dbt build` on that copy only.
- On success, CHECKPOINT, hashes and row counts run. Then `os.replace` swaps the copy into the published path. A failed copy moves to `failed/`, and the published file stays unchanged.
- The run state lives in Postgres `mart_publish_runs`.
- A partial unique index allows only one active run (BUILDING or PUBLISHING) at a time.

**Tech Stack:** Python 3.12, DuckDB 1.5.5, dbt-duckdb 1.11 / dbt-core 1.12.3, psycopg 3, Airflow SDK DAG, SeaweedFS S3, pytest, uv, ruff (line length 100).

**Spec:** `docs/superpowers/specs/2026-09-18-phase7-data-quality-publish-design.md` (PRD: `PRD_v1.13.md` §11, §12, §13.2, §17)

## Global Constraints

- Put a short docstring directly below every new class and function. Write it in Korean unless it must be English (code, identifier, SQL).
- Write non-ASCII strings as literal UTF-8. Never use `\uXXXX` escapes.
- ruff line length is 100. Run `uv run ruff check .` before each commit.
- Never print, cat, or commit `.env`. Tests read credentials only through `PostgresSettings.from_environment()` and `environment_values()`.
- Integration tests use `pytestmark = pytest.mark.integration` and skip unless `RUN_POSTGRES_INTEGRATION=1` (plus `RUN_SEAWEEDFS_INTEGRATION=1` when S3 is touched). The real-dbt publish gate test also needs `RUN_DBT_PUBLISH_INTEGRATION=1`.
- `FAILED_BUILD_RETENTION = 3` (keep the newest 3 failed `.duckdb` files by mtime).
- `PUBLISH_STALE_AFTER = timedelta(hours=1)`.
- Active publish mutex index name: `mart_publish_runs_single_active_idx`.
- `classify_error` default becomes `UNKNOWN_ERROR`. New types: `DBT_BUILD_ERROR`, `DBT_TEST_ERROR`, `UNKNOWN_ERROR`.
- After Task 5, never run bare `dbt build` against `data/warehouse/commerce.duckdb`. Use `uv run python -m src.warehouse.publish`.
- Commit messages: use `/caveman:caveman-commit`. End each message with `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>`.
- Commit code, SQL, dbt, tests, scripts, and README. **Do not commit anything under `docs/`** except `docs/architect-review/`. Task 11 writes docs files but leaves them uncommitted.
- Run shell commands from the project root `/home/kang/projects/commerce-data-platform`.

## File Structure

| Path | Kind | Responsibility |
|---|---|---|
| `src/warehouse/errors.py` | Create | Leaf module. Warehouse error type constants and exception classes |
| `src/ingestion/errors.py` | Modify | Classify warehouse exceptions; default `UNKNOWN_ERROR` |
| `sql/metadata/005_create_mart_publish_runs.sql` | Create | `mart_publish_runs` table, CHECKs, mutex index |
| `src/warehouse/publish_metadata.py` | Create | Publish run state transitions in Postgres |
| `src/warehouse/dbt_runner.py` | Create | Run `dbt build` on a build file and classify the result |
| `src/warehouse/mart_hash.py` | Modify | Add `mart_row_counts` |
| `src/warehouse/publish.py` | Create | Build-then-swap orchestration, recovery, CLI |
| `airflow/dags/warehouse_pipeline_dag.py` | Modify | Replace catalog/dbt tasks with prepare → dbt_build → publish_mart |
| `src/rebaseline.py` | Modify | Refuse rebaseline during an active publish |
| `src/ingestion/rules.py` | Create | Ordered validation rule catalog |
| `src/ingestion/validation.py` | Modify | Use rule codes from `rules.py` |
| `src/ingestion/corruption.py` | Modify | Add `DUPLICATE_PRIMARY_KEY` and `apply_page` |
| `src/ingestion/service.py` | Modify | Use `CorruptionPlan.apply_page` |
| `dbt/tests/generic/non_negative.sql` | Create | Generic non-negative test |
| `dbt/tests/*.sql` | Create | Singular quality tests and publish gate canary |
| `dbt/models/marts/facts/schema.yml` | Modify | Status domains, FK, non-negative tests |
| `sql/validation/observability_run_status.sql` | Create | One status row per batch |
| `scripts/verify_clean_clone.sh` | Create | Clean-clone Phase 0–7 verification |
| `README.md` | Modify | Phase 0–7 verification section |
| `docs/adr/016-publish-mart-via-warehouse-file-swap.md`, `docs/phases/phase-07-*.md`, `ROADMAP.md` | Create/Modify | Docs (not committed) |

---

### Task 1: Warehouse error types and classification

**Files:**
- Create: `src/warehouse/errors.py`
- Modify: `src/ingestion/errors.py`
- Test: `tests/ingestion/test_errors.py` (create if missing; else append)

**Interfaces:**
- Produces: `src.warehouse.errors` with `DBT_BUILD_ERROR = "DBT_BUILD_ERROR"`, `DBT_TEST_ERROR = "DBT_TEST_ERROR"`, `UNKNOWN_ERROR = "UNKNOWN_ERROR"`, `WarehouseBuildError(error_type: str, message: str)` with attribute `.error_type`, `PublishedWalError`, `PublishInProgressError`, `PublishStateError`.
- Produces: `src.ingestion.errors.classify_error` returns `LEASE_UNAVAILABLE` for `PublishInProgressError`, `error.error_type` for `WarehouseBuildError`, `CONFIGURATION_ERROR` for `PublishedWalError`, and `UNKNOWN_ERROR` for anything unknown. `src.ingestion.errors` re-exports `DBT_BUILD_ERROR`, `DBT_TEST_ERROR`, `UNKNOWN_ERROR`.

- [ ] **Step 1: Write the failing test**

Run `ls tests/ingestion/test_errors.py` first. If the file exists, append the tests below and merge the imports.

```python
"""Error Type 분류 규칙을 검증한다."""

from __future__ import annotations

import pytest

from src.ingestion.errors import (
    CONFIGURATION_ERROR,
    DBT_BUILD_ERROR,
    DBT_TEST_ERROR,
    LEASE_UNAVAILABLE,
    UNKNOWN_ERROR,
    classify_error,
    is_retryable,
)
from src.warehouse.errors import (
    PublishedWalError,
    PublishInProgressError,
    WarehouseBuildError,
)


def test_unknown_exception_is_classified_as_unknown_error() -> None:
    """등록되지 않은 예외는 CONFIGURATION_ERROR가 아니라 UNKNOWN_ERROR로 분류된다."""
    assert classify_error(RuntimeError("boom")) == UNKNOWN_ERROR
    assert not is_retryable(RuntimeError("boom"))


@pytest.mark.parametrize("error_type", [DBT_BUILD_ERROR, DBT_TEST_ERROR])
def test_warehouse_build_error_keeps_its_own_type(error_type: str) -> None:
    """WarehouseBuildError는 생성 시 받은 Error Type을 그대로 반환한다."""
    assert classify_error(WarehouseBuildError(error_type, "failed")) == error_type


def test_publish_in_progress_is_retryable_lease_unavailable() -> None:
    """다른 Publish가 진행 중이면 재시도 가능한 LEASE_UNAVAILABLE이다."""
    error = PublishInProgressError("active publish")
    assert classify_error(error) == LEASE_UNAVAILABLE
    assert is_retryable(error)


def test_published_wal_is_configuration_error() -> None:
    """Published 파일에 WAL이 남아 있으면 운영 설정 오류로 분류한다."""
    assert classify_error(PublishedWalError("wal")) == CONFIGURATION_ERROR


def test_warehouse_build_error_rejects_empty_type() -> None:
    """빈 Error Type으로는 WarehouseBuildError를 만들 수 없다."""
    with pytest.raises(ValueError):
        WarehouseBuildError("", "failed")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/ingestion/test_errors.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.warehouse.errors'`

- [ ] **Step 3: Create `src/warehouse/errors.py`**

```python
"""Warehouse Build·Publish 단계의 Error Type 상수와 예외를 정의한다."""

from __future__ import annotations

DBT_BUILD_ERROR = "DBT_BUILD_ERROR"
DBT_TEST_ERROR = "DBT_TEST_ERROR"
UNKNOWN_ERROR = "UNKNOWN_ERROR"


class WarehouseBuildError(RuntimeError):
    """dbt Build 또는 Test 실패를 분류된 Error Type과 함께 전달한다."""

    def __init__(self, error_type: str, message: str) -> None:
        """Error Type과 요약 메시지를 받아 예외를 만든다."""
        if not error_type:
            raise ValueError("error_type must not be empty")
        super().__init__(message)
        self.error_type = error_type


class PublishedWalError(RuntimeError):
    """Published 파일 또는 CHECKPOINT 후 Build 파일에 WAL이 남아 Publish를 거부할 때 발생한다."""


class PublishInProgressError(RuntimeError):
    """다른 활성 Publish Run이 있어 새 Run을 시작할 수 없을 때 발생한다."""


class PublishStateError(RuntimeError):
    """Publish Run 상태 전이가 기대 상태와 맞지 않을 때 발생한다."""
```

- [ ] **Step 4: Modify `src/ingestion/errors.py`**

Add the import below the existing `src.ingestion.verification` import:

```python
from src.warehouse.errors import (
    DBT_BUILD_ERROR,
    DBT_TEST_ERROR,
    UNKNOWN_ERROR,
    PublishedWalError,
    PublishInProgressError,
    WarehouseBuildError,
)
```

Add this line below the constant block. It uses the three imported constants, so ruff F401 stays quiet, and callers can import them from `src.ingestion.errors`:

```python
WAREHOUSE_ERROR_TYPES = frozenset({DBT_BUILD_ERROR, DBT_TEST_ERROR, UNKNOWN_ERROR})
```

Extend the lease-unavailable tuple:

```python
_LEASE_UNAVAILABLE_EXCEPTION_TYPES: tuple[type[Exception], ...] = (
    LeaseUnavailableError,
    TableLeaseUnavailableError,
    PublishInProgressError,
)
```

Replace the tail of `classify_error` (from `if isinstance(error, BatchIdentityConflictError):` to the end of the function) with:

```python
    if isinstance(error, BatchIdentityConflictError):
        return BATCH_IDENTITY_CONFLICT
    if isinstance(error, WarehouseBuildError):
        return error.error_type
    if isinstance(error, PublishedWalError):
        return CONFIGURATION_ERROR
    return UNKNOWN_ERROR
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/ingestion/test_errors.py tests/test_airflow_dags.py -v`
Expected: PASS. Then run `grep -rn "CONFIGURATION_ERROR" src tests airflow` and confirm no caller depends on the old default.

- [ ] **Step 6: Commit**

```bash
uv run ruff check src/warehouse/errors.py src/ingestion/errors.py tests/ingestion/test_errors.py
git add src/warehouse/errors.py src/ingestion/errors.py tests/ingestion/test_errors.py
git commit  # message via /caveman:caveman-commit, e.g. "feat(errors): add warehouse error types, default UNKNOWN_ERROR"
```

---

### Task 2: Publish run metadata

**Files:**
- Create: `sql/metadata/005_create_mart_publish_runs.sql`
- Create: `src/warehouse/publish_metadata.py`
- Test: `tests/integration/test_publish_metadata_integration.py`

**Interfaces:**
- Consumes: `PublishInProgressError`, `PublishStateError` (Task 1); `PostgresSettings.pipeline_connection()`, `apply_sql_file` from `src.common.database`.
- Produces (`src.warehouse.publish_metadata`):
  - Constants `BUILDING`, `PUBLISHING`, `PUBLISHED`, `FAILED`, `ACTIVE_STATUSES = (BUILDING, PUBLISHING)`, `ACTIVE_PUBLISH_INDEX = "mart_publish_runs_single_active_idx"`.
  - `@dataclass(frozen=True) PublishRun(publish_run_id: uuid.UUID, pipeline_name: str, batch_id: str | None = None, dag_run_id: str | None = None)`
  - `@dataclass(frozen=True) PublishRecord(publish_run_id: uuid.UUID, status: str, started_at: datetime, previous_publish_run_id: uuid.UUID | None, mart_hashes: dict[str, str] | None, error_type: str | None)`
  - `ensure_publish_metadata(settings) -> None`
  - `start_publish_run(settings, run, *, now: datetime) -> PublishRecord` (raises `PublishInProgressError`)
  - `record_dbt_result(settings, publish_run_id, *, invocation_id: str | None, tests_passed: int, tests_failed: int) -> None`
  - `mark_publishing(settings, publish_run_id, *, mart_hashes: dict[str, str], mart_row_counts: dict[str, int]) -> None`
  - `mark_published(settings, publish_run_id, *, now: datetime) -> None`
  - `mark_failed(settings, publish_run_id, *, error_type: str, error_message: str, failed_path: str | None, now: datetime) -> None`
  - `get_publish_run(settings, publish_run_id) -> PublishRecord | None`
  - `stale_active_runs(settings, *, older_than: datetime) -> tuple[PublishRecord, ...]`
  - `assert_no_active_publish(settings) -> None` (raises `PublishInProgressError`)

- [ ] **Step 1: Write the failing integration test**

```python
"""mart_publish_runs 상태 전이와 단일 활성 Run 제약을 실제 PostgreSQL에서 검증한다."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest

from src.common.database import PostgresSettings
from src.warehouse.errors import PublishInProgressError, PublishStateError
from src.warehouse.publish_metadata import (
    BUILDING,
    FAILED,
    PUBLISHED,
    PUBLISHING,
    PublishRun,
    assert_no_active_publish,
    ensure_publish_metadata,
    get_publish_run,
    mark_failed,
    mark_published,
    mark_publishing,
    record_dbt_result,
    stale_active_runs,
    start_publish_run,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
        reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
    ),
]

NOW = datetime(2026, 9, 18, tzinfo=UTC)


@pytest.fixture
def settings() -> Iterator[PostgresSettings]:
    """활성 Publish가 없는 상태에서만 테스트하고, 만든 Row를 정리한다."""
    settings = PostgresSettings.from_environment()
    ensure_publish_metadata(settings)
    try:
        assert_no_active_publish(settings)
    except PublishInProgressError:
        pytest.skip("A real publish run is active; rerun after it finishes.")
    yield settings
    with settings.pipeline_connection() as connection:
        connection.execute(
            "DELETE FROM mart_publish_runs WHERE pipeline_name LIKE 'test_publish_%'"
        )


def _run() -> PublishRun:
    """테스트 전용 Pipeline 이름을 가진 Publish Run을 만든다."""
    return PublishRun(uuid.uuid4(), f"test_publish_{uuid.uuid4().hex[:8]}", "batch-1", "dag-run")


def test_full_success_transition_links_previous_published_run(settings) -> None:
    """BUILDING→PUBLISHING→PUBLISHED 전이 후 다음 Run은 직전 PUBLISHED Run을 가리킨다."""
    first = start_publish_run(settings, _run(), now=NOW)
    assert first.status == BUILDING
    record_dbt_result(
        settings, first.publish_run_id, invocation_id="inv-1", tests_passed=10, tests_failed=0
    )
    mark_publishing(
        settings,
        first.publish_run_id,
        mart_hashes={"dimensions.dim_date": "abc"},
        mart_row_counts={"dimensions.dim_date": 3},
    )
    mark_published(settings, first.publish_run_id, now=NOW)
    stored = get_publish_run(settings, first.publish_run_id)
    assert stored is not None
    assert stored.status == PUBLISHED
    assert stored.mart_hashes == {"dimensions.dim_date": "abc"}

    second = start_publish_run(settings, _run(), now=NOW + timedelta(minutes=1))
    assert second.previous_publish_run_id == first.publish_run_id
    mark_failed(
        settings,
        second.publish_run_id,
        error_type="DBT_TEST_ERROR",
        error_message="x" * 5000,
        failed_path="failed/x.duckdb",
        now=NOW,
    )
    failed = get_publish_run(settings, second.publish_run_id)
    assert failed is not None
    assert failed.status == FAILED
    assert failed.error_type == "DBT_TEST_ERROR"


def test_second_active_run_is_rejected(settings) -> None:
    """BUILDING Run이 있으면 두 번째 Run 시작과 Rebaseline Guard가 모두 거부된다."""
    active = start_publish_run(settings, _run(), now=NOW)
    with pytest.raises(PublishInProgressError):
        start_publish_run(settings, _run(), now=NOW)
    with pytest.raises(PublishInProgressError):
        assert_no_active_publish(settings)
    mark_failed(
        settings, active.publish_run_id, error_type="UNKNOWN_ERROR",
        error_message="cleanup", failed_path=None, now=NOW,
    )
    assert_no_active_publish(settings)


def test_invalid_transition_raises_state_error(settings) -> None:
    """BUILDING에서 곧바로 PUBLISHED로 가는 전이는 거부된다."""
    active = start_publish_run(settings, _run(), now=NOW)
    with pytest.raises(PublishStateError):
        mark_published(settings, active.publish_run_id, now=NOW)
    mark_failed(
        settings, active.publish_run_id, error_type="UNKNOWN_ERROR",
        error_message="cleanup", failed_path=None, now=NOW,
    )


def test_publishing_requires_hash_object(settings) -> None:
    """PUBLISHING 상태는 mart_hashes JSON Object 없이 저장될 수 없다."""
    active = start_publish_run(settings, _run(), now=NOW)
    with settings.pipeline_connection() as connection, pytest.raises(Exception):
        connection.execute(
            "UPDATE mart_publish_runs SET status = 'PUBLISHING' WHERE publish_run_id = %s",
            (active.publish_run_id,),
        )
    mark_failed(
        settings, active.publish_run_id, error_type="UNKNOWN_ERROR",
        error_message="cleanup", failed_path=None, now=NOW,
    )


def test_stale_active_runs_returns_only_old_active_rows(settings) -> None:
    """기준 시각보다 먼저 시작한 활성 Run만 Stale로 반환한다."""
    old = start_publish_run(settings, _run(), now=NOW - timedelta(hours=2))
    stale = stale_active_runs(settings, older_than=NOW - timedelta(hours=1))
    assert [record.publish_run_id for record in stale] == [old.publish_run_id]
    assert stale_active_runs(settings, older_than=NOW - timedelta(hours=3)) == ()
    mark_failed(
        settings, old.publish_run_id, error_type="UNKNOWN_ERROR",
        error_message="cleanup", failed_path=None, now=NOW,
    )
```

Note: the fixture deletes only rows whose `pipeline_name` starts with `test_publish_`. `previous_publish_run_id` uses `ON DELETE SET NULL`, so real rows that point to test rows stay valid.

- [ ] **Step 2: Run test to verify it fails**

Run: `RUN_POSTGRES_INTEGRATION=1 uv run pytest tests/integration/test_publish_metadata_integration.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.warehouse.publish_metadata'`

- [ ] **Step 3: Create `sql/metadata/005_create_mart_publish_runs.sql`**

```sql
CREATE TABLE IF NOT EXISTS mart_publish_runs (
    publish_run_id UUID PRIMARY KEY,
    pipeline_name VARCHAR(128) NOT NULL CHECK (length(pipeline_name) > 0),
    batch_id VARCHAR(192),
    dag_run_id VARCHAR(250),
    dbt_invocation_id VARCHAR(64),
    status VARCHAR(16) NOT NULL
        CHECK (status IN ('BUILDING', 'PUBLISHING', 'PUBLISHED', 'FAILED')),
    error_type VARCHAR(64),
    error_message TEXT,
    previous_publish_run_id UUID
        REFERENCES mart_publish_runs (publish_run_id) ON DELETE SET NULL,
    mart_hashes JSONB,
    mart_row_counts JSONB,
    tests_passed INTEGER NOT NULL DEFAULT 0 CHECK (tests_passed >= 0),
    tests_failed INTEGER NOT NULL DEFAULT 0 CHECK (tests_failed >= 0),
    failed_path TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    CONSTRAINT mart_publish_runs_finished_check CHECK (
        (status IN ('BUILDING', 'PUBLISHING')) = (finished_at IS NULL)
    ),
    CONSTRAINT mart_publish_runs_error_check CHECK (
        (status <> 'FAILED' AND error_type IS NULL AND error_message IS NULL)
        OR (status = 'FAILED' AND error_type IS NOT NULL AND length(error_type) > 0)
    ),
    CONSTRAINT mart_publish_runs_hash_check CHECK (
        status NOT IN ('PUBLISHING', 'PUBLISHED')
        OR (
            mart_hashes IS NOT NULL
            AND jsonb_typeof(mart_hashes) = 'object'
            AND mart_row_counts IS NOT NULL
            AND jsonb_typeof(mart_row_counts) = 'object'
        )
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS mart_publish_runs_single_active_idx
    ON mart_publish_runs ((true))
    WHERE status IN ('BUILDING', 'PUBLISHING');

CREATE INDEX IF NOT EXISTS mart_publish_runs_batch_started_idx
    ON mart_publish_runs (batch_id, started_at);
```

- [ ] **Step 4: Create `src/warehouse/publish_metadata.py`**

```python
"""mart_publish_runs 상태 전이를 PostgreSQL Transaction으로 기록한다."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime

import psycopg
from psycopg.types.json import Jsonb

from src.common.database import PostgresSettings, apply_sql_file
from src.warehouse.errors import PublishInProgressError, PublishStateError

BUILDING = "BUILDING"
PUBLISHING = "PUBLISHING"
PUBLISHED = "PUBLISHED"
FAILED = "FAILED"
ACTIVE_STATUSES = (BUILDING, PUBLISHING)
ACTIVE_PUBLISH_INDEX = "mart_publish_runs_single_active_idx"
ERROR_MESSAGE_LIMIT = 4000

_RECORD_COLUMNS = (
    "publish_run_id, status, started_at, previous_publish_run_id, mart_hashes, error_type"
)


@dataclass(frozen=True)
class PublishRun:
    """새 Publish Run을 시작할 때 필요한 식별 정보를 담는다."""

    publish_run_id: uuid.UUID
    pipeline_name: str
    batch_id: str | None = None
    dag_run_id: str | None = None


@dataclass(frozen=True)
class PublishRecord:
    """저장된 Publish Run 한 건의 상태 요약을 담는다."""

    publish_run_id: uuid.UUID
    status: str
    started_at: datetime
    previous_publish_run_id: uuid.UUID | None
    mart_hashes: dict[str, str] | None
    error_type: str | None


def ensure_publish_metadata(settings: PostgresSettings) -> None:
    """mart_publish_runs Table과 Index를 멱등적으로 준비한다."""
    with settings.pipeline_connection() as connection:
        apply_sql_file(connection, "sql/metadata/005_create_mart_publish_runs.sql")


def start_publish_run(
    settings: PostgresSettings, run: PublishRun, *, now: datetime
) -> PublishRecord:
    """BUILDING Row를 만들고, 다른 활성 Run이 있으면 PublishInProgressError를 낸다."""
    try:
        with settings.pipeline_connection() as connection:
            row = connection.execute(
                f"""
                INSERT INTO mart_publish_runs (
                    publish_run_id, pipeline_name, batch_id, dag_run_id, status,
                    previous_publish_run_id, started_at
                )
                SELECT %s, %s, %s, %s, 'BUILDING', (
                    SELECT publish_run_id FROM mart_publish_runs
                    WHERE status = 'PUBLISHED'
                    ORDER BY finished_at DESC, started_at DESC
                    LIMIT 1
                ), %s
                RETURNING {_RECORD_COLUMNS}
                """,
                (run.publish_run_id, run.pipeline_name, run.batch_id, run.dag_run_id, now),
            ).fetchone()
    except psycopg.errors.UniqueViolation as error:
        if error.diag.constraint_name == ACTIVE_PUBLISH_INDEX:
            raise PublishInProgressError("Another mart publish run is active") from error
        raise
    return _record(row)


def record_dbt_result(
    settings: PostgresSettings,
    publish_run_id: uuid.UUID,
    *,
    invocation_id: str | None,
    tests_passed: int,
    tests_failed: int,
) -> None:
    """BUILDING Run에 dbt Invocation ID와 Test 통과·실패 수를 기록한다."""
    _transition(
        settings,
        publish_run_id,
        (BUILDING,),
        "dbt_invocation_id = %s, tests_passed = %s, tests_failed = %s",
        (invocation_id, tests_passed, tests_failed),
    )


def mark_publishing(
    settings: PostgresSettings,
    publish_run_id: uuid.UUID,
    *,
    mart_hashes: dict[str, str],
    mart_row_counts: dict[str, int],
) -> None:
    """Swap 직전 Hash와 Row 수를 저장하고 BUILDING을 PUBLISHING으로 바꾼다."""
    _transition(
        settings,
        publish_run_id,
        (BUILDING,),
        "status = 'PUBLISHING', mart_hashes = %s, mart_row_counts = %s",
        (Jsonb(mart_hashes), Jsonb(mart_row_counts)),
    )


def mark_published(settings: PostgresSettings, publish_run_id: uuid.UUID, *, now: datetime) -> None:
    """Swap이 끝난 PUBLISHING Run을 PUBLISHED로 종료한다."""
    _transition(
        settings, publish_run_id, (PUBLISHING,), "status = 'PUBLISHED', finished_at = %s", (now,)
    )


def mark_failed(
    settings: PostgresSettings,
    publish_run_id: uuid.UUID,
    *,
    error_type: str,
    error_message: str,
    failed_path: str | None,
    now: datetime,
) -> None:
    """활성 Run을 Error Type·메시지·격리 경로와 함께 FAILED로 종료한다."""
    _transition(
        settings,
        publish_run_id,
        ACTIVE_STATUSES,
        "status = 'FAILED', error_type = %s, error_message = %s, failed_path = %s,"
        " finished_at = %s",
        (error_type, error_message[:ERROR_MESSAGE_LIMIT], failed_path, now),
    )


def get_publish_run(
    settings: PostgresSettings, publish_run_id: uuid.UUID
) -> PublishRecord | None:
    """Publish Run 한 건을 조회하고 없으면 None을 반환한다."""
    with settings.pipeline_connection() as connection:
        row = connection.execute(
            f"SELECT {_RECORD_COLUMNS} FROM mart_publish_runs WHERE publish_run_id = %s",
            (publish_run_id,),
        ).fetchone()
    return None if row is None else _record(row)


def stale_active_runs(
    settings: PostgresSettings, *, older_than: datetime
) -> tuple[PublishRecord, ...]:
    """기준 시각 이전에 시작해 아직 활성 상태인 Run을 반환한다."""
    with settings.pipeline_connection() as connection:
        rows = connection.execute(
            f"""
            SELECT {_RECORD_COLUMNS} FROM mart_publish_runs
            WHERE status IN ('BUILDING', 'PUBLISHING') AND started_at < %s
            ORDER BY started_at
            """,
            (older_than,),
        ).fetchall()
    return tuple(_record(row) for row in rows)


def assert_no_active_publish(settings: PostgresSettings) -> None:
    """활성 Publish Run이 있으면 PublishInProgressError를 낸다."""
    ensure_publish_metadata(settings)
    with settings.pipeline_connection() as connection:
        row = connection.execute(
            "SELECT publish_run_id FROM mart_publish_runs"
            " WHERE status IN ('BUILDING', 'PUBLISHING') LIMIT 1"
        ).fetchone()
    if row is not None:
        raise PublishInProgressError(f"Mart publish run {row[0]} is active")


def _transition(
    settings: PostgresSettings,
    publish_run_id: uuid.UUID,
    expected: tuple[str, ...],
    assignments: str,
    values: tuple[object, ...],
) -> None:
    """기대 상태일 때만 한 Row를 갱신하고, 아니면 PublishStateError를 낸다."""
    with settings.pipeline_connection() as connection:
        cursor = connection.execute(
            f"UPDATE mart_publish_runs SET {assignments}"
            " WHERE publish_run_id = %s AND status = ANY(%s)",
            (*values, publish_run_id, list(expected)),
        )
        if cursor.rowcount != 1:
            raise PublishStateError(
                f"Publish run {publish_run_id} is not in {', '.join(expected)}"
            )


def _record(row: tuple) -> PublishRecord:
    """SELECT 결과 Tuple을 PublishRecord로 바꾼다."""
    return PublishRecord(
        publish_run_id=row[0],
        status=row[1],
        started_at=row[2],
        previous_publish_run_id=row[3],
        mart_hashes=row[4],
        error_type=row[5],
    )
```

`_transition` raises inside the `with` block. psycopg rolls the transaction back on exception, so a failed guard changes nothing.

- [ ] **Step 5: Run tests to verify they pass**

Run: `docker compose up -d --wait postgres && RUN_POSTGRES_INTEGRATION=1 uv run pytest tests/integration/test_publish_metadata_integration.py -v`
Expected: 5 passed.

- [ ] **Step 6: Commit**

```bash
uv run ruff check src/warehouse tests/integration/test_publish_metadata_integration.py
git add sql/metadata/005_create_mart_publish_runs.sql src/warehouse/publish_metadata.py \
  tests/integration/test_publish_metadata_integration.py
git commit  # via /caveman:caveman-commit, e.g. "feat(warehouse): add mart_publish_runs metadata with single-active mutex"
```

---

### Task 3: dbt runner and failure classification

**Files:**
- Create: `src/warehouse/dbt_runner.py`
- Test: `tests/test_dbt_runner.py`

**Interfaces:**
- Consumes: `DBT_BUILD_ERROR`, `DBT_TEST_ERROR` (Task 1); `PROJECT_ROOT`, `environment_values` from `src.common.database`.
- Produces (`src.warehouse.dbt_runner`):
  - `DBT_PROJECT_DIR = PROJECT_ROOT / "dbt"`
  - `@dataclass(frozen=True) DbtRunResult(returncode: int, invocation_id: str | None, tests_passed: int, tests_failed: int, error_type: str | None, failed_nodes: tuple[str, ...], output: str)` with property `succeeded -> bool` (`returncode == 0 and error_type is None`).
  - `run_dbt_build(build_path: Path, target_path: Path, *, dbt_executable: str | None = None, project_dir: Path = DBT_PROJECT_DIR, extra_args: tuple[str, ...] = ()) -> DbtRunResult`
  - `parse_run_results(path: Path, returncode: int, output: str) -> DbtRunResult`
  - `classify_dbt_failure(results: list[dict]) -> str`
  - Callers type the runner as `Callable[[Path, Path], DbtRunResult]`.

Classification rule (spec §dbt failure):
1. Any node whose `unique_id` starts with `model.`, `seed.` or `snapshot.` and whose `status` is `error` → `DBT_BUILD_ERROR`.
2. Else any node starting with `test.` or `unit_test.` whose `status` is `fail` or `error` → `DBT_TEST_ERROR`.
3. Else (including missing `run_results.json` with non-zero exit) → `DBT_BUILD_ERROR`.

- [ ] **Step 1: Write the failing test**

```python
"""dbt run_results.json 해석과 실패 분류를 검증한다."""

from __future__ import annotations

import json
import stat
import sys
from pathlib import Path

import pytest

from src.warehouse.dbt_runner import (
    classify_dbt_failure,
    parse_run_results,
    run_dbt_build,
)
from src.warehouse.errors import DBT_BUILD_ERROR, DBT_TEST_ERROR


def _node(unique_id: str, status: str) -> dict[str, str]:
    """run_results.json의 Result 한 건을 만든다."""
    return {"unique_id": unique_id, "status": status}


def _write_results(path: Path, results: list[dict[str, str]]) -> Path:
    """Invocation ID와 Result 목록을 가진 run_results.json을 쓴다."""
    path.write_text(
        json.dumps({"metadata": {"invocation_id": "inv-1"}, "results": results}),
        encoding="utf-8",
    )
    return path


@pytest.mark.parametrize(
    ("results", "expected"),
    [
        ([_node("model.p.fct_order", "error"), _node("test.p.x", "fail")], DBT_BUILD_ERROR),
        ([_node("seed.p.s", "error")], DBT_BUILD_ERROR),
        ([_node("snapshot.p.s", "error")], DBT_BUILD_ERROR),
        ([_node("model.p.fct_order", "success"), _node("test.p.x", "fail")], DBT_TEST_ERROR),
        ([_node("unit_test.p.u", "error")], DBT_TEST_ERROR),
        ([_node("model.p.fct_order", "skipped")], DBT_BUILD_ERROR),
        ([], DBT_BUILD_ERROR),
    ],
)
def test_classify_dbt_failure(results: list[dict[str, str]], expected: str) -> None:
    """Model 오류가 Test 실패보다 우선하고, 근거가 없으면 Build 오류로 본다."""
    assert classify_dbt_failure(results) == expected


def test_parse_success_counts_tests(tmp_path: Path) -> None:
    """성공 Run은 Error Type 없이 Test 통과·실패 수를 센다."""
    path = _write_results(
        tmp_path / "run_results.json",
        [
            _node("model.p.fct_order", "success"),
            _node("test.p.a", "pass"),
            _node("unit_test.p.b", "pass"),
            _node("test.p.c", "warn"),
        ],
    )
    result = parse_run_results(path, 0, "ok")
    assert result.succeeded
    assert result.invocation_id == "inv-1"
    assert (result.tests_passed, result.tests_failed) == (3, 0)
    assert result.failed_nodes == ()


def test_parse_failure_lists_failed_nodes(tmp_path: Path) -> None:
    """실패 Run은 분류된 Error Type과 실패 Node 목록을 가진다."""
    path = _write_results(
        tmp_path / "run_results.json",
        [_node("model.p.fct_order", "success"), _node("test.p.a", "fail")],
    )
    result = parse_run_results(path, 1, "x" * 10_000)
    assert not result.succeeded
    assert result.error_type == DBT_TEST_ERROR
    assert result.failed_nodes == ("test.p.a",)
    assert result.tests_failed == 1
    assert len(result.output) == 4000


def test_parse_missing_file_with_nonzero_exit_is_build_error(tmp_path: Path) -> None:
    """run_results.json이 없고 Exit Code가 0이 아니면 Build 오류다."""
    result = parse_run_results(tmp_path / "missing.json", 2, "compile error")
    assert result.error_type == DBT_BUILD_ERROR
    assert result.invocation_id is None


def test_parse_nonzero_exit_without_failed_nodes_is_build_error(tmp_path: Path) -> None:
    """모든 Node가 성공이어도 Exit Code가 0이 아니면 성공으로 보지 않는다."""
    path = _write_results(tmp_path / "run_results.json", [_node("test.p.a", "pass")])
    assert parse_run_results(path, 1, "").error_type == DBT_BUILD_ERROR


def test_run_dbt_build_passes_build_path_and_target_path(tmp_path: Path) -> None:
    """Runner는 WAREHOUSE_PATH와 --target-path를 Build 경로로 넘긴다."""
    fake = tmp_path / "fake-dbt"
    fake.write_text(
        f"#!{sys.executable}\n"
        "import json, os, pathlib, sys\n"
        "target = pathlib.Path(sys.argv[sys.argv.index('--target-path') + 1])\n"
        "target.mkdir(parents=True, exist_ok=True)\n"
        "(target / 'run_results.json').write_text(json.dumps({\n"
        "    'metadata': {'invocation_id': os.environ['WAREHOUSE_PATH']},\n"
        "    'results': [{'unique_id': 'test.p.a', 'status': 'pass'}],\n"
        "}))\n",
        encoding="utf-8",
    )
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    build_path = tmp_path / "build" / "run.duckdb"
    result = run_dbt_build(build_path, tmp_path / "target", dbt_executable=str(fake))
    assert result.succeeded
    assert result.invocation_id == str(build_path)
    assert result.tests_passed == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_dbt_runner.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.warehouse.dbt_runner'`

- [ ] **Step 3: Create `src/warehouse/dbt_runner.py`**

```python
"""Build Warehouse 파일에 dbt build를 실행하고 결과를 분류한다."""

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from src.common.database import PROJECT_ROOT, environment_values
from src.warehouse.errors import DBT_BUILD_ERROR, DBT_TEST_ERROR

DBT_PROJECT_DIR = PROJECT_ROOT / "dbt"
OUTPUT_LIMIT = 4000
_BUILD_NODE_PREFIXES = ("model.", "seed.", "snapshot.")
_TEST_NODE_PREFIXES = ("test.", "unit_test.")
_FAILED_STATUSES = frozenset({"error", "fail"})


@dataclass(frozen=True)
class DbtRunResult:
    """dbt build 한 번의 Exit Code, Test 수, 분류된 실패 정보를 담는다."""

    returncode: int
    invocation_id: str | None
    tests_passed: int
    tests_failed: int
    error_type: str | None
    failed_nodes: tuple[str, ...]
    output: str

    @property
    def succeeded(self) -> bool:
        """Exit Code가 0이고 분류된 실패가 없으면 True다."""
        return self.returncode == 0 and self.error_type is None


def run_dbt_build(
    build_path: Path,
    target_path: Path,
    *,
    dbt_executable: str | None = None,
    project_dir: Path = DBT_PROJECT_DIR,
    extra_args: tuple[str, ...] = (),
) -> DbtRunResult:
    """WAREHOUSE_PATH를 Build 파일로 지정해 dbt build를 실행하고 결과를 해석한다."""
    environment = {**environment_values(), "WAREHOUSE_PATH": str(build_path)}
    process = subprocess.run(
        [
            dbt_executable or _default_dbt_executable(),
            "build",
            "--project-dir",
            str(project_dir),
            "--profiles-dir",
            str(project_dir),
            "--target-path",
            str(target_path),
            *extra_args,
        ],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    output = f"{process.stdout}\n{process.stderr}"
    return parse_run_results(target_path / "run_results.json", process.returncode, output)


def parse_run_results(path: Path, returncode: int, output: str) -> DbtRunResult:
    """run_results.json과 Exit Code로 DbtRunResult를 만든다."""
    invocation_id: str | None = None
    results: list[dict] = []
    if path.is_file():
        payload = json.loads(path.read_text(encoding="utf-8"))
        invocation_id = payload.get("metadata", {}).get("invocation_id")
        results = payload.get("results", [])
    tests = [item for item in results if item["unique_id"].startswith(_TEST_NODE_PREFIXES)]
    tests_failed = sum(item["status"] in _FAILED_STATUSES for item in tests)
    failed_nodes = tuple(
        item["unique_id"] for item in results if item["status"] in _FAILED_STATUSES
    )
    failed = returncode != 0 or bool(failed_nodes)
    return DbtRunResult(
        returncode=returncode,
        invocation_id=invocation_id,
        tests_passed=len(tests) - tests_failed,
        tests_failed=tests_failed,
        error_type=classify_dbt_failure(results) if failed else None,
        failed_nodes=failed_nodes,
        output=output[-OUTPUT_LIMIT:],
    )


def classify_dbt_failure(results: list[dict]) -> str:
    """Model·Seed·Snapshot 오류를 Test 실패보다 먼저 보고, 근거가 없으면 Build 오류로 본다."""
    if any(
        item["unique_id"].startswith(_BUILD_NODE_PREFIXES) and item["status"] == "error"
        for item in results
    ):
        return DBT_BUILD_ERROR
    if any(
        item["unique_id"].startswith(_TEST_NODE_PREFIXES) and item["status"] in _FAILED_STATUSES
        for item in results
    ):
        return DBT_TEST_ERROR
    return DBT_BUILD_ERROR


def _default_dbt_executable() -> str:
    """현재 Python 옆의 dbt를 우선 쓰고, 없으면 PATH의 dbt를 쓴다."""
    sibling = Path(sys.executable).with_name("dbt")
    return str(sibling) if sibling.is_file() else "dbt"
```

Note: `warn` counts as passed. dbt returns exit 0 for warnings.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_dbt_runner.py -v`
Expected: 12 passed.

- [ ] **Step 5: Commit**

```bash
uv run ruff check src/warehouse/dbt_runner.py tests/test_dbt_runner.py
git add src/warehouse/dbt_runner.py tests/test_dbt_runner.py
git commit  # via /caveman:caveman-commit, e.g. "feat(warehouse): add dbt runner with build/test failure classification"
```

---

### Task 4: Build-then-swap publish module

**Files:**
- Modify: `src/warehouse/mart_hash.py` (add `mart_row_counts` after `mart_logical_hashes`)
- Create: `src/warehouse/publish.py`
- Test: `tests/test_mart_hash.py` (append), `tests/test_warehouse_publish.py`, `tests/integration/test_warehouse_publish_integration.py`

**Interfaces:**
- Consumes: Task 1 errors; Task 2 `publish_metadata` API; Task 3 `DbtRunResult`, `run_dbt_build`; `sync_bronze_catalog(settings, database_path)` from `src.ingestion.catalog`; `classify_error` from `src.ingestion.errors`; `mart_logical_hashes`, `mismatched_relations`, `target_for`, `MART_HASH_TARGETS`, `MartTarget` from `src.warehouse.mart_hash`.
- Produces (`src.warehouse.mart_hash`): `mart_row_counts(warehouse_path: Path, targets: tuple[MartTarget, ...] = MART_HASH_TARGETS) -> dict[str, int]`.
- Produces (`src.warehouse.publish`):
  - `DEFAULT_WAREHOUSE_ROOT = PROJECT_ROOT / "data" / "warehouse"`, `PUBLISHED_FILE_NAME = "warehouse.duckdb"`, `FAILED_BUILD_RETENTION = 3`, `PUBLISH_STALE_AFTER = timedelta(hours=1)`, `ABANDONED_MESSAGE = "abandoned"`
  - `DbtRunner = Callable[[Path, Path], DbtRunResult]`
  - `@dataclass(frozen=True) WarehousePaths(published: Path, build_dir: Path, failed_dir: Path)` with `under(root) -> WarehousePaths` (classmethod), `published_wal` (property), `build_file(publish_run_id)`, `failed_file(publish_run_id)`, `target_dir(publish_run_id)`
  - `@dataclass(frozen=True) PublishOutcome(publish_run_id: uuid.UUID, previous_publish_run_id: uuid.UUID | None, mart_hashes: dict[str, str], mart_row_counts: dict[str, int], changed_relations: tuple[str, ...])`
  - `recover_incomplete_publishes(settings, paths, *, now: datetime) -> tuple[uuid.UUID, ...]`
  - `prepare_warehouse_build(settings, paths, run: PublishRun, *, now: datetime | None = None) -> Path`
  - `build_warehouse(settings, paths, publish_run_id, *, dbt_runner: DbtRunner = run_dbt_build) -> DbtRunResult`
  - `publish_build(settings, paths, publish_run_id, *, targets=MART_HASH_TARGETS, now: datetime | None = None) -> PublishOutcome`
  - `publish_warehouse(settings, paths, run, *, dbt_runner: DbtRunner = run_dbt_build, targets=MART_HASH_TARGETS) -> PublishOutcome`
  - CLI: `uv run python -m src.warehouse.publish [--warehouse-root PATH] [--pipeline-name NAME] [--dbt-vars YAML] [--recover-only]`. Prints one JSON line. Exit 0 on publish, 1 on `WarehouseBuildError`.

Failure contract:
- `PublishInProgressError` from `start_publish_run` propagates without touching any row or file.
- Any other exception after the BUILDING row exists calls `_fail`. `_fail` moves the build file (and `.wal`) to `failed/`, sets its mtime to `now`, marks the row FAILED with `classify_error(error)`, prunes `failed/` to 3 files, and removes the target dir. Then the exception re-raises.
- dbt failure raises `WarehouseBuildError(result.error_type, summary)` after `_fail`.
- In `publish_build`, `_fail` runs only while the build file still exists. After `os.replace`, a `mark_published` failure leaves the row PUBLISHING. Recovery confirms it after 1 hour by comparing hashes.

- [ ] **Step 1: Write the failing `mart_row_counts` test**

Append to `tests/test_mart_hash.py` and add `MartTarget, mart_row_counts` to its `src.warehouse.mart_hash` import:

```python
def test_mart_row_counts_counts_each_target(tmp_path: Path) -> None:
    """대상 Relation별 Row 수를 Relation 이름 Key로 반환한다."""
    warehouse_path = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(warehouse_path)) as connection:
        connection.execute("CREATE SCHEMA dimensions")
        connection.execute(
            "CREATE TABLE dimensions.dim_date AS SELECT range AS date_key FROM range(3)"
        )
    target = MartTarget("dimensions", "dim_date", ("date_key",))
    assert mart_row_counts(warehouse_path, (target,)) == {"dimensions.dim_date": 3}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/test_mart_hash.py::test_mart_row_counts_counts_each_target -v`
Expected: FAIL with `ImportError: cannot import name 'mart_row_counts'`

- [ ] **Step 3: Implement `mart_row_counts`**

Insert after `mart_logical_hashes` in `src/warehouse/mart_hash.py`:

```python
def mart_row_counts(
    warehouse_path: Path, targets: tuple[MartTarget, ...] = MART_HASH_TARGETS
) -> dict[str, int]:
    """Hash 대상 Mart 전부의 Row 수를 Relation 이름 기준 Dict로 반환한다."""
    with duckdb.connect(str(warehouse_path), read_only=True) as connection:
        return {
            target.relation: connection.execute(
                f"SELECT count(*) FROM {target.relation}"
            ).fetchone()[0]
            for target in targets
        }
```

Run: `uv run pytest tests/test_mart_hash.py -v`
Expected: PASS.

- [ ] **Step 4: Write the failing unit tests for file handling**

Create `tests/test_warehouse_publish.py`:

```python
"""Publish 경로 규칙과 실패 Build 격리·보존 규칙을 검증한다."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

from src.warehouse.publish import (
    FAILED_BUILD_RETENTION,
    WarehousePaths,
    _isolate_build,
    _prune_failed_builds,
)

NOW = datetime(2026, 9, 18, tzinfo=UTC)


def test_paths_under_root_follow_the_spec_layout(tmp_path: Path) -> None:
    """Published·Build·Failed 경로가 Spec의 Directory 구조를 따른다."""
    paths = WarehousePaths.under(tmp_path)
    run_id = uuid.UUID(int=1)
    assert paths.published == tmp_path / "warehouse.duckdb"
    assert paths.published_wal == tmp_path / "warehouse.duckdb.wal"
    assert paths.build_file(run_id) == tmp_path / "build" / f"{run_id}.duckdb"
    assert paths.failed_file(run_id) == tmp_path / "failed" / f"{run_id}.duckdb"
    assert paths.target_dir(run_id) == tmp_path / "build" / f"{run_id}-target"


def test_isolate_build_moves_file_and_wal_and_touches_mtime(tmp_path: Path) -> None:
    """실패 Build와 WAL을 failed/로 옮기고 mtime을 격리 시각으로 맞춘다."""
    paths = WarehousePaths.under(tmp_path)
    run_id = uuid.uuid4()
    paths.build_dir.mkdir()
    paths.build_file(run_id).write_bytes(b"db")
    paths.build_file(run_id).with_name(f"{run_id}.duckdb.wal").write_bytes(b"wal")

    isolated = _isolate_build(paths, run_id, now=NOW)

    assert isolated == paths.failed_file(run_id)
    assert isolated.read_bytes() == b"db"
    assert isolated.with_name(f"{run_id}.duckdb.wal").read_bytes() == b"wal"
    assert not paths.build_file(run_id).exists()
    assert isolated.stat().st_mtime == NOW.timestamp()


def test_isolate_build_returns_none_when_build_is_missing(tmp_path: Path) -> None:
    """Build 파일이 없으면 아무것도 옮기지 않고 None을 반환한다."""
    assert _isolate_build(WarehousePaths.under(tmp_path), uuid.uuid4(), now=NOW) is None


def test_prune_keeps_only_the_newest_failed_builds(tmp_path: Path) -> None:
    """failed/에는 mtime 기준 최신 3개 Build와 그 WAL만 남는다."""
    failed_dir = tmp_path / "failed"
    failed_dir.mkdir()
    for index in range(5):
        build = failed_dir / f"run-{index}.duckdb"
        build.write_bytes(b"db")
        build.with_name(f"run-{index}.duckdb.wal").write_bytes(b"wal")
        os.utime(build, (1_000 + index, 1_000 + index))

    _prune_failed_builds(failed_dir)

    remaining = sorted(path.name for path in failed_dir.glob("*.duckdb"))
    assert FAILED_BUILD_RETENTION == 3
    assert remaining == ["run-2.duckdb", "run-3.duckdb", "run-4.duckdb"]
    assert not (failed_dir / "run-0.duckdb.wal").exists()
```

Run: `uv run pytest tests/test_warehouse_publish.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.warehouse.publish'`

- [ ] **Step 5: Write the failing integration test**

Create `tests/integration/test_warehouse_publish_integration.py`. The fake dbt runner writes `dimensions.dim_date` into the build file, so this test needs only PostgreSQL. `sync_bronze_catalog` reads COMMITTED rows from the real metadata DB. It writes only into the temp build file.

```python
"""Build-then-swap Publish의 성공·실패·복구 시나리오를 실제 PostgreSQL로 검증한다."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from src.common.database import PostgresSettings
from src.ingestion.metadata import ensure_ingestion_metadata
from src.warehouse.dbt_runner import DbtRunResult
from src.warehouse.errors import (
    DBT_TEST_ERROR,
    PublishedWalError,
    PublishInProgressError,
    WarehouseBuildError,
)
from src.warehouse.mart_hash import MartTarget, mart_logical_hashes, mart_row_counts
from src.warehouse.publish import (
    ABANDONED_MESSAGE,
    WarehousePaths,
    prepare_warehouse_build,
    publish_warehouse,
    recover_incomplete_publishes,
)
from src.warehouse.publish_metadata import (
    FAILED,
    PUBLISHED,
    PublishRun,
    assert_no_active_publish,
    ensure_publish_metadata,
    get_publish_run,
    mark_failed,
    mark_publishing,
    start_publish_run,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
        reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
    ),
]

DIM_DATE_TARGET = MartTarget("dimensions", "dim_date", ("date_key",))
TARGETS = (DIM_DATE_TARGET,)


@pytest.fixture
def settings() -> Iterator[PostgresSettings]:
    """활성 Publish가 없을 때만 실행하고, 테스트 Run Row를 정리한다."""
    settings = PostgresSettings.from_environment()
    ensure_ingestion_metadata(settings)
    ensure_publish_metadata(settings)
    try:
        assert_no_active_publish(settings)
    except PublishInProgressError:
        pytest.skip("A real publish run is active; rerun after it finishes.")
    yield settings
    with settings.pipeline_connection() as connection:
        connection.execute(
            "DELETE FROM mart_publish_runs WHERE pipeline_name LIKE 'test_publish_%'"
        )


def _run() -> PublishRun:
    """테스트 전용 Pipeline 이름을 가진 Publish Run을 만든다."""
    return PublishRun(uuid.uuid4(), f"test_publish_{uuid.uuid4().hex[:8]}")


def _dbt_writing(rows: int, returncode: int = 0, error_type: str | None = None):
    """Build 파일에 dim_date를 rows개로 쓰는 가짜 dbt Runner를 만든다."""

    def runner(build_path: Path, target_path: Path) -> DbtRunResult:
        """Build 파일에 Mart Table 하나를 쓰고 지정한 결과를 반환한다."""
        with duckdb.connect(str(build_path)) as connection:
            connection.execute("CREATE SCHEMA IF NOT EXISTS dimensions")
            connection.execute(
                "CREATE OR REPLACE TABLE dimensions.dim_date AS "
                f"SELECT range AS date_key FROM range({rows})"
            )
        failed = () if error_type is None else ("test.fake.gate",)
        return DbtRunResult(
            returncode, "fake-invocation", 1, len(failed), error_type, failed, "fake output"
        )

    return runner


def _published_hash(paths: WarehousePaths) -> dict[str, str]:
    """Published 파일의 dim_date Hash를 계산한다."""
    return mart_logical_hashes(paths.published, TARGETS)


def test_failed_build_never_reaches_the_published_file(settings, tmp_path: Path) -> None:
    """성공 → 실패 → 성공 순서에서 실패 Build는 Published 파일을 바꾸지 않는다."""
    paths = WarehousePaths.under(tmp_path)

    first = publish_warehouse(settings, paths, _run(), dbt_runner=_dbt_writing(3), targets=TARGETS)
    first_hash = _published_hash(paths)
    assert mart_row_counts(paths.published, TARGETS) == {"dimensions.dim_date": 3}

    failing = _run()
    with pytest.raises(WarehouseBuildError) as raised:
        publish_warehouse(
            settings, paths, failing,
            dbt_runner=_dbt_writing(5, 1, DBT_TEST_ERROR), targets=TARGETS,
        )
    assert raised.value.error_type == DBT_TEST_ERROR
    assert _published_hash(paths) == first_hash
    assert paths.failed_file(failing.publish_run_id).is_file()
    assert not paths.build_file(failing.publish_run_id).exists()
    failed_record = get_publish_run(settings, failing.publish_run_id)
    assert failed_record is not None
    assert (failed_record.status, failed_record.error_type) == (FAILED, DBT_TEST_ERROR)

    third = publish_warehouse(settings, paths, _run(), dbt_runner=_dbt_writing(7), targets=TARGETS)
    assert third.previous_publish_run_id == first.publish_run_id
    assert third.changed_relations == ("dimensions.dim_date",)
    assert mart_row_counts(paths.published, TARGETS) == {"dimensions.dim_date": 7}
    assert get_publish_run(settings, third.publish_run_id).status == PUBLISHED


def test_published_wal_stops_the_build_as_configuration_error(settings, tmp_path: Path) -> None:
    """Published 파일에 WAL이 있으면 복사하지 않고 CONFIGURATION_ERROR로 끝낸다."""
    paths = WarehousePaths.under(tmp_path)
    publish_warehouse(settings, paths, _run(), dbt_runner=_dbt_writing(3), targets=TARGETS)
    paths.published_wal.write_bytes(b"pending")
    run = _run()

    with pytest.raises(PublishedWalError):
        prepare_warehouse_build(settings, paths, run)

    record = get_publish_run(settings, run.publish_run_id)
    assert (record.status, record.error_type) == (FAILED, "CONFIGURATION_ERROR")


def test_fresh_active_run_blocks_a_new_build(settings, tmp_path: Path) -> None:
    """1시간이 지나지 않은 활성 Run이 있으면 새 Build를 시작하지 않는다."""
    paths = WarehousePaths.under(tmp_path)
    active = start_publish_run(settings, _run(), now=datetime.now(UTC))
    blocked = _run()
    try:
        with pytest.raises(PublishInProgressError):
            prepare_warehouse_build(settings, paths, blocked)
        assert get_publish_run(settings, blocked.publish_run_id) is None
    finally:
        mark_failed(
            settings, active.publish_run_id, error_type="UNKNOWN_ERROR",
            error_message="cleanup", failed_path=None, now=datetime.now(UTC),
        )


def test_recovery_confirms_a_publishing_run_whose_hash_matches(settings, tmp_path: Path) -> None:
    """Swap 후 기록 전에 멈춘 PUBLISHING Run은 Hash가 같으면 PUBLISHED로 확정한다."""
    paths = WarehousePaths.under(tmp_path)
    publish_warehouse(settings, paths, _run(), dbt_runner=_dbt_writing(3), targets=TARGETS)
    now = datetime.now(UTC)
    stuck = start_publish_run(settings, _run(), now=now - timedelta(hours=2))
    mark_publishing(
        settings, stuck.publish_run_id,
        mart_hashes=_published_hash(paths),
        mart_row_counts=mart_row_counts(paths.published, TARGETS),
    )
    paths.build_file(stuck.publish_run_id).write_bytes(b"leftover")

    recovered = recover_incomplete_publishes(settings, paths, now=now)

    assert recovered == (stuck.publish_run_id,)
    assert get_publish_run(settings, stuck.publish_run_id).status == PUBLISHED
    assert not paths.build_file(stuck.publish_run_id).exists()


def test_recovery_fails_an_abandoned_building_run(settings, tmp_path: Path) -> None:
    """오래된 BUILDING Run은 UNKNOWN_ERROR abandoned로 닫고 Build를 격리한다."""
    paths = WarehousePaths.under(tmp_path)
    now = datetime.now(UTC)
    abandoned = start_publish_run(settings, _run(), now=now - timedelta(hours=2))
    paths.build_dir.mkdir(parents=True)
    paths.build_file(abandoned.publish_run_id).write_bytes(b"partial")

    recovered = recover_incomplete_publishes(settings, paths, now=now)

    assert recovered == (abandoned.publish_run_id,)
    record = get_publish_run(settings, abandoned.publish_run_id)
    assert (record.status, record.error_type) == (FAILED, "UNKNOWN_ERROR")
    assert paths.failed_file(abandoned.publish_run_id).read_bytes() == b"partial"
    with settings.pipeline_connection() as connection:
        message = connection.execute(
            "SELECT error_message FROM mart_publish_runs WHERE publish_run_id = %s",
            (abandoned.publish_run_id,),
        ).fetchone()[0]
    assert message == ABANDONED_MESSAGE
```

Run: `RUN_POSTGRES_INTEGRATION=1 uv run pytest tests/integration/test_warehouse_publish_integration.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.warehouse.publish'`

- [ ] **Step 6: Create `src/warehouse/publish.py`**

```python
"""Build-then-swap 방식으로 검증된 Warehouse 파일만 Published 경로에 교체한다."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path

import duckdb

from src.common.database import PROJECT_ROOT, PostgresSettings
from src.ingestion.catalog import sync_bronze_catalog
from src.ingestion.errors import classify_error
from src.warehouse.dbt_runner import DbtRunResult, run_dbt_build
from src.warehouse.errors import (
    DBT_BUILD_ERROR,
    UNKNOWN_ERROR,
    PublishedWalError,
    PublishStateError,
    WarehouseBuildError,
)
from src.warehouse.mart_hash import (
    MART_HASH_TARGETS,
    MartTarget,
    mart_logical_hashes,
    mart_row_counts,
    mismatched_relations,
    target_for,
)
from src.warehouse.publish_metadata import (
    PUBLISHING,
    PublishRecord,
    PublishRun,
    ensure_publish_metadata,
    get_publish_run,
    mark_failed,
    mark_published,
    mark_publishing,
    record_dbt_result,
    stale_active_runs,
    start_publish_run,
)

DEFAULT_WAREHOUSE_ROOT = PROJECT_ROOT / "data" / "warehouse"
PUBLISHED_FILE_NAME = "warehouse.duckdb"
FAILED_BUILD_RETENTION = 3
PUBLISH_STALE_AFTER = timedelta(hours=1)
ABANDONED_MESSAGE = "abandoned"
FAILED_NODE_LIMIT = 20

DbtRunner = Callable[[Path, Path], DbtRunResult]


@dataclass(frozen=True)
class WarehousePaths:
    """Published·Build·Failed Warehouse 파일 위치를 한곳에서 정한다."""

    published: Path
    build_dir: Path
    failed_dir: Path

    @classmethod
    def under(cls, root: Path) -> WarehousePaths:
        """Warehouse Root 아래 Spec 기본 Directory 구조를 만든다."""
        return cls(root / PUBLISHED_FILE_NAME, root / "build", root / "failed")

    @property
    def published_wal(self) -> Path:
        """Published 파일의 WAL 경로를 반환한다."""
        return _wal_path(self.published)

    def build_file(self, publish_run_id: uuid.UUID) -> Path:
        """Publish Run의 Build 파일 경로를 반환한다."""
        return self.build_dir / f"{publish_run_id}.duckdb"

    def failed_file(self, publish_run_id: uuid.UUID) -> Path:
        """Publish Run의 실패 Build 격리 경로를 반환한다."""
        return self.failed_dir / f"{publish_run_id}.duckdb"

    def target_dir(self, publish_run_id: uuid.UUID) -> Path:
        """Publish Run 전용 dbt --target-path를 반환한다."""
        return self.build_dir / f"{publish_run_id}-target"


@dataclass(frozen=True)
class PublishOutcome:
    """Publish 성공 결과와 직전 Publish 대비 바뀐 Relation을 담는다."""

    publish_run_id: uuid.UUID
    previous_publish_run_id: uuid.UUID | None
    mart_hashes: dict[str, str]
    mart_row_counts: dict[str, int]
    changed_relations: tuple[str, ...]


def recover_incomplete_publishes(
    settings: PostgresSettings, paths: WarehousePaths, *, now: datetime
) -> tuple[uuid.UUID, ...]:
    """1시간 넘게 활성 상태인 Run을 PUBLISHED 확정 또는 abandoned FAILED로 닫는다."""
    recovered: list[uuid.UUID] = []
    for record in stale_active_runs(settings, older_than=now - PUBLISH_STALE_AFTER):
        if record.status == PUBLISHING and _published_matches(paths.published, record.mart_hashes):
            mark_published(settings, record.publish_run_id, now=now)
            paths.build_file(record.publish_run_id).unlink(missing_ok=True)
            shutil.rmtree(paths.target_dir(record.publish_run_id), ignore_errors=True)
        else:
            _fail(settings, paths, record.publish_run_id, UNKNOWN_ERROR, ABANDONED_MESSAGE, now)
        recovered.append(record.publish_run_id)
    return tuple(recovered)


def prepare_warehouse_build(
    settings: PostgresSettings,
    paths: WarehousePaths,
    run: PublishRun,
    *,
    now: datetime | None = None,
) -> Path:
    """활성 Run을 잡고 Published 파일을 복사한 Build 파일에 Catalog를 동기화한다."""
    now = now or datetime.now(UTC)
    ensure_publish_metadata(settings)
    recover_incomplete_publishes(settings, paths, now=now)
    start_publish_run(settings, run, now=now)
    build_path = paths.build_file(run.publish_run_id)
    try:
        if paths.published_wal.exists():
            raise PublishedWalError(f"Published warehouse has a WAL: {paths.published_wal}")
        paths.build_dir.mkdir(parents=True, exist_ok=True)
        if paths.published.is_file():
            shutil.copyfile(paths.published, build_path)
        sync_bronze_catalog(settings, build_path)
    except Exception as error:
        _fail(settings, paths, run.publish_run_id, classify_error(error), str(error))
        raise
    return build_path


def build_warehouse(
    settings: PostgresSettings,
    paths: WarehousePaths,
    publish_run_id: uuid.UUID,
    *,
    dbt_runner: DbtRunner = run_dbt_build,
) -> DbtRunResult:
    """Build 파일에 dbt build를 실행하고 실패하면 격리 후 WarehouseBuildError를 낸다."""
    try:
        result = dbt_runner(paths.build_file(publish_run_id), paths.target_dir(publish_run_id))
        record_dbt_result(
            settings,
            publish_run_id,
            invocation_id=result.invocation_id,
            tests_passed=result.tests_passed,
            tests_failed=result.tests_failed,
        )
    except Exception as error:
        _fail(settings, paths, publish_run_id, classify_error(error), str(error))
        raise
    if not result.succeeded:
        error_type = result.error_type or DBT_BUILD_ERROR
        summary = _failure_summary(error_type, result)
        _fail(settings, paths, publish_run_id, error_type, summary)
        raise WarehouseBuildError(error_type, summary)
    return result


def publish_build(
    settings: PostgresSettings,
    paths: WarehousePaths,
    publish_run_id: uuid.UUID,
    *,
    targets: tuple[MartTarget, ...] = MART_HASH_TARGETS,
    now: datetime | None = None,
) -> PublishOutcome:
    """검증된 Build 파일을 CHECKPOINT·Hash 기록 후 Published 경로로 원자 교체한다."""
    record = get_publish_run(settings, publish_run_id)
    if record is None:
        raise PublishStateError(f"Publish run {publish_run_id} does not exist")
    build_path = paths.build_file(publish_run_id)
    try:
        _checkpoint(build_path)
        if _wal_path(build_path).exists():
            raise PublishedWalError(f"Build warehouse still has a WAL: {build_path}")
        hashes = mart_logical_hashes(build_path, targets)
        counts = mart_row_counts(build_path, targets)
        mark_publishing(settings, publish_run_id, mart_hashes=hashes, mart_row_counts=counts)
        os.replace(build_path, paths.published)
        _fsync_directory(paths.published.parent)
        mark_published(settings, publish_run_id, now=now or datetime.now(UTC))
    except Exception as error:
        if build_path.exists():
            _fail(settings, paths, publish_run_id, classify_error(error), str(error))
        raise
    shutil.rmtree(paths.target_dir(publish_run_id), ignore_errors=True)
    return PublishOutcome(
        publish_run_id=publish_run_id,
        previous_publish_run_id=record.previous_publish_run_id,
        mart_hashes=hashes,
        mart_row_counts=counts,
        changed_relations=mismatched_relations(_previous_hashes(settings, record), hashes),
    )


def publish_warehouse(
    settings: PostgresSettings,
    paths: WarehousePaths,
    run: PublishRun,
    *,
    dbt_runner: DbtRunner = run_dbt_build,
    targets: tuple[MartTarget, ...] = MART_HASH_TARGETS,
) -> PublishOutcome:
    """준비·dbt build·Publish를 한 Process에서 순서대로 실행한다."""
    prepare_warehouse_build(settings, paths, run)
    build_warehouse(settings, paths, run.publish_run_id, dbt_runner=dbt_runner)
    return publish_build(settings, paths, run.publish_run_id, targets=targets)


def _previous_hashes(settings: PostgresSettings, record: PublishRecord) -> dict[str, str]:
    """직전 PUBLISHED Run의 Hash를 반환하고, 없으면 빈 Dict를 반환한다."""
    if record.previous_publish_run_id is None:
        return {}
    previous = get_publish_run(settings, record.previous_publish_run_id)
    return {} if previous is None else dict(previous.mart_hashes or {})


def _published_matches(published: Path, recorded: dict[str, str] | None) -> bool:
    """Published 파일 Hash가 기록된 Hash와 같으면 True다."""
    if not recorded or not published.is_file():
        return False
    try:
        targets = tuple(target_for(relation) for relation in recorded)
        return mart_logical_hashes(published, targets) == recorded
    except (KeyError, duckdb.Error):
        return False


def _fail(
    settings: PostgresSettings,
    paths: WarehousePaths,
    publish_run_id: uuid.UUID,
    error_type: str,
    message: str,
    now: datetime | None = None,
) -> None:
    """Build를 격리하고 Run을 FAILED로 기록한 뒤 오래된 실패 Build를 정리한다."""
    now = now or datetime.now(UTC)
    failed_path = _isolate_build(paths, publish_run_id, now=now)
    mark_failed(
        settings,
        publish_run_id,
        error_type=error_type,
        error_message=message or error_type,
        failed_path=None if failed_path is None else str(failed_path),
        now=now,
    )
    _prune_failed_builds(paths.failed_dir)
    shutil.rmtree(paths.target_dir(publish_run_id), ignore_errors=True)


def _isolate_build(
    paths: WarehousePaths, publish_run_id: uuid.UUID, *, now: datetime
) -> Path | None:
    """Build 파일과 WAL을 failed/로 옮기고 mtime을 격리 시각으로 맞춘다."""
    source = paths.build_file(publish_run_id)
    if not source.exists():
        return None
    paths.failed_dir.mkdir(parents=True, exist_ok=True)
    target = paths.failed_file(publish_run_id)
    os.replace(source, target)
    if _wal_path(source).exists():
        os.replace(_wal_path(source), _wal_path(target))
    os.utime(target, (now.timestamp(), now.timestamp()))
    return target


def _prune_failed_builds(failed_dir: Path, keep: int = FAILED_BUILD_RETENTION) -> None:
    """mtime 기준 최신 keep개만 남기고 오래된 실패 Build와 WAL을 지운다."""
    if not failed_dir.is_dir():
        return
    builds = sorted(
        failed_dir.glob("*.duckdb"), key=lambda path: path.stat().st_mtime, reverse=True
    )
    for stale in builds[keep:]:
        stale.unlink(missing_ok=True)
        _wal_path(stale).unlink(missing_ok=True)


def _checkpoint(warehouse_path: Path) -> None:
    """WAL 내용을 본 파일에 반영하고 연결을 닫는다."""
    with duckdb.connect(str(warehouse_path)) as connection:
        connection.execute("CHECKPOINT")


def _fsync_directory(directory: Path) -> None:
    """Rename 결과가 Disk에 남도록 Directory를 fsync한다."""
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _wal_path(warehouse_path: Path) -> Path:
    """DuckDB 파일의 WAL 경로를 반환한다."""
    return warehouse_path.with_name(f"{warehouse_path.name}.wal")


def _failure_summary(error_type: str, result: DbtRunResult) -> str:
    """실패 Node 목록과 dbt 출력 끝부분으로 오류 메시지를 만든다."""
    nodes = ", ".join(result.failed_nodes[:FAILED_NODE_LIMIT]) or "none"
    return f"{error_type}: failed nodes: {nodes}\n{result.output}"


def main(argv: list[str] | None = None) -> int:
    """Publish 또는 복구를 실행하고 결과를 JSON 한 줄로 출력한다."""
    parser = argparse.ArgumentParser(description="Build, test and publish the mart warehouse.")
    parser.add_argument("--warehouse-root", type=Path, default=DEFAULT_WAREHOUSE_ROOT)
    parser.add_argument("--pipeline-name", default="manual_publish")
    parser.add_argument("--dbt-vars", help="YAML/JSON string passed to dbt --vars")
    parser.add_argument("--recover-only", action="store_true")
    args = parser.parse_args(argv)

    settings = PostgresSettings.from_environment()
    paths = WarehousePaths.under(args.warehouse_root)
    if args.recover_only:
        ensure_publish_metadata(settings)
        recovered = recover_incomplete_publishes(settings, paths, now=datetime.now(UTC))
        print(json.dumps({"recovered": [str(run_id) for run_id in recovered]}))
        return 0

    runner: DbtRunner = run_dbt_build
    if args.dbt_vars is not None:
        runner = partial(run_dbt_build, extra_args=("--vars", args.dbt_vars))
    run = PublishRun(uuid.uuid4(), args.pipeline_name)
    try:
        outcome = publish_warehouse(settings, paths, run, dbt_runner=runner)
    except WarehouseBuildError as error:
        print(
            json.dumps(
                {
                    "publish_run_id": str(run.publish_run_id),
                    "status": "FAILED",
                    "error_type": error.error_type,
                }
            )
        )
        return 1
    print(
        json.dumps(
            {
                "publish_run_id": str(outcome.publish_run_id),
                "status": "PUBLISHED",
                "previous_publish_run_id": (
                    None
                    if outcome.previous_publish_run_id is None
                    else str(outcome.previous_publish_run_id)
                ),
                "changed_relations": list(outcome.changed_relations),
                "mart_row_counts": outcome.mart_row_counts,
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

Note: if `mart_logical_hash` rejects a relation that does not exist yet (first publish before dbt creates it), the exception goes through `_fail` as `UNKNOWN_ERROR`. That is correct: a successful dbt build always creates every mart in `MART_HASH_TARGETS`.

- [ ] **Step 7: Run all Task 4 tests**

Run: `uv run pytest tests/test_mart_hash.py tests/test_warehouse_publish.py -v`
Expected: PASS.

Run: `RUN_POSTGRES_INTEGRATION=1 uv run pytest tests/integration/test_warehouse_publish_integration.py tests/integration/test_publish_metadata_integration.py -v`
Expected: 10 passed.

- [ ] **Step 8: Commit**

```bash
uv run ruff check src/warehouse tests/test_mart_hash.py tests/test_warehouse_publish.py \
  tests/integration/test_warehouse_publish_integration.py
git add src/warehouse/mart_hash.py src/warehouse/publish.py tests/test_mart_hash.py \
  tests/test_warehouse_publish.py tests/integration/test_warehouse_publish_integration.py
git commit  # via /caveman:caveman-commit, e.g. "feat(warehouse): publish mart via build-then-swap with recovery"
```

---

### Task 5: Airflow DAG publish tasks and rebaseline guard

**Files:**
- Modify: `airflow/dags/warehouse_pipeline_dag.py` (imports, constants, tasks after `verify_bronze_commit_task`, wiring at the bottom)
- Modify: `src/rebaseline.py:125-147` (`run_rebaseline`)
- Test: `tests/test_airflow_dags.py:267-280` (replace `test_warehouse_pipeline_defines_the_dbt_build_boundary`), `tests/test_rebaseline.py` (append)

**Interfaces:**
- Consumes: Task 4 `WarehousePaths`, `DEFAULT_WAREHOUSE_ROOT`, `prepare_warehouse_build`, `build_warehouse`, `publish_build`; Task 2 `PublishRun`, `get_publish_run`, `assert_no_active_publish`; Task 1 `PublishInProgressError`.
- Produces: DAG task ids `prepare_warehouse_build`, `dbt_build`, `publish_mart`. XCom from `prepare_warehouse_build`: `{"publish_run_id": str, "build_path": str}`. XCom from `publish_mart`: `{"publish_run_id": str, "changed_relations": list[str]}`.

Retry policy:
- `prepare_warehouse_build` keeps the DAG default (`retries=2`). `PublishInProgressError` is LEASE_UNAVAILABLE, so Airflow retries while another publish runs. Each attempt creates a new `publish_run_id`.
- `dbt_build` and `publish_mart` use `retries=0`. After a failure their run is already FAILED and the build file is isolated, so a retry cannot succeed.

- [ ] **Step 1: Replace the DAG source test**

Replace `test_warehouse_pipeline_defines_the_dbt_build_boundary` in `tests/test_airflow_dags.py` with:

```python
def test_warehouse_pipeline_publishes_through_build_then_swap() -> None:
    """Warehouse DAG는 검증 뒤 Build 준비 → dbt build → Publish 순서로만 Mart를 교체한다."""
    dag_source = (PROJECT_ROOT / "airflow/dags/warehouse_pipeline_dag.py").read_text(
        encoding="utf-8"
    )
    compile(dag_source, "warehouse_pipeline_dag.py", "exec")

    for task_id in ("prepare_warehouse_build", "dbt_build", "publish_mart"):
        pattern = rf'@task\(\s*task_id="{task_id}",\s*trigger_rule="all_success"'
        assert re.search(pattern, dag_source)
    for task_id in ("dbt_build", "publish_mart"):
        pattern = rf'task_id="{task_id}",\s*trigger_rule="all_success",\s*retries=0'
        assert re.search(pattern, dag_source)
    assert "extract_results >> verification >> build >> dbt_result >> publish" in dag_source
    assert "publish_run_summary(run_info, verification, build, publish)" in dag_source
    assert "sync_bronze_catalog" not in dag_source
    assert "subprocess" not in dag_source
    assert "CATALOG_PATH" not in dag_source
```

Run: `uv run pytest tests/test_airflow_dags.py::test_warehouse_pipeline_publishes_through_build_then_swap -v`
Expected: FAIL on the first `task_id` assertion.

- [ ] **Step 2: Update DAG imports and constants**

In `airflow/dags/warehouse_pipeline_dag.py`:
- Delete `import subprocess`.
- Change `from airflow.sdk import DAG, task` to `from airflow.sdk import DAG, get_current_context, task`.
- Delete `from src.ingestion.catalog import sync_bronze_catalog`.
- Delete the `CATALOG_PATH` and `DBT_PROJECT_DIR` lines.
- Add these imports after `from src.ingestion.verification import verify_bronze_commit`:

```python
from src.warehouse.publish import (
    DEFAULT_WAREHOUSE_ROOT,
    WarehousePaths,
    build_warehouse,
    prepare_warehouse_build,
    publish_build,
)
from src.warehouse.publish_metadata import PublishRun, get_publish_run
```

- Add below `LOCAL_DIRECTORY = ...`:

```python
WAREHOUSE_PATHS = WarehousePaths.under(DEFAULT_WAREHOUSE_ROOT)
```

- [ ] **Step 3: Replace the catalog, dbt and summary tasks**

Delete `sync_bronze_catalog_task`, `dbt_build_task` and `publish_run_summary`. Put these in their place, directly after `verify_bronze_commit_task`:

```python
    @task(task_id="prepare_warehouse_build", trigger_rule="all_success")
    def prepare_warehouse_build_task(run_info: dict, verification: dict) -> dict:
        """검증 뒤 Published 파일을 복사한 Build 파일을 만들고 Catalog를 동기화한다."""
        settings = PostgresSettings.from_environment()
        run = PublishRun(
            publish_run_id=uuid.uuid4(),
            pipeline_name=dag.dag_id,
            batch_id=run_info["batch_id"],
            dag_run_id=get_current_context()["run_id"],
        )
        try:
            build_path = prepare_warehouse_build(settings, WAREHOUSE_PATHS, run)
        except Exception as error:
            _reraise_classified(error)
            raise
        return {"publish_run_id": str(run.publish_run_id), "build_path": str(build_path)}

    @task(task_id="dbt_build", trigger_rule="all_success", retries=0)
    def dbt_build_task(build: dict) -> dict:
        """Build 파일에만 dbt build를 실행한다. 실패 Build는 격리되고 Published 파일은 그대로다."""
        settings = PostgresSettings.from_environment()
        try:
            result = build_warehouse(
                settings, WAREHOUSE_PATHS, uuid.UUID(build["publish_run_id"])
            )
        except Exception as error:
            _reraise_classified(error)
            raise
        return {"tests_passed": result.tests_passed, "tests_failed": result.tests_failed}

    @task(task_id="publish_mart", trigger_rule="all_success", retries=0)
    def publish_mart_task(build: dict, dbt_result: dict) -> dict:
        """dbt build가 성공한 Build 파일을 Published 경로로 원자 교체한다."""
        settings = PostgresSettings.from_environment()
        try:
            outcome = publish_build(settings, WAREHOUSE_PATHS, uuid.UUID(build["publish_run_id"]))
        except Exception as error:
            _reraise_classified(error)
            raise
        return {
            "publish_run_id": str(outcome.publish_run_id),
            "changed_relations": list(outcome.changed_relations),
        }

    @task(trigger_rule="all_done")
    def publish_run_summary(
        run_info: dict, verification: dict | None, build: dict | None, publish: dict | None
    ) -> dict:
        """DagRun의 Bronze 검증과 Mart Publish 결과를 작은 JSON Summary로 남긴다."""
        publish_status = "SKIPPED"
        publish_error_type = None
        if build:
            record = get_publish_run(
                PostgresSettings.from_environment(), uuid.UUID(build["publish_run_id"])
            )
            publish_status = record.status if record else "UNKNOWN"
            publish_error_type = record.error_type if record else None
        summary = {
            "batch_id": run_info["batch_id"],
            "verified_table_count": verification["verified_table_count"] if verification else 0,
            "publish_run_id": build["publish_run_id"] if build else None,
            "publish_status": publish_status,
            "publish_error_type": publish_error_type,
            "changed_relations": publish["changed_relations"] if publish else [],
        }
        print(summary)
        return summary
```

Replace the wiring block at the bottom (from `catalog = sync_bronze_catalog_task(verification)` to the end) with:

```python
    build = prepare_warehouse_build_task(run_info, verification)
    dbt_result = dbt_build_task(build)
    publish = publish_mart_task(build, dbt_result)

    extract_results >> release_task
    extract_results >> verification >> build >> dbt_result >> publish
    publish_run_summary(run_info, verification, build, publish)
```

- [ ] **Step 4: Run the DAG test**

Run: `uv run pytest tests/test_airflow_dags.py -v`
Expected: PASS (Compose smoke tests stay skipped).

If Docker is available, also run the Airflow parse smoke test, because `get_current_context` must import inside the Airflow image:
Run: `RUN_AIRFLOW_SMOKE_TEST=1 uv run pytest -m airflow tests/test_airflow_dags.py -v`
Expected: PASS. Then run `docker compose --profile airflow down` if the test left services running.

- [ ] **Step 5: Write the failing rebaseline guard test**

Append to `tests/test_rebaseline.py` (add imports `from pathlib import Path`, `import pytest`, `from src.rebaseline import run_rebaseline`, `from src.warehouse.errors import PublishInProgressError`):

```python
def test_rebaseline_refuses_while_a_publish_is_active(monkeypatch, tmp_path: Path) -> None:
    """활성 Publish가 있으면 Lease를 잡거나 상태를 지우기 전에 재기준화를 거부한다."""
    calls: list[str] = []

    def active_publish(settings) -> None:
        """활성 Publish가 있는 상황을 흉내 낸다."""
        raise PublishInProgressError("active publish")

    monkeypatch.setattr("src.rebaseline.inspect_rebaseline", lambda *args: "inventory")
    monkeypatch.setattr("src.rebaseline.assert_no_active_publish", active_publish)
    monkeypatch.setattr(
        "src.rebaseline.acquire_source_mutation_lease",
        lambda *args, **kwargs: calls.append("lease"),
    )

    with pytest.raises(PublishInProgressError):
        run_rebaseline(
            input_dir=tmp_path,
            seeded_at=datetime(2026, 9, 3, tzinfo=UTC),
            catalog_path=tmp_path / "warehouse.duckdb",
            postgres=None,  # type: ignore[arg-type]
            storage=None,  # type: ignore[arg-type]
        )
    assert calls == []
```

Run: `uv run pytest tests/test_rebaseline.py -v`
Expected: FAIL with `AttributeError: <module 'src.rebaseline'> has no attribute 'assert_no_active_publish'`

- [ ] **Step 6: Add the guard to `run_rebaseline`**

Add the import in `src/rebaseline.py`:

```python
from src.warehouse.publish_metadata import assert_no_active_publish
```

Insert one line right after `inventory = inspect_rebaseline(postgres, storage, catalog_path)`:

```python
    assert_no_active_publish(postgres)
```

Do not add `mart_publish_runs` to `PIPELINE_METADATA_TABLES`. Publish history must survive a rebaseline.

- [ ] **Step 7: Run tests to verify they pass**

Run: `uv run pytest tests/test_rebaseline.py tests/test_airflow_dags.py -v && uv run pytest -q`
Expected: PASS. The default suite has no failures.

- [ ] **Step 8: Commit**

```bash
uv run ruff check airflow/dags/warehouse_pipeline_dag.py src/rebaseline.py tests/test_airflow_dags.py tests/test_rebaseline.py
git add airflow/dags/warehouse_pipeline_dag.py src/rebaseline.py tests/test_airflow_dags.py tests/test_rebaseline.py
git commit  # via /caveman:caveman-commit, e.g. "feat(dag): gate mart publish behind build-then-swap tasks"
```

---

### Task 6: Validation rule registry, duplicate corruption, reject-rate boundaries

**Files:**
- Create: `src/ingestion/rules.py`
- Modify: `src/ingestion/validation.py` (`_validate_record`, `_schema_and_type_errors`, `_domain_and_numeric_errors`)
- Modify: `src/ingestion/corruption.py`
- Modify: `src/ingestion/service.py:412-423` (corruption in the page loop)
- Test: `tests/ingestion/test_rules.py` (create), `tests/ingestion/test_corruption.py` (append), `tests/ingestion/test_quarantine.py` (replace the reject-rate test)

**Interfaces:**
- Produces (`src.ingestion.rules`): `ROW = "ROW"`, `BATCH = "BATCH"`, `@dataclass(frozen=True) ValidationRule(code: str, order: int, scope: str)`, rule constants `SCHEMA_MISMATCH`, `REQUIRED_NULL`, `TYPE_MISMATCH`, `KEY_NULL`, `BATCH_DUPLICATE`, `STATUS_DOMAIN_INVALID`, `NUMERIC_RANGE_INVALID`, `BROKEN_REFERENCE`, `CURSOR_OUT_OF_RANGE`, `VALIDATION_RULES: tuple[ValidationRule, ...]` (PRD §11 order), `ROW_ERROR_CODES: frozenset[str]`.
- Produces (`src.ingestion.corruption`): `DUPLICATE_PRIMARY_KEY = "DUPLICATE_PRIMARY_KEY"` (in `SUPPORTED_CORRUPTION_KINDS`), `CorruptionPlan.apply_page(records: tuple[SourceRecord, ...], first_ordinal: int) -> tuple[SourceRecord, ...]`.
- Behaviour kept: row error codes and their order stay exactly the same. Batch errors still raise `SourceContractError` with the same messages.

- [ ] **Step 1: Write the failing rule tests**

Create `tests/ingestion/test_rules.py`:

```python
"""검증 Rule Registry가 PRD §11 순서와 validation.py 사용 Code를 따르는지 검증한다."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MappingProxyType

from src.ingestion import rules
from src.ingestion.extract import SourcePage, SourceRecord
from src.ingestion.metadata import CursorPosition
from src.ingestion.tables import ORDERS_TABLE
from src.ingestion.validation import ValidationPipeline

VALIDATION_SOURCE = Path(__file__).resolve().parents[2] / "src/ingestion/validation.py"


def test_registry_follows_prd_section_11_order() -> None:
    """Registry 순서와 order 값이 PRD §11 검증 순서와 같다."""
    assert [rule.code for rule in rules.VALIDATION_RULES] == [
        "SCHEMA_MISMATCH",
        "REQUIRED_NULL",
        "TYPE_MISMATCH",
        "KEY_NULL",
        "BATCH_DUPLICATE",
        "STATUS_DOMAIN_INVALID",
        "NUMERIC_RANGE_INVALID",
        "BROKEN_REFERENCE",
        "CURSOR_OUT_OF_RANGE",
    ]
    assert [rule.order for rule in rules.VALIDATION_RULES] == list(range(1, 10))


def test_batch_rules_are_schema_and_cursor_range_only() -> None:
    """Batch 오류는 Schema 불일치와 Cursor 범위 이탈 두 가지뿐이다."""
    batch_codes = {rule.code for rule in rules.VALIDATION_RULES if rule.scope == rules.BATCH}
    assert batch_codes == {"SCHEMA_MISMATCH", "CURSOR_OUT_OF_RANGE"}
    assert rules.ROW_ERROR_CODES == {
        rule.code for rule in rules.VALIDATION_RULES if rule.scope == rules.ROW
    }


def test_validation_module_has_no_error_code_literals() -> None:
    """validation.py는 오류 Code 문자열을 직접 쓰지 않고 Registry만 참조한다."""
    source = VALIDATION_SOURCE.read_text(encoding="utf-8")
    assert re.findall(r'"[A-Z][A-Z_]{3,}"', source) == []


def test_combined_row_errors_follow_registry_order() -> None:
    """한 Record의 여러 오류 Code는 Registry order 순서로 나온다."""
    timestamp = datetime(2026, 9, 7, tzinfo=UTC)
    values = {
        "order_id": None,
        "customer_id": "customer-0001",
        "order_status": "__invalid__",
        "order_purchase_timestamp": timestamp,
        "order_approved_at": None,
        "order_delivered_carrier_date": None,
        "order_delivered_customer_date": None,
        "order_estimated_delivery_date": timestamp + timedelta(days=7),
        "created_at": timestamp,
        "updated_at": timestamp,
    }
    record = SourceRecord(
        ORDERS_TABLE,
        MappingProxyType(values),
        cursor_override=CursorPosition(timestamp, ("order-0000",)),
    )
    lower = CursorPosition(timestamp - timedelta(seconds=1), ("order-0000",))
    page = SourcePage(records=(record,), lower_bound=lower, extract_upper_bound=record.cursor)

    result = ValidationPipeline(ORDERS_TABLE, lower, record.cursor).validate_page(page)

    codes = result.rejected_records[0].error_codes
    assert codes == ("REQUIRED_NULL", "KEY_NULL", "STATUS_DOMAIN_INVALID")
    order = {rule.code: rule.order for rule in rules.VALIDATION_RULES}
    assert [order[code] for code in codes] == sorted(order[code] for code in codes)
```

Run: `uv run pytest tests/ingestion/test_rules.py -v`
Expected: FAIL with `ImportError: cannot import name 'rules' from 'src.ingestion'`

- [ ] **Step 2: Create `src/ingestion/rules.py`**

```python
"""PRD §11 수집 검증 Rule의 Code·순서·범위를 한곳에 정의한다."""

from __future__ import annotations

from dataclasses import dataclass

ROW = "ROW"
BATCH = "BATCH"


@dataclass(frozen=True)
class ValidationRule:
    """검증 Rule 하나의 오류 Code, 검증 순서, Row·Batch 범위를 담는다."""

    code: str
    order: int
    scope: str


SCHEMA_MISMATCH = ValidationRule("SCHEMA_MISMATCH", 1, BATCH)
REQUIRED_NULL = ValidationRule("REQUIRED_NULL", 2, ROW)
TYPE_MISMATCH = ValidationRule("TYPE_MISMATCH", 3, ROW)
KEY_NULL = ValidationRule("KEY_NULL", 4, ROW)
BATCH_DUPLICATE = ValidationRule("BATCH_DUPLICATE", 5, ROW)
STATUS_DOMAIN_INVALID = ValidationRule("STATUS_DOMAIN_INVALID", 6, ROW)
NUMERIC_RANGE_INVALID = ValidationRule("NUMERIC_RANGE_INVALID", 7, ROW)
BROKEN_REFERENCE = ValidationRule("BROKEN_REFERENCE", 8, ROW)
CURSOR_OUT_OF_RANGE = ValidationRule("CURSOR_OUT_OF_RANGE", 9, BATCH)

VALIDATION_RULES: tuple[ValidationRule, ...] = (
    SCHEMA_MISMATCH,
    REQUIRED_NULL,
    TYPE_MISMATCH,
    KEY_NULL,
    BATCH_DUPLICATE,
    STATUS_DOMAIN_INVALID,
    NUMERIC_RANGE_INVALID,
    BROKEN_REFERENCE,
    CURSOR_OUT_OF_RANGE,
)
ROW_ERROR_CODES = frozenset(rule.code for rule in VALIDATION_RULES if rule.scope == ROW)
```

- [ ] **Step 3: Point `validation.py` at the registry**

Add `from src.ingestion import rules` to the imports. Replace every error-code literal. Keep all logic, order and messages unchanged:

| Location | Old | New |
|---|---|---|
| `_validate_record` | `if "SCHEMA_MISMATCH" in errors:` | `if rules.SCHEMA_MISMATCH.code in errors:` |
| `_validate_record` | `errors.append("KEY_NULL")` | `errors.append(rules.KEY_NULL.code)` |
| `_validate_record` | `errors.append("BATCH_DUPLICATE")` | `errors.append(rules.BATCH_DUPLICATE.code)` |
| `_validate_record` | `errors.append("BROKEN_REFERENCE")` | `errors.append(rules.BROKEN_REFERENCE.code)` |
| `_schema_and_type_errors` | `return ["SCHEMA_MISMATCH"]` | `return [rules.SCHEMA_MISMATCH.code]` |
| `_schema_and_type_errors` | `errors.append("REQUIRED_NULL")` | `errors.append(rules.REQUIRED_NULL.code)` |
| `_schema_and_type_errors` | `errors.append("TYPE_MISMATCH")` | `errors.append(rules.TYPE_MISMATCH.code)` |
| `_domain_and_numeric_errors` | `errors.append("STATUS_DOMAIN_INVALID")` | `errors.append(rules.STATUS_DOMAIN_INVALID.code)` |
| `_domain_and_numeric_errors` | `errors.append("NUMERIC_RANGE_INVALID")` | `errors.append(rules.NUMERIC_RANGE_INVALID.code)` |

The cursor-range check keeps raising `SourceContractError("Source record cursor is outside the fixed extraction range")`. `CURSOR_OUT_OF_RANGE` is the registry name for this batch error. It does not appear in quarantine.

Run: `grep -nE '"[A-Z][A-Z_]{3,}"' src/ingestion/validation.py`
Expected: no output.

Run: `uv run pytest tests/ingestion/test_rules.py tests/ingestion/test_validation.py tests/ingestion/test_corruption.py -v`
Expected: PASS.

- [ ] **Step 4: Write the failing duplicate corruption tests**

Append to `tests/ingestion/test_corruption.py`. Add `DUPLICATE_PRIMARY_KEY` to the `src.ingestion.corruption` import and `import pytest`:

```python
def test_duplicate_primary_key_copies_the_previous_record_key() -> None:
    """Duplicate Corruption은 직전 Record PK를 복사해 BATCH_DUPLICATE 하나만 만든다."""
    base = datetime(2026, 9, 7, tzinfo=UTC)
    orders = tuple(_order(base + timedelta(seconds=index), index) for index in range(3))
    plan = CorruptionPlan({1: DUPLICATE_PRIMARY_KEY})

    corrupted = plan.apply_page(orders, 0)

    assert corrupted[1].values["order_id"] == orders[0].values["order_id"]
    assert corrupted[1].cursor == orders[1].cursor
    assert orders[1].values["order_id"] == "order-0001"
    page = SourcePage(
        records=corrupted,
        lower_bound=CursorPosition(base - timedelta(seconds=1), ("order-0000",)),
        extract_upper_bound=orders[-1].cursor,
    )
    result = ValidationPipeline(
        ORDERS_TABLE, page.lower_bound, page.extract_upper_bound
    ).validate_page(page)
    assert [rejected.error_codes for rejected in result.rejected_records] == [
        ("BATCH_DUPLICATE",)
    ]
    assert len(result.valid_records) == 2


def test_apply_page_uses_batch_ordinals_across_pages() -> None:
    """apply_page는 first_ordinal 기준 Batch 순번으로 Rule을 찾는다."""
    base = datetime(2026, 9, 7, tzinfo=UTC)
    orders = tuple(_order(base + timedelta(seconds=index), index) for index in range(2))
    corrupted = CorruptionPlan({21: INVALID_STATUS}).apply_page(orders, 20)
    assert corrupted[0] is orders[0]
    assert corrupted[1].values["order_status"] == "__invalid_status__"


def test_duplicate_needs_a_preceding_record_in_the_page() -> None:
    """Page 첫 Record에는 복사할 직전 PK가 없으므로 Duplicate를 거부한다."""
    base = datetime(2026, 9, 7, tzinfo=UTC)
    orders = (_order(base, 0),)
    with pytest.raises(ValueError, match="preceding record"):
        CorruptionPlan({5: DUPLICATE_PRIMARY_KEY}).apply_page(orders, 5)
    with pytest.raises(ValueError, match="apply_page"):
        CorruptionPlan({0: DUPLICATE_PRIMARY_KEY}).apply(orders[0], 0)
```

Run: `uv run pytest tests/ingestion/test_corruption.py -v`
Expected: FAIL with `ImportError: cannot import name 'DUPLICATE_PRIMARY_KEY'`

- [ ] **Step 5: Implement duplicate corruption**

In `src/ingestion/corruption.py`:

```python
DUPLICATE_PRIMARY_KEY = "DUPLICATE_PRIMARY_KEY"
SUPPORTED_CORRUPTION_KINDS = frozenset(
    {
        NULL_PRIMARY_KEY,
        INVALID_STATUS,
        NEGATIVE_NUMERIC,
        TYPE_MISMATCH,
        BROKEN_REFERENCE,
        DUPLICATE_PRIMARY_KEY,
    }
)
```

At the top of `CorruptionPlan.apply`, directly after `if kind is None: return record`, add:

```python
        if kind == DUPLICATE_PRIMARY_KEY:
            raise ValueError("DUPLICATE_PRIMARY_KEY needs the previous record; use apply_page")
```

Add this method to `CorruptionPlan`, after `apply`:

```python
    def apply_page(
        self, records: tuple[SourceRecord, ...], first_ordinal: int
    ) -> tuple[SourceRecord, ...]:
        """Page 전체에 Corruption을 적용하고 Duplicate는 직전 출력 Record PK를 복사한다."""
        output: list[SourceRecord] = []
        for index, record in enumerate(records):
            ordinal = first_ordinal + index
            if self.rules.get(ordinal) != DUPLICATE_PRIMARY_KEY:
                output.append(self.apply(record, ordinal))
                continue
            if not output:
                raise ValueError("DUPLICATE_PRIMARY_KEY requires a preceding record in the page")
            output.append(_with_primary_key_of(record, output[-1]))
        return tuple(output)
```

Add this module function below `_child_reference_column`:

```python
def _with_primary_key_of(record: SourceRecord, previous: SourceRecord) -> SourceRecord:
    """Record의 PK Column 값만 직전 Record 값으로 바꾼 복제본을 만든다."""
    values = dict(record.values)
    for column in record.config.primary_key_columns:
        values[column] = previous.values[column]
    return SourceRecord(record.config, MappingProxyType(values), cursor_override=record.cursor)
```

- [ ] **Step 6: Use `apply_page` in the service**

In `src/ingestion/service.py`, replace:

```python
            records = tuple(
                request.corruption_plan.apply(record, rows_extracted + index)
                if request.corruption_plan is not None
                else record
                for index, record in enumerate(page.records)
            )
```

with:

```python
            records = (
                request.corruption_plan.apply_page(tuple(page.records), rows_extracted)
                if request.corruption_plan is not None
                else tuple(page.records)
            )
```

Leave the `rows_corrupted` block unchanged.

- [ ] **Step 7: Replace the reject-rate test with boundary cases**

In `tests/ingestion/test_quarantine.py`, replace `test_reject_rate_allows_five_percent_and_rejects_more` with:

```python
@pytest.mark.parametrize(
    ("total_rows", "rejected_rows"), [(0, 0), (20, 0), (20, 1), (100, 5)]
)
def test_reject_rate_allows_up_to_five_percent(total_rows: int, rejected_rows: int) -> None:
    """빈 Batch, 0건, 정확히 5%까지는 Batch를 통과시킨다."""
    assert MAX_REJECT_RATE == 0.05
    assert_reject_rate(total_rows=total_rows, rejected_rows=rejected_rows)


@pytest.mark.parametrize(("total_rows", "rejected_rows"), [(20, 2), (100, 6), (1, 1)])
def test_reject_rate_rejects_more_than_five_percent(total_rows: int, rejected_rows: int) -> None:
    """5%를 넘으면 Watermark를 전진시키지 않을 Batch 오류로 만든다."""
    with pytest.raises(RejectRateExceededError, match="5%"):
        assert_reject_rate(total_rows=total_rows, rejected_rows=rejected_rows)
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `uv run pytest tests/ingestion -v`
Expected: PASS.

Run: `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/integration/test_orders_ingestion_service_integration.py tests/integration/test_validation_integration.py -v`
Expected: PASS (existing corruption paths still work through `apply_page`).

- [ ] **Step 9: Commit**

```bash
uv run ruff check src/ingestion tests/ingestion
git add src/ingestion/rules.py src/ingestion/validation.py src/ingestion/corruption.py \
  src/ingestion/service.py tests/ingestion/test_rules.py tests/ingestion/test_corruption.py \
  tests/ingestion/test_quarantine.py
git commit  # via /caveman:caveman-commit, e.g. "feat(ingestion): add validation rule registry and duplicate corruption"
```

---

### Task 7: Corruption matrix and frozen-parent integration tests (AC-08, P7-03)

**Files:**
- Test: `tests/integration/test_corruption_matrix_integration.py` (create)
- Test: `tests/integration/test_child_parent_references_integration.py` (append P7-03 frozen-parent test)

**Interfaces:**
- Consumes: Task 6 `DUPLICATE_PRIMARY_KEY` and `apply_page` (through `ingest_table`); existing `NULL_PRIMARY_KEY`, `INVALID_STATUS`, `NEGATIVE_NUMERIC`, `BROKEN_REFERENCE`, `CorruptionPlan`; `TableIngestionRequest.for_dag_run(..., corruption_plan=...)`, `ingest_table`, `table_object_keys`, `quarantine_object_keys` from `src.ingestion.service`; `get_or_create_watermark`, `CursorPosition` from `src.ingestion.metadata`; `table_config`, `TableConfig` from `src.ingestion.tables`.
- Produces: nothing new in `src/`. This task is test-only. No production code changes are expected. If a case fails, report to architect before changing `src/`.

The five cases are the ingestion rows of the phase-07 Corruption Matrix. Each case uses one batch of exactly 20 rows (`page_size = 20`) with the corruption at ordinal 1. One reject in 20 rows is exactly 5%, so the batch commits.

| Case | Table | Kind | Expected `error_counts` |
|---|---|---|---|
| Duplicate | `orders` | `DUPLICATE_PRIMARY_KEY` | `{"BATCH_DUPLICATE": 1}` |
| NULL Key | `orders` | `NULL_PRIMARY_KEY` | `{"REQUIRED_NULL": 1, "KEY_NULL": 1}` |
| Broken FK | `order_items` | `BROKEN_REFERENCE` | `{"BROKEN_REFERENCE": 1}` |
| Invalid Status | `orders` | `INVALID_STATUS` | `{"STATUS_DOMAIN_INVALID": 1}` |
| Negative Value | `order_items` | `NEGATIVE_NUMERIC` | `{"NUMERIC_RANGE_INVALID": 1}` |

- [ ] **Step 1: Write the test**

```python
"""Corruption Matrix의 Ingestion 5종이 격리·Commit·Watermark 규칙을 지키는지 검증한다 (AC-08)."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime

import pytest
from psycopg.types.json import Jsonb

from src.common.database import PostgresSettings
from src.ingestion.corruption import (
    BROKEN_REFERENCE,
    DUPLICATE_PRIMARY_KEY,
    INVALID_STATUS,
    NEGATIVE_NUMERIC,
    NULL_PRIMARY_KEY,
    CorruptionPlan,
)
from src.ingestion.metadata import CursorPosition, get_or_create_watermark
from src.ingestion.service import (
    TableIngestionRequest,
    ingest_table,
    quarantine_object_keys,
    table_object_keys,
)
from src.ingestion.storage import SeaweedFSSettings, seaweedfs_s3_client
from src.ingestion.tables import TableConfig, table_config

pytestmark = pytest.mark.integration

BATCH_ROWS = 20
CORRUPTED_ORDINAL = 1

CORRUPTION_MATRIX = (
    pytest.param("orders", DUPLICATE_PRIMARY_KEY, {"BATCH_DUPLICATE": 1}, id="duplicate"),
    pytest.param(
        "orders", NULL_PRIMARY_KEY, {"REQUIRED_NULL": 1, "KEY_NULL": 1}, id="null-key"
    ),
    pytest.param("order_items", BROKEN_REFERENCE, {"BROKEN_REFERENCE": 1}, id="broken-fk"),
    pytest.param("orders", INVALID_STATUS, {"STATUS_DOMAIN_INVALID": 1}, id="invalid-status"),
    pytest.param(
        "order_items", NEGATIVE_NUMERIC, {"NUMERIC_RANGE_INVALID": 1}, id="negative-value"
    ),
)


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
@pytest.mark.parametrize(("source_table", "kind", "expected_errors"), CORRUPTION_MATRIX)
def test_corruption_is_quarantined_while_valid_rows_commit(
    tmp_path, source_table: str, kind: str, expected_errors: dict[str, int]
) -> None:
    """오염 1건은 격리되고 19건은 Commit되며 Source는 그대로이고 Watermark는 전진한다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    config = table_config(source_table)
    now = datetime.now(UTC)
    pipeline_name = f"test_corruption_{uuid.uuid4().hex}"
    request = TableIngestionRequest.for_dag_run(
        source_table=source_table,
        dag_id=f"warehouse_{uuid.uuid4().hex}",
        logical_date=now,
        pipeline_name=pipeline_name,
        page_size=BATCH_ROWS,
        corruption_plan=CorruptionPlan({CORRUPTED_ORDINAL: kind}),
    )
    source_before = _latest_source_rows(postgres, config)
    newest_cursor = _cursor_at_offset(postgres, config, 0)
    start_cursor = _cursor_at_offset(postgres, config, BATCH_ROWS)
    _set_watermark(postgres, pipeline_name, config, start_cursor, now)

    try:
        result = ingest_table(postgres, storage, request, local_directory=tmp_path, now=now)

        assert result.status == "SUCCESS"
        assert (result.row_count, result.rows_rejected, result.rows_corrupted) == (19, 1, 1)
        with postgres.pipeline_connection() as connection:
            quarantine = connection.execute(
                """
                SELECT object_key, row_count, error_counts FROM quarantine_batches
                WHERE table_batch_id = %s
                """,
                (f"{request.batch_id}__{source_table}",),
            ).fetchone()
            run_counts = connection.execute(
                """
                SELECT rows_extracted, rows_valid, rows_rejected, rows_loaded, status
                FROM pipeline_runs WHERE pipeline_name = %s AND source_table = %s
                """,
                (pipeline_name, source_table),
            ).fetchone()
            watermark = connection.execute(
                """
                SELECT watermark_timestamp, watermark_keys FROM watermarks
                WHERE pipeline_name = %s AND source_table = %s
                """,
                (pipeline_name, source_table),
            ).fetchone()
        assert quarantine == (result.quarantine_object_key, 1, expected_errors)
        assert run_counts == (BATCH_ROWS, 19, 1, 19, "SUCCESS")
        assert watermark == (newest_cursor.timestamp, newest_cursor.as_json())
        assert _latest_source_rows(postgres, config) == source_before
    finally:
        _cleanup(postgres, storage, pipeline_name, request)


def _cursor_at_offset(
    settings: PostgresSettings, config: TableConfig, offset: int
) -> CursorPosition:
    """최신순으로 offset번째 Row의 Composite Cursor를 읽는다."""
    with settings.source_connection() as connection:
        row = connection.execute(
            f"""
            SELECT {", ".join(config.cursor_columns)}
            FROM {config.source_table}
            ORDER BY {_order_by(config, descending=True)}
            OFFSET %s LIMIT 1
            """,
            (offset,),
        ).fetchone()
    if row is None:
        raise RuntimeError(f"The seeded source needs more than {offset} {config.source_table}")
    return CursorPosition(row[0], tuple(row[1:]))


def _latest_source_rows(settings: PostgresSettings, config: TableConfig) -> list[tuple]:
    """Source 무변경 비교용으로 최신 BATCH_ROWS개 Row 전체를 읽는다."""
    with settings.source_connection() as connection:
        return connection.execute(
            f"""
            SELECT * FROM {config.source_table}
            ORDER BY {_order_by(config, descending=True)}
            LIMIT %s
            """,
            (BATCH_ROWS,),
        ).fetchall()


def _set_watermark(
    settings: PostgresSettings,
    pipeline_name: str,
    config: TableConfig,
    cursor: CursorPosition,
    now: datetime,
) -> None:
    """최신 BATCH_ROWS개 Row만 추출되도록 Watermark를 그 직전 Cursor로 설정한다."""
    get_or_create_watermark(settings, pipeline_name, config.source_table, now=now)
    with settings.pipeline_connection() as connection:
        connection.execute(
            """
            UPDATE watermarks SET watermark_timestamp = %s, watermark_keys = %s
            WHERE pipeline_name = %s AND source_table = %s
            """,
            (cursor.timestamp, Jsonb(cursor.as_json()), pipeline_name, config.source_table),
        )
        connection.commit()


def _order_by(config: TableConfig, *, descending: bool = False) -> str:
    """문자열 Key의 C Collation을 보존한 Cursor SQL ORDER BY를 만든다."""
    suffix = " DESC" if descending else ""
    key_columns = [
        f'{column} COLLATE "C"' if column == "order_id" else column
        for column in config.cursor_columns
    ]
    return ", ".join(f"{column}{suffix}" for column in key_columns)


def _cleanup(
    postgres: PostgresSettings,
    storage: SeaweedFSSettings,
    pipeline_name: str,
    request: TableIngestionRequest,
) -> None:
    """테스트가 만든 Bronze·Quarantine Object와 Metadata만 정리한다."""
    table = request.source_table
    keys = (
        *table_object_keys(table, request.batch_id, request.logical_date),
        *quarantine_object_keys(table, request.batch_id, request.logical_date),
    )
    client = seaweedfs_s3_client(storage)
    for key in keys:
        client.delete_object(Bucket=storage.bucket, Key=key)
    table_batch_id = f"{request.batch_id}__{table}"
    with postgres.pipeline_connection() as connection:
        connection.execute(
            "DELETE FROM quarantine_batches WHERE table_batch_id = %s", (table_batch_id,)
        )
        connection.execute(
            "DELETE FROM bronze_objects WHERE table_batch_id = %s", (table_batch_id,)
        )
        connection.execute(
            "DELETE FROM pipeline_runs WHERE pipeline_name = %s AND source_table = %s",
            (pipeline_name, table),
        )
        connection.execute(
            "DELETE FROM watermarks WHERE pipeline_name = %s AND source_table = %s",
            (pipeline_name, table),
        )
        connection.commit()
```

Check before running:
- `request.source_table` exists on `TableIngestionRequest` (`grep -n "class TableIngestionRequest" -A12 src/ingestion/service.py`). If the field has another name, use that name in `_cleanup`.
- Do not run this test while a generator run writes to the source DB. New source rows would move the latest-20 window.

- [ ] **Step 2: Add the frozen-parent reference test (P7-03)**

The existing test `tests/integration/test_child_parent_references_integration.py` only proves that seeded children resolve. P7-03 also needs proof that parent lookups use the frozen snapshot. A parent committed after the snapshot opens must stay invisible, so a child that points to it is a broken reference.

Append to `tests/integration/test_child_parent_references_integration.py`. Add `import uuid`, `from dataclasses import replace` and `from src.ingestion.extract import SourcePage` to the imports. The module already has `_lower_bound_before_five_latest_rows` and `_order_by`.

```python
@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
def test_parent_committed_after_snapshot_is_not_visible() -> None:
    """Snapshot을 연 뒤 Commit된 Parent는 보이지 않아 해당 Child 참조는 Broken으로 판정된다."""
    settings = PostgresSettings.from_environment()
    config = table_config("order_items")
    lower_bound = _lower_bound_before_five_latest_rows(settings, config)
    late_seller_id = f"test-late-seller-{uuid.uuid4().hex}"

    try:
        with open_table_snapshot(settings, config, lower_bound, page_size=2) as snapshot:
            page = next(snapshot.pages())
            with settings.source_connection() as connection:
                connection.execute(
                    """
                    INSERT INTO sellers (
                        seller_id, seller_city, seller_state, created_at, updated_at
                    )
                    VALUES (%s, 'test', 'SP', now(), now())
                    """,
                    (late_seller_id,),
                )
                connection.commit()
            first = page.records[0]
            late_child = replace(first, values={**first.values, "seller_id": late_seller_id})
            probe = SourcePage(
                (late_child, *page.records[1:]), page.lower_bound, page.extract_upper_bound
            )
            broken = find_broken_parent_references(snapshot, probe)
    finally:
        with settings.source_connection() as connection:
            connection.execute("DELETE FROM sellers WHERE seller_id = %s", (late_seller_id,))
            connection.commit()

    assert [(item.parent_table, item.missing_key) for item in broken] == [
        ("sellers", late_seller_id)
    ]
```

Check before running:
- `replace(first, values=...)` keeps the column order because `{**first.values, ...}` only overwrites an existing key. `SourceRecord.__post_init__` checks the order.
- If the source `sellers` table has a mutation guard trigger or needs a source mutation lease, stop and report to architect. Do not disable the guard.

Run: `RUN_POSTGRES_INTEGRATION=1 uv run pytest tests/integration/test_child_parent_references_integration.py -v`
Expected: 3 passed (2 existing, 1 new).

- [ ] **Step 3: Run the corruption matrix test**

Run: `docker compose up -d --wait postgres seaweedfs && RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/integration/test_corruption_matrix_integration.py -v`
Expected: 5 passed.

- [ ] **Step 4: Commit**

```bash
uv run ruff check tests/integration/test_corruption_matrix_integration.py \
  tests/integration/test_child_parent_references_integration.py
git add tests/integration/test_corruption_matrix_integration.py \
  tests/integration/test_child_parent_references_integration.py
git commit  # via /caveman:caveman-commit, e.g. "test(ingestion): add corruption matrix integration test"
```

---

### Task 8: dbt quality tests and publish gate canary

**Files:**
- Create: `dbt/tests/generic/non_negative.sql`
- Create: `dbt/tests/stg_orders_timestamp_order.sql`
- Create: `dbt/tests/fct_order_item_total_matches_order.sql`
- Create: `dbt/tests/publish_gate_canary.sql`
- Modify: `dbt/models/marts/facts/schema.yml`
- Test: `tests/test_warehouse_quality_contract.py` (create)
- Doc (not committed): `docs/phases/phase-07-data-quality-publish.md`, a P7-06/P7-10 audit table

**Interfaces:**
- Consumes: Task 4 CLI `uv run python -m src.warehouse.publish [--dbt-vars YAML]`, which prints one JSON line and returns 0 (PUBLISHED) or 1 (FAILED). Task 3 classifies a failed `test` node as `DBT_TEST_ERROR`.
- Produces: dbt var `publish_gate_canary` (default `false`). When `true`, the singular test `publish_gate_canary` fails. Task 10 uses this var to force a gate failure with real dbt.

Facts the tests rely on (checked against the current models):
- `fct_order.order_status` comes from macro `standardized_order_status`. The 8 values are `CREATED, APPROVED, PROCESSING, INVOICED, SHIPPED, DELIVERED, CANCELED, UNAVAILABLE`. An unmapped source value becomes NULL, so `not_null` catches it.
- `fct_order_payment.payment_status` comes from `standardized_payment_status`. The values are `PENDING, COMPLETED, FAILED, REFUNDED`.
- `fct_order.gross_order_value = coalesce(sum(price), 0) + coalesce(sum(freight_value), 0)` per order (`int_order_fact_ready`). `fct_order_item.line_gross_value = price + freight_value`. The two sums must match for every order.
- `stg_orders` exposes `purchase_at`, `approved_at`, `delivered_at`.
- Relationship tests in this repo use the `arguments:` block form. Keep it.

- [ ] **Step 1: Write the failing contract test**

`pyyaml` is already installed as a dbt dependency. Do not add it to `pyproject.toml`.

```python
"""Phase 7 품질 Gate가 요구하는 dbt Test가 Schema와 tests/ 경로에 선언됐는지 검증한다."""

from __future__ import annotations

from pathlib import Path

import yaml

DBT_DIR = Path(__file__).resolve().parents[1] / "dbt"
FACT_SCHEMA = DBT_DIR / "models" / "marts" / "facts" / "schema.yml"
ORDER_STATUSES = [
    "CREATED",
    "APPROVED",
    "PROCESSING",
    "INVOICED",
    "SHIPPED",
    "DELIVERED",
    "CANCELED",
    "UNAVAILABLE",
]
PAYMENT_STATUSES = ["PENDING", "COMPLETED", "FAILED", "REFUNDED"]


def _column_tests(model: str, column: str) -> list:
    """facts/schema.yml에서 지정 Model·Column의 data_tests 목록을 읽는다."""
    schema = yaml.safe_load(FACT_SCHEMA.read_text(encoding="utf-8"))
    model_entry = next(item for item in schema["models"] if item["name"] == model)
    column_entry = next(item for item in model_entry["columns"] if item["name"] == column)
    return column_entry.get("data_tests", [])


def _named(tests: list, name: str) -> dict | None:
    """data_tests 목록에서 이름이 name인 Test의 설정 Dict를 찾는다."""
    for test in tests:
        if isinstance(test, dict) and name in test:
            return test[name]
    return None


def _relationship_target(model: str, column: str) -> tuple[str, str]:
    """relationships Test의 대상 ref와 Field를 반환한다."""
    arguments = _named(_column_tests(model, column), "relationships")["arguments"]
    return arguments["to"], arguments["field"]


def test_order_status_domain_is_enforced() -> None:
    """주문 상태는 NULL 금지이고 8개 표준 상태만 허용한다."""
    tests = _column_tests("fct_order", "order_status")
    assert "not_null" in tests
    assert _named(tests, "accepted_values")["arguments"]["values"] == ORDER_STATUSES


def test_payment_status_domain_is_enforced() -> None:
    """주문 결제 상태는 NULL 금지이고 4개 표준 상태만 허용한다."""
    tests = _column_tests("fct_order_payment", "payment_status")
    assert "not_null" in tests
    assert _named(tests, "accepted_values")["arguments"]["values"] == PAYMENT_STATUSES


def test_fact_foreign_keys_are_enforced() -> None:
    """Line·결제 Fact의 FK는 주문 Fact와 상품·판매자 Dimension을 가리킨다."""
    assert _relationship_target("fct_order_item", "order_id") == (
        "ref('fct_order')",
        "order_id",
    )
    assert _relationship_target("fct_order_item", "product_id") == (
        "ref('dim_product')",
        "product_id",
    )
    assert _relationship_target("fct_order_item", "seller_id") == (
        "ref('dim_seller')",
        "seller_id",
    )
    assert _relationship_target("fct_order_payment", "order_id") == (
        "ref('fct_order')",
        "order_id",
    )


def test_money_columns_are_non_negative() -> None:
    """금액 Column에는 non_negative Generic Test가 붙는다."""
    for model, column in (
        ("fct_order_item", "item_price"),
        ("fct_order_item", "freight_value"),
        ("fct_order_payment", "payment_value"),
        ("fct_subscription_payment", "payment_value"),
    ):
        assert "non_negative" in _column_tests(model, column), (model, column)


def test_singular_quality_tests_exist() -> None:
    """시간 순서·금액 합계·Gate Canary Singular Test와 Generic Test 파일이 있다."""
    tests_dir = DBT_DIR / "tests"
    for name in (
        "generic/non_negative.sql",
        "stg_orders_timestamp_order.sql",
        "fct_order_item_total_matches_order.sql",
        "publish_gate_canary.sql",
    ):
        assert (tests_dir / name).is_file(), name


def test_publish_gate_canary_defaults_to_pass() -> None:
    """Canary는 var 기본값 false에서는 Row를 반환하지 않는다."""
    sql = (DBT_DIR / "tests" / "publish_gate_canary.sql").read_text(encoding="utf-8")
    assert "var('publish_gate_canary', false)" in sql
```

- [ ] **Step 2: Run the test to see it fail**

Run: `uv run pytest tests/test_warehouse_quality_contract.py -v`
Expected: FAIL. `StopIteration` for `order_status`, and missing test files.

- [ ] **Step 3: Create the dbt test files**

`dbt/tests/generic/non_negative.sql`:

```sql
{% test non_negative(model, column_name) %}
-- 금액·수량 Column에 음수가 없는지 검증한다. NULL은 not_null Test가 따로 판단한다.
select *
from {{ model }}
where {{ column_name }} < 0
{% endtest %}
```

`dbt/tests/stg_orders_timestamp_order.sql`:

```sql
-- 승인·배송완료 시각은 구매 시각보다 빠를 수 없다. NULL 시각은 아직 발생하지 않은 이벤트로 통과한다.
select
    order_id,
    purchase_at,
    approved_at,
    delivered_at
from {{ ref('stg_orders') }}
where approved_at < purchase_at
    or delivered_at < purchase_at
```

`dbt/tests/fct_order_item_total_matches_order.sql`:

```sql
-- 주문 Fact의 gross_order_value는 해당 주문 Line Fact의 line_gross_value 합계와 같아야 한다.
with item_totals as (
    select
        order_id,
        sum(line_gross_value) as line_total
    from {{ ref('fct_order_item') }}
    group by order_id
)

select
    fct_order.order_id,
    fct_order.gross_order_value,
    coalesce(item_totals.line_total, 0) as line_total
from {{ ref('fct_order') }} as fct_order
left join item_totals using (order_id)
where abs(fct_order.gross_order_value - coalesce(item_totals.line_total, 0)) > 0.01
```

`dbt/tests/publish_gate_canary.sql`:

```sql
-- Publish Gate 검증용 Canary. --vars '{publish_gate_canary: true}'일 때만 1 Row를 반환해 실패한다.
select 1 as failure
where {{ var('publish_gate_canary', false) }}
```

- [ ] **Step 4: Extend `dbt/models/marts/facts/schema.yml`**

Add these column entries. Keep all existing entries unchanged.

Under `fct_order.columns`:

```yaml
      - name: order_status
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values:
                  [CREATED, APPROVED, PROCESSING, INVOICED, SHIPPED, DELIVERED, CANCELED, UNAVAILABLE]
```

Replace the `fct_order_item.order_id` entry and add columns:

```yaml
      - name: order_id
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('fct_order')
                field: order_id
      - name: product_id
        data_tests:
          - relationships:
              arguments:
                to: ref('dim_product')
                field: product_id
      - name: seller_id
        data_tests:
          - relationships:
              arguments:
                to: ref('dim_seller')
                field: seller_id
      - name: item_price
        data_tests: [non_negative]
      - name: freight_value
        data_tests: [non_negative]
```

Replace the `fct_order_payment.order_id` entry and add columns:

```yaml
      - name: order_id
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('fct_order')
                field: order_id
      - name: payment_status
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: [PENDING, COMPLETED, FAILED, REFUNDED]
      - name: payment_value
        data_tests: [non_negative]
```

Under `fct_subscription_payment.columns`:

```yaml
      - name: payment_value
        data_tests: [non_negative]
```

- [ ] **Step 5: Run the contract test**

Run: `uv run pytest tests/test_warehouse_quality_contract.py -v`
Expected: 6 passed.

- [ ] **Step 6: Run the gate on real data**

Prerequisite: Postgres and SeaweedFS are running, and Bronze holds at least one ingested batch per table.

```bash
uv run python -m src.warehouse.publish
uv run python -m src.warehouse.mart_hash data/warehouse/warehouse.duckdb > "$SCRATCH/hash_before.json"
uv run python -m src.warehouse.publish --dbt-vars '{publish_gate_canary: true}'; echo "exit=$?"
uv run python -m src.warehouse.mart_hash data/warehouse/warehouse.duckdb > "$SCRATCH/hash_after.json"
diff "$SCRATCH/hash_before.json" "$SCRATCH/hash_after.json" && echo "published unchanged"
```

`$SCRATCH` is any temporary directory outside the repo.

Expected:
- The first publish prints `"status": "PUBLISHED"` and returns 0.
- The canary run prints `"status": "FAILED", "error_type": "DBT_TEST_ERROR"` and `exit=1`.
- `diff` shows no difference and prints `published unchanged`.
- `mart_publish_runs` has the canary run as `FAILED`. Its build file is in `data/warehouse/failed/`.

If a new relationship, accepted_values or singular test fails on real data (not the canary), stop. Do not relax the test. Report the failing node and a sample row to architect.

- [ ] **Step 7: Record the audit table in the phase-07 doc (not committed)**

Add this section to `docs/phases/phase-07-data-quality-publish.md`. Fill `Result` from the Step 6 run.

```markdown
## P7-06 · P7-10 dbt 품질 Test 감사

| 영역 | Test | 위치 | 결과 |
|---|---|---|---|
| 필수값 | not_null (주문·결제 상태) | facts/schema.yml | PASS |
| 도메인 | accepted_values 주문 8종·결제 4종 | facts/schema.yml | PASS |
| FK | relationships item→order/product/seller, payment→order | facts/schema.yml | PASS |
| 금액 범위 | non_negative 4개 Column | tests/generic/non_negative.sql | PASS |
| 시간 순서 | stg_orders_timestamp_order | dbt/tests/ | PASS |
| 금액 합계 | fct_order_item_total_matches_order | dbt/tests/ | PASS |
| Gate 차단 | publish_gate_canary=true → DBT_TEST_ERROR, Hash 무변경 | dbt/tests/ | PASS |
```

- [ ] **Step 8: Commit**

```bash
uv run ruff check tests/test_warehouse_quality_contract.py
git add dbt/tests/generic/non_negative.sql dbt/tests/stg_orders_timestamp_order.sql \
  dbt/tests/fct_order_item_total_matches_order.sql dbt/tests/publish_gate_canary.sql \
  dbt/models/marts/facts/schema.yml tests/test_warehouse_quality_contract.py
git commit  # via /caveman:caveman-commit, e.g. "test(dbt): add publish quality gate tests"
```

---

### Task 9: Observability run status query (AC-13)

**Files:**
- Create: `sql/validation/observability_run_status.sql`
- Test: `tests/integration/test_observability_run_status_integration.py` (create)

**Interfaces:**
- Consumes: `pipeline_runs` (Phase 3), Task 2 `mart_publish_runs` and `ensure_publish_metadata(settings)`, `ensure_ingestion_metadata(settings)` from `src.ingestion.metadata`.
- Produces: one SQL query. It returns one row per `(pipeline_name, batch_id)` with these columns: `pipeline_name, batch_id, logical_date, run_outcome, table_count, max_attempt_number, rows_extracted, rows_rejected, rows_loaded, ingestion_error_types, publish_run_id, publish_status, publish_error_type, started_at, finished_at`.

Rules:
- `run_outcome` priority: `FAILED` > `RUNNING` > `RERUN` > `EMPTY` > `SUCCESS`.
  - `FAILED`: the latest attempt of any table is `FAILED`, or the latest publish run is `FAILED`.
  - `RUNNING`: any latest table attempt is `RUNNING`, or the latest publish run is `BUILDING`/`PUBLISHING`.
  - `RERUN`: any attempt has `attempt_number > 1`, or any latest table attempt is `SKIPPED_ALREADY_COMMITTED`.
  - `EMPTY`: every latest table attempt is `SUCCESS_NO_DATA`.
  - `SUCCESS`: everything else.
- Row counts use only the latest attempt per table. Earlier failed attempts must not double count.
- The latest publish run per `batch_id` comes from `DISTINCT ON (batch_id)` ordered by `started_at DESC`.
- The file has no trailing semicolon and no `%` character. The test wraps it as a subquery with psycopg parameters.

- [ ] **Step 1: Write the failing integration test**

```python
"""Observability SQL 한 번으로 성공·빈·실패·재실행 Batch가 구분되는지 검증한다 (AC-13)."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from psycopg.types.json import Jsonb

from src.common.database import PostgresSettings
from src.ingestion.metadata import ensure_ingestion_metadata
from src.warehouse.publish_metadata import ensure_publish_metadata

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parents[2]
QUERY_PATH = PROJECT_ROOT / "sql" / "validation" / "observability_run_status.sql"
# 다른 Publish의 previous 조회에 끼지 않도록 Fixture Publish Row는 과거 시각으로 만든다.
FIXTURE_TIME = datetime(2000, 1, 1, tzinfo=UTC)


@pytest.fixture
def settings() -> PostgresSettings:
    """Metadata Table을 준비한 PostgreSQL 설정을 반환한다."""
    if os.environ.get("RUN_POSTGRES_INTEGRATION") != "1":
        pytest.skip("Set RUN_POSTGRES_INTEGRATION=1 after starting PostgreSQL.")
    postgres = PostgresSettings.from_environment()
    ensure_ingestion_metadata(postgres)
    ensure_publish_metadata(postgres)
    return postgres


def test_observability_query_distinguishes_run_outcomes(settings) -> None:
    """네 가지 상태와 Publish 실패가 run_outcome과 Publish Column에 드러난다."""
    pipeline_name = f"test_observability_{uuid.uuid4().hex}"
    runs = (
        # (batch_id, source_table, attempt_number, status, rows_extracted, error_type)
        ("b1_success", "orders", 1, "SUCCESS", 10, None),
        ("b1_success", "payments", 1, "SUCCESS", 5, None),
        ("b2_empty", "orders", 1, "SUCCESS_NO_DATA", 0, None),
        ("b2_empty", "payments", 1, "SUCCESS_NO_DATA", 0, None),
        ("b3_ingest_failed", "orders", 1, "FAILED", 0, "SOURCE_CONNECTION_ERROR"),
        ("b3_ingest_failed", "payments", 1, "SUCCESS", 5, None),
        ("b4_rerun", "orders", 1, "FAILED", 0, "SOURCE_CONNECTION_ERROR"),
        ("b4_rerun", "orders", 2, "SUCCESS", 10, None),
        ("b5_skipped", "orders", 1, "SKIPPED_ALREADY_COMMITTED", 0, None),
        ("b6_publish_failed", "orders", 1, "SUCCESS", 10, None),
    )
    publishes = (
        # (batch_id, status, error_type)
        ("b1_success", "PUBLISHED", None),
        ("b6_publish_failed", "FAILED", "DBT_TEST_ERROR"),
    )
    try:
        _insert_pipeline_runs(settings, pipeline_name, runs)
        _insert_publish_runs(settings, pipeline_name, publishes)
        rows = _query(settings, pipeline_name)
    finally:
        _cleanup(settings, pipeline_name)

    assert rows == [
        (f"{pipeline_name}:b1_success", "SUCCESS", 2, 1, 15, "PUBLISHED", None),
        (f"{pipeline_name}:b2_empty", "EMPTY", 2, 1, 0, None, None),
        (f"{pipeline_name}:b3_ingest_failed", "FAILED", 2, 1, 5, None, None),
        (f"{pipeline_name}:b4_rerun", "RERUN", 1, 2, 10, None, None),
        (f"{pipeline_name}:b5_skipped", "RERUN", 1, 1, 0, None, None),
        (f"{pipeline_name}:b6_publish_failed", "FAILED", 1, 1, 10, "FAILED", "DBT_TEST_ERROR"),
    ]


def _query(settings: PostgresSettings, pipeline_name: str) -> list[tuple]:
    """Observability SQL을 Subquery로 감싸 테스트 Pipeline의 Row만 읽는다."""
    sql = QUERY_PATH.read_text(encoding="utf-8")
    with settings.pipeline_connection() as connection:
        return connection.execute(
            f"""
            SELECT batch_id, run_outcome, table_count, max_attempt_number,
                   rows_extracted, publish_status, publish_error_type
            FROM ({sql}) AS run_status
            WHERE pipeline_name = %s
            ORDER BY batch_id
            """,
            (pipeline_name,),
        ).fetchall()


def _insert_pipeline_runs(
    settings: PostgresSettings, pipeline_name: str, runs: tuple[tuple, ...]
) -> None:
    """Fixture Ingestion Run을 pipeline_runs에 넣는다."""
    with settings.pipeline_connection() as connection:
        for index, (batch, table, attempt, status, extracted, error_type) in enumerate(runs):
            started_at = FIXTURE_TIME + timedelta(minutes=index)
            connection.execute(
                """
                INSERT INTO pipeline_runs (
                    run_id, source_table, pipeline_name, batch_id, logical_date,
                    attempt_number, started_at, finished_at, watermark_before,
                    rows_extracted, rows_valid, rows_loaded, status, error_type, error_message
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    uuid.uuid4(),
                    table,
                    pipeline_name,
                    f"{pipeline_name}:{batch}",
                    FIXTURE_TIME,
                    attempt,
                    started_at,
                    started_at + timedelta(seconds=30),
                    Jsonb({}),
                    extracted,
                    extracted,
                    extracted,
                    status,
                    error_type,
                    None if error_type is None else "fixture failure",
                ),
            )
        connection.commit()


def _insert_publish_runs(
    settings: PostgresSettings, pipeline_name: str, publishes: tuple[tuple, ...]
) -> None:
    """Fixture Publish Run을 종료 상태로 mart_publish_runs에 넣는다."""
    with settings.pipeline_connection() as connection:
        for batch, status, error_type in publishes:
            published = status == "PUBLISHED"
            connection.execute(
                """
                INSERT INTO mart_publish_runs (
                    publish_run_id, pipeline_name, batch_id, status, error_type,
                    error_message, mart_hashes, mart_row_counts, started_at, finished_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    uuid.uuid4(),
                    pipeline_name,
                    f"{pipeline_name}:{batch}",
                    status,
                    error_type,
                    None if error_type is None else "fixture failure",
                    Jsonb({}) if published else None,
                    Jsonb({}) if published else None,
                    FIXTURE_TIME,
                    FIXTURE_TIME + timedelta(minutes=1),
                ),
            )
        connection.commit()


def _cleanup(settings: PostgresSettings, pipeline_name: str) -> None:
    """테스트 Pipeline 이름으로 만든 Fixture Row만 지운다."""
    with settings.pipeline_connection() as connection:
        connection.execute(
            "DELETE FROM mart_publish_runs WHERE pipeline_name = %s", (pipeline_name,)
        )
        connection.execute("DELETE FROM pipeline_runs WHERE pipeline_name = %s", (pipeline_name,))
        connection.commit()
```

- [ ] **Step 2: Run the test to see it fail**

Run: `RUN_POSTGRES_INTEGRATION=1 uv run pytest tests/integration/test_observability_run_status_integration.py -v`
Expected: FAIL with `FileNotFoundError` for `observability_run_status.sql`.

- [ ] **Step 3: Write `sql/validation/observability_run_status.sql`**

```sql
-- Batch별 Ingestion 결과와 연결된 최신 Warehouse Publish 결과를 한 번에 조회한다.
-- run_outcome 우선순위: FAILED > RUNNING > RERUN > EMPTY > SUCCESS.
with ranked_runs as (
    select
        pipeline_runs.*,
        row_number() over (
            partition by pipeline_name, batch_id, source_table
            order by attempt_number desc, started_at desc
        ) as attempt_rank,
        max(attempt_number) over (
            partition by pipeline_name, batch_id
        ) as batch_max_attempt_number
    from pipeline_runs
),

latest_runs as (
    select *
    from ranked_runs
    where attempt_rank = 1
),

batch_runs as (
    select
        pipeline_name,
        batch_id,
        min(logical_date) as logical_date,
        count(*) as table_count,
        max(batch_max_attempt_number) as max_attempt_number,
        count(*) filter (where status = 'FAILED') as failed_tables,
        count(*) filter (where status = 'RUNNING') as running_tables,
        count(*) filter (where status = 'SKIPPED_ALREADY_COMMITTED') as skipped_tables,
        count(*) filter (where status = 'SUCCESS_NO_DATA') as empty_tables,
        sum(rows_extracted) as rows_extracted,
        sum(rows_rejected) as rows_rejected,
        sum(rows_loaded) as rows_loaded,
        string_agg(distinct error_type, ',' order by error_type) as ingestion_error_types,
        min(started_at) as started_at,
        max(finished_at) as finished_at
    from latest_runs
    group by pipeline_name, batch_id
),

latest_publish as (
    select distinct on (batch_id)
        batch_id,
        publish_run_id,
        status,
        error_type
    from mart_publish_runs
    where batch_id is not null
    order by batch_id, started_at desc
)

select
    batch_runs.pipeline_name,
    batch_runs.batch_id,
    batch_runs.logical_date,
    case
        when batch_runs.failed_tables > 0 or latest_publish.status = 'FAILED' then 'FAILED'
        when batch_runs.running_tables > 0
            or latest_publish.status in ('BUILDING', 'PUBLISHING') then 'RUNNING'
        when batch_runs.max_attempt_number > 1 or batch_runs.skipped_tables > 0 then 'RERUN'
        when batch_runs.empty_tables = batch_runs.table_count then 'EMPTY'
        else 'SUCCESS'
    end as run_outcome,
    batch_runs.table_count,
    batch_runs.max_attempt_number,
    batch_runs.rows_extracted,
    batch_runs.rows_rejected,
    batch_runs.rows_loaded,
    batch_runs.ingestion_error_types,
    latest_publish.publish_run_id,
    latest_publish.status as publish_status,
    latest_publish.error_type as publish_error_type,
    batch_runs.started_at,
    batch_runs.finished_at
from batch_runs
left join latest_publish using (batch_id)
order by batch_runs.started_at desc
```

- [ ] **Step 4: Run the test**

Run: `RUN_POSTGRES_INTEGRATION=1 uv run pytest tests/integration/test_observability_run_status_integration.py -v`
Expected: 1 passed.

`sum(bigint)` returns `numeric` in PostgreSQL. psycopg maps it to `Decimal`, and `Decimal(15) == 15` is true, so the tuple comparison passes. Do not cast in the SQL.

- [ ] **Step 5: Commit**

```bash
uv run ruff check tests/integration/test_observability_run_status_integration.py
git add sql/validation/observability_run_status.sql tests/integration/test_observability_run_status_integration.py
git commit  # via /caveman:caveman-commit, e.g. "feat(metadata): add observability run status query"
```

---

### Task 10: E2E gate, real dbt publish gate, clean clone script, README

**Files:**
- Modify: `tests/integration/test_order_e2e_and_late_order_mart_integration.py` (`test_fixed_order_is_traceable_from_source_to_fact`, lines 48-88)
- Create: `tests/integration/test_publish_gate_dbt_integration.py`
- Create: `scripts/verify_clean_clone.sh`
- Modify: `README.md` (PRD link at line 5, and a new section before `## 로컬 데이터와 Secret`)

**Interfaces:**
- Consumes:
  - Task 3: `run_dbt_build(build_path, target_path, *, extra_args=...)`.
  - Task 4: `publish_warehouse(settings, paths, run, *, dbt_runner)` returns `PublishOutcome(publish_run_id, previous_publish_run_id, mart_hashes, mart_row_counts, changed_relations)`. Also `WarehousePaths.under(root)` and `mart_row_counts(path)`.
  - Task 2: `PublishRun(publish_run_id, pipeline_name)`, `ensure_publish_metadata`, `assert_no_active_publish`, `get_publish_run`, `FAILED`.
  - Task 1: `WarehouseBuildError.error_type`, `DBT_TEST_ERROR`.
  - Task 8: dbt var `publish_gate_canary`.
  - Task 4 CLI: `python -m src.warehouse.publish`.
- Produces: `scripts/verify_clean_clone.sh`. It returns 0 only when every step passes. It writes a log to `$VERIFY_LOG`, default `/tmp/cdp-clean-clone-<timestamp>.log`.

- [ ] **Step 1: Extend the fixed-order E2E test (P7-18)**

In `test_fixed_order_is_traceable_from_source_to_fact`, replace the block from `with duckdb.connect(str(warehouse_path), read_only=True) as connection:` up to `assert fact_row == ...` with:

```python
        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            catalog_counts = dict(
                connection.execute(
                    "SELECT source_table, row_count FROM control.bronze_files"
                ).fetchall()
            )
            fact_row = connection.execute(
                "SELECT order_id, order_count FROM facts.fct_order WHERE order_id = ?",
                [bundle.order.order_id],
            ).fetchone()
            item_count = connection.execute(
                "SELECT count(*) FROM facts.fct_order_item WHERE order_id = ?",
                [bundle.order.order_id],
            ).fetchone()[0]
            payment_count = connection.execute(
                "SELECT count(*) FROM facts.fct_order_payment WHERE order_id = ?",
                [bundle.order.order_id],
            ).fetchone()[0]
        assert catalog_counts == {
            result.run.source_table: result.row_count for result in results
        }
        assert fact_row == (bundle.order.order_id, 1)
        assert item_count == len(bundle.items) == mutation.items_inserted
        assert payment_count == len(bundle.payments) == mutation.payments_inserted
```

The docstring already says "같은 Count로 이어진다". Keep it.

Run: `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/integration/test_order_e2e_and_late_order_mart_integration.py -v`
Expected: 2 passed.

If `item_count` differs from `len(bundle.items)`, the fact has a fan-out or drop bug. Report it to architect. Do not change the assertion.

- [ ] **Step 2: Write the real dbt publish gate test (P7-14, AC-09)**

This test runs real `dbt build` three times against all committed Bronze objects. It is opt-in: it needs `RUN_DBT_PUBLISH_INTEGRATION=1` in addition to the two container flags. Ingest at least one batch for all 9 tables first.

`tests/integration/test_publish_gate_dbt_integration.py`:

```python
"""실제 dbt Test 실패가 Published Warehouse를 바꾸지 못하는지 검증한다 (AC-09)."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from functools import partial
from pathlib import Path

import pytest

from src.common.database import PostgresSettings
from src.ingestion.errors import DBT_TEST_ERROR
from src.warehouse.dbt_runner import run_dbt_build
from src.warehouse.errors import PublishInProgressError, WarehouseBuildError
from src.warehouse.mart_hash import mart_logical_hashes, mart_row_counts
from src.warehouse.publish import WarehousePaths, publish_warehouse
from src.warehouse.publish_metadata import (
    FAILED,
    PublishRun,
    assert_no_active_publish,
    ensure_publish_metadata,
    get_publish_run,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        any(
            os.environ.get(flag) != "1"
            for flag in (
                "RUN_POSTGRES_INTEGRATION",
                "RUN_SEAWEEDFS_INTEGRATION",
                "RUN_DBT_PUBLISH_INTEGRATION",
            )
        ),
        reason="Set PostgreSQL, SeaweedFS and RUN_DBT_PUBLISH_INTEGRATION flags to run real dbt.",
    ),
]

CANARY_RUNNER = partial(run_dbt_build, extra_args=("--vars", "{publish_gate_canary: true}"))


@pytest.fixture
def settings() -> Iterator[PostgresSettings]:
    """활성 Publish가 없을 때만 실행하고 테스트 Run Row를 정리한다."""
    settings = PostgresSettings.from_environment()
    ensure_publish_metadata(settings)
    try:
        assert_no_active_publish(settings)
    except PublishInProgressError:
        pytest.skip("A real publish run is active; rerun after it finishes.")
    yield settings
    with settings.pipeline_connection() as connection:
        connection.execute(
            "DELETE FROM mart_publish_runs WHERE pipeline_name LIKE 'test_publish_%'"
        )


def _run() -> PublishRun:
    """테스트 전용 Pipeline 이름을 가진 Publish Run을 만든다."""
    return PublishRun(uuid.uuid4(), f"test_publish_{uuid.uuid4().hex[:8]}")


def test_dbt_test_failure_keeps_the_published_warehouse(settings, tmp_path: Path) -> None:
    """Publish 성공 → Canary 실패 → 재Publish에서 실패 Run은 Published Hash를 바꾸지 않는다."""
    paths = WarehousePaths.under(tmp_path)

    first = publish_warehouse(settings, paths, _run(), dbt_runner=run_dbt_build)
    assert mart_logical_hashes(paths.published) == first.mart_hashes
    assert mart_row_counts(paths.published) == first.mart_row_counts

    failing = _run()
    with pytest.raises(WarehouseBuildError) as raised:
        publish_warehouse(settings, paths, failing, dbt_runner=CANARY_RUNNER)
    assert raised.value.error_type == DBT_TEST_ERROR
    assert mart_logical_hashes(paths.published) == first.mart_hashes
    assert mart_row_counts(paths.published) == first.mart_row_counts
    failed_record = get_publish_run(settings, failing.publish_run_id)
    assert failed_record is not None
    assert (failed_record.status, failed_record.error_type) == (FAILED, DBT_TEST_ERROR)
    assert paths.failed_file(failing.publish_run_id).is_file()

    third = publish_warehouse(settings, paths, _run(), dbt_runner=run_dbt_build)
    assert third.previous_publish_run_id == first.publish_run_id
    assert third.mart_hashes == first.mart_hashes
    assert third.changed_relations == ()
```

Check before running:
- `DBT_TEST_ERROR` comes from `src.ingestion.errors`, which re-exports it from `src.warehouse.errors` (Task 1).
- `third.previous_publish_run_id == first.publish_run_id` assumes no other publish finishes during the test. The fixture skip covers active runs at start.

Run: `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 RUN_DBT_PUBLISH_INTEGRATION=1 uv run pytest tests/integration/test_publish_gate_dbt_integration.py -v`
Expected: 1 passed. It takes several minutes.

- [ ] **Step 3: Write `scripts/verify_clean_clone.sh` (P7-20)**

```bash
#!/usr/bin/env bash
# 새 Clone 경로에서 Phase 0~7 검증 명령을 처음부터 순서대로 실행하고 로그를 남긴다.
# 사전 조건: 메인 Checkout의 Compose Stack을 먼저 내린다 (같은 Host Port를 쓴다).
set -euo pipefail

source_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
clone_dir="$(mktemp -d /tmp/cdp-clean-clone-XXXXXX)"
compose_project="cdp-clean-clone"
log_file="${VERIFY_LOG:-/tmp/cdp-clean-clone-$(date -u +%Y%m%dT%H%M%SZ).log}"
tables=(
  customers customer_subscriptions customer_membership_tiers subscription_payments
  products sellers orders order_items order_payments
)

cleanup() {
  # Clone 전용 Compose Project만 내린다. 메인 Stack의 Volume은 건드리지 않는다.
  if [[ -f "$clone_dir/compose.yaml" ]]; then
    (cd "$clone_dir" && docker compose -p "$compose_project" down -v >/dev/null 2>&1) || true
  fi
  rm -rf "$clone_dir"
}
trap cleanup EXIT

step() {
  # 단계 이름을 로그에 남긴다.
  echo "==> $*"
}

exec > >(tee -a "$log_file") 2>&1
echo "clean clone verification log: $log_file"

if [[ -n "$(docker compose -p commerce-data-platform ps -q 2>/dev/null)" ]]; then
  echo "Main compose stack is running. Stop it first: docker compose down" >&2
  exit 1
fi

step "clone"
git clone --quiet "$source_root" "$clone_dir"
# Secret과 Raw Dataset은 Git 밖에 있으므로 복사만 하고 내용은 출력하지 않는다.
cp "$source_root/.env" "$clone_dir/.env"
mkdir -p "$clone_dir/data"
cp -r "$source_root/data/raw" "$clone_dir/data/raw"
cd "$clone_dir"

step "versions"
uv --version
uv run python --version
docker compose version

step "dependencies and static checks"
uv sync --frozen
uv run ruff check .
uv run pytest

step "containers"
docker compose -p "$compose_project" up -d --wait postgres seaweedfs

step "seed"
uv run python -m src.seed --seeded-at 2026-09-03T00:00:00Z

step "generator"
uv run python -m src.generator --seed 7 --logical-date 2026-09-04T00:00:00Z --orders 20 \
  --initialize-metadata

step "ingestion"
uv run python -m src.ingestion --dag-id clean_clone_verification \
  --logical-date 2026-09-04T00:00:00Z --tables "${tables[@]}"

step "publish (dbt build + tests + swap)"
uv run python -m src.warehouse.publish --pipeline-name clean_clone_verification

step "integration tests"
RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 \
  uv run pytest -m "integration and not airflow"

echo "clean clone verification passed"
```

Notes for the implementer:
- `compose.yaml` sets `name: commerce-data-platform`. `-p cdp-clean-clone` overrides it. Without `-p`, `down -v` would delete the main stack's volumes. Keep `-p` on every `docker compose` call in the clone.
- Container ports come from `.env` and match the main stack, so the main stack must be down. The script checks this and exits 1.
- Do not `cat`, `echo` or `set -x` anything from `.env`.

Run:

```bash
chmod +x scripts/verify_clean_clone.sh
docker compose down
./scripts/verify_clean_clone.sh
```

Expected: the last line is `clean clone verification passed`, exit code 0. Keep the log path from the first line for the phase-07 doc (Task 11).

- [ ] **Step 4: Update README (P7-21)**

Change line 5 to:

```markdown
현재 기준 문서는 [PRD v1.13](PRD_v1.13.md)입니다.
```

Add this section before `## 로컬 데이터와 Secret`:

````markdown
## Phase 0~7 전체 검증

새 Clone에서 Seed·Generator·Ingestion·Publish·Integration Test를 순서대로 실행합니다. 메인 Compose Stack을 먼저 내립니다.

```bash
docker compose down
./scripts/verify_clean_clone.sh
```

로그 경로는 첫 줄에 출력됩니다. `VERIFY_LOG=/path/to/log`로 바꿀 수 있습니다.

Warehouse Publish만 수동으로 실행하려면:

```bash
uv run python -m src.warehouse.publish
uv run python -m src.warehouse.publish --recover-only
```

Publish는 `data/warehouse/build/`에서 dbt build와 Test를 마친 뒤에만 `data/warehouse/warehouse.duckdb`를 교체합니다. 실패한 Build는 `data/warehouse/failed/`에 최근 3개만 남습니다.

실제 dbt로 Publish Gate를 검증하는 테스트는 별도 Flag가 필요합니다.

```bash
RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 RUN_DBT_PUBLISH_INTEGRATION=1 \
  uv run pytest tests/integration/test_publish_gate_dbt_integration.py
```
````

- [ ] **Step 5: Commit**

```bash
uv run ruff check tests/integration/test_order_e2e_and_late_order_mart_integration.py \
  tests/integration/test_publish_gate_dbt_integration.py
git add tests/integration/test_order_e2e_and_late_order_mart_integration.py \
  tests/integration/test_publish_gate_dbt_integration.py scripts/verify_clean_clone.sh README.md
git commit  # via /caveman:caveman-commit, e.g. "test(e2e): add publish gate and clean clone verification"
```

---

### Task 11: ADR 016 and Phase 7 closure docs (not committed)

**Files:**
- Create: `docs/adr/016-publish-mart-via-warehouse-file-swap.md`
- Modify: `docs/phases/phase-07-data-quality-publish.md`
- Modify: `docs/phases/ROADMAP.md` (line 11, PRD link)

**Interfaces:**
- Consumes: evidence from Task 8 Step 6 (hash before and after the canary run), Task 10 Step 2 (real dbt gate test), and Task 10 Step 3 (clean clone log path).
- Produces: documents only. Team rule: only `docs/architect-review/` is committed under `docs/`. Do not `git add` any file in this task.

- [ ] **Step 1: Write the ADR**

Follow the PRD §25 ADR format: Status, Context, Decision, Alternatives, Consequences, Validation. Fill the `Validation` rows from your own Task 8 and Task 10 runs. Write the real values, not placeholders.

```markdown
# ADR 016. Warehouse File 교체로 Mart를 Publish한다

## Status

Accepted (2026-09-18, Phase 7)

## Context

- Phase 6까지 dbt는 Published Warehouse 파일 `data/warehouse/warehouse.duckdb`에 직접 build했다.
- dbt Test가 실패해도 Model은 이미 바뀐 뒤라, 마지막 성공 Mart를 보존할 수 없었다.
- Incremental Fact와 `control.dbt_processed_batch`가 같은 파일에 있다. 실패 Build가 Incremental 경계를 전진시킬 수 있었다.
- Airflow Task는 별도 Process라 파일 Lock으로 동시 Publish를 막을 수 없다.

## Decision

- Published 파일과 Build 파일을 분리한다.
  - Build: `data/warehouse/build/{publish_run_id}.duckdb`. Published 파일을 복사하고 Bronze Catalog를 동기화한 뒤 `dbt build`를 실행한다.
  - 성공: `CHECKPOINT`, Mart Hash·Row Count 기록, `os.replace`, Directory `fsync` 순서로 교체한다.
  - 실패: Build 파일을 `data/warehouse/failed/`로 옮긴다. 최근 3개만 남긴다. Published 파일은 건드리지 않는다.
- Publish 상태는 PostgreSQL `mart_publish_runs`에 기록한다 (`BUILDING` → `PUBLISHING` → `PUBLISHED` 또는 `FAILED`).
- 활성 상태 부분 Unique Index `mart_publish_runs_single_active_idx`를 동시 Publish Mutex로 쓴다.
- 1시간 넘게 활성 상태인 Run은 다음 Publish 시작 시 복구한다. Published Hash가 기록 Hash와 같으면 `PUBLISHED`, 아니면 `FAILED(UNKNOWN_ERROR, abandoned)`로 닫는다.
- dbt 실패는 `run_results.json`으로 `DBT_BUILD_ERROR`와 `DBT_TEST_ERROR`로 나눈다. 분류할 수 없는 예외는 `UNKNOWN_ERROR`다.

## Alternatives

| 대안 | 기각 사유 |
| ---- | --------- |
| 같은 파일 안 Build Schema → Swap | DuckDB에 `ALTER SCHEMA RENAME`이 없다. Table별 Rename은 원자 단위가 아니다. Build 중 Writer Lock이 Metabase 읽기를 막는다. |
| Published 파일에 `ATTACH` 후 Mart Table만 복사 | Catalog·Control·Mart 복사 코드가 늘어난다. 복사 도중 실패하면 부분 반영이 생긴다. |
| `fcntl` 파일 Lock Mutex | Airflow Task 경계를 넘지 못한다. |

## Consequences

- 실행마다 Warehouse 파일을 한 번 복사한다. 현재 크기는 약 40MB다.
- 실패 Build는 3개까지 Disk에 남는다. 원인 분석용이다.
- Metabase의 기존 연결은 교체 전 inode를 계속 읽는다. Reload 정책은 ADR 012(Phase 10)에서 정한다.
- `rebaseline.py`는 활성 Publish가 있으면 거부한다.
- Ingestion CLI `--sync-catalog`는 Published 파일에 Catalog를 직접 쓴다. 개발용 관리 동작으로 남긴다. Airflow DAG는 이 옵션을 쓰지 않는다.
- `classify_error`의 기본값이 `CONFIGURATION_ERROR`에서 `UNKNOWN_ERROR`로 바뀌었다 (PRD v1.13 §18).

## Validation

| 근거 | 명령 | 결과 |
| ---- | ---- | ---- |
| 가짜 Runner 5단계 시나리오 | `RUN_POSTGRES_INTEGRATION=1 uv run pytest tests/integration/test_warehouse_publish_integration.py` | (Task 4 실행 결과) |
| 실제 dbt Canary 실패 전후 Hash | `uv run python -m src.warehouse.mart_hash data/warehouse/warehouse.duckdb` | (Task 8 Step 6의 diff 결과와 Canary Run ID) |
| 실제 dbt Publish Gate 테스트 | `RUN_DBT_PUBLISH_INTEGRATION=1 ... pytest tests/integration/test_publish_gate_dbt_integration.py` | (Task 10 Step 2 결과) |
| 새 Clone 재현 | `./scripts/verify_clean_clone.sh` | (Task 10 Step 3 로그 경로와 마지막 줄) |
```

Replace each parenthesized cell in `Validation` with the real output, for example `1 passed in 212.4s`. Do not leave a parenthesized cell.

- [ ] **Step 2: Update the phase-07 doc**

1. Header:
   - `> 상태: Planned  ` becomes `> 상태: Done  `.
   - `[PRD v1.11](../../PRD_v1.11.md)` becomes `[PRD v1.13](../../PRD_v1.13.md)`.
2. Check `[x]` on every `P7-01` to `P7-21` task and every Definition of Done item that your runs proved. Leave an item unchecked if its evidence failed, and report it to architect.
3. Under `## Portfolio Evidence`, add the Corruption Matrix result (5 cases, `error_counts` from Task 7) and the canary hash diff from Task 8.
4. Add this section before `## 다음 Phase 인계`. Keep the phase-04 table format: path, change kind, content.

```markdown
## 파일·폴더별 변경 요약

| 경로 | 구분 | 변경 내용 |
| ---- | ---- | --------- |
| `src/warehouse/errors.py` | 생성 | Warehouse 오류 종류와 `WarehouseBuildError`·`PublishedWalError`·`PublishInProgressError`·`PublishStateError`를 정의했다. |
| `src/ingestion/errors.py` | 수정 | Warehouse 오류 분류를 추가하고 기본값을 `UNKNOWN_ERROR`로 바꿨다. |
| `sql/metadata/005_create_mart_publish_runs.sql` | 생성 | Publish Run 상태·Hash·Row Count Table과 활성 Run Mutex Index를 추가했다. |
| `src/warehouse/publish_metadata.py` | 생성 | Publish Run 상태 전이와 활성 Run 조회를 구현했다. |
| `src/warehouse/dbt_runner.py` | 생성 | Build 파일 대상 `dbt build` 실행과 `run_results.json` 실패 분류를 구현했다. |
| `src/warehouse/mart_hash.py` | 수정 | Mart Row Count 조회를 추가했다. |
| `src/warehouse/publish.py` | 생성 | Build → 검증 → 원자 교체 Publish, 실패 Build 격리, 복구, CLI를 구현했다. |
| `airflow/dags/warehouse_pipeline_dag.py` | 수정 | dbt 직접 실행을 `prepare_warehouse_build` → `dbt_build` → `publish_mart` Task로 바꿨다. |
| `src/rebaseline.py` | 수정 | 활성 Publish가 있으면 Rebaseline을 거부한다. |
| `src/ingestion/rules.py` | 생성 | PRD §11 순서의 Validation Rule Registry를 추가했다. |
| `src/ingestion/validation.py` | 수정 | 오류 Code 문자열을 Registry 상수로 바꿨다. |
| `src/ingestion/corruption.py` | 수정 | `DUPLICATE_PRIMARY_KEY` Corruption과 Page 단위 적용을 추가했다. |
| `src/ingestion/service.py` | 수정 | Corruption을 Page 단위로 적용한다. |
| `dbt/tests/generic/non_negative.sql` | 생성 | 금액 음수 금지 Generic Test를 추가했다. |
| `dbt/tests/stg_orders_timestamp_order.sql` | 생성 | 주문 시각 순서 Test를 추가했다. |
| `dbt/tests/fct_order_item_total_matches_order.sql` | 생성 | 주문 금액과 Line 합계 일치 Test를 추가했다. |
| `dbt/tests/publish_gate_canary.sql` | 생성 | Publish Gate 검증용 Canary Test를 추가했다. |
| `dbt/models/marts/facts/schema.yml` | 수정 | 상태 도메인·FK·금액 범위 Test를 추가했다. |
| `sql/validation/observability_run_status.sql` | 생성 | Batch별 Ingestion·Publish 상태 조회 SQL을 추가했다. |
| `scripts/verify_clean_clone.sh` | 생성 | 새 Clone 전체 검증 Script를 추가했다. |
| `README.md` | 수정 | PRD v1.13 링크와 Phase 0~7 검증 절차를 추가했다. |
| `tests/` | 생성·수정 | 위 변경의 단위·통합 테스트를 추가했다. |
| `PRD_v1.13.md` | 생성 | §12 `mart_publish_runs`, §13.2 Publish Task Graph, §18 `UNKNOWN_ERROR`를 반영했다. 이전 Version은 `PRD.bak/`로 옮겼다. |
| `docs/adr/016-publish-mart-via-warehouse-file-swap.md` | 생성 | Publish 전략 결정과 관측 근거를 기록했다. |
```

- [ ] **Step 3: Update ROADMAP**

In `docs/phases/ROADMAP.md` line 11, change `[PRD v1.11](../../PRD_v1.11.md)` to `[PRD v1.13](../../PRD_v1.13.md)`.

- [ ] **Step 4: Confirm nothing under `docs/` is staged**

Run: `git status --short docs/`
Expected: only untracked or modified entries (`??` or ` M`). No staged entries (`A ` or `M `).

---

## Execution Order

- Tasks 1 → 2 → 3 → 4 → 5 are sequential. Each one uses names from the one before it.
- Tasks 6, 7, 8 and 9 are independent of each other. Task 7 needs Task 6. Task 8 needs Task 4. Task 9 needs Task 2.
- Task 10 needs Tasks 4 and 8. Task 11 runs last.
