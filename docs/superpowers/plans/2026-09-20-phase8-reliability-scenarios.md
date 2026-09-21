# Phase 8 Reliability Scenarios Implementation Plan

> **For agentic workers:** Implement this plan one task at a time, in order. Steps use checkbox (`- [ ]`) syntax for tracking. After each task, report the result to architect. Do not use subagent or worktree execution.

**Goal:** Break the Phase 0–7 invariants on purpose and prove each failure is reproducible, detectable, recoverable and re-verifiable. Ship a reliability harness, the missing scenario tests, and one runbook per scenario.

**Architecture:**
- Fault injection lives only in `tests/reliability/faults.py`. It monkeypatches existing module-level functions. No fault flag enters `src/`.
- `tests/reliability/harness.py` collects one `PlatformState` snapshot from existing sources: the observability SQL, S3 object keys, orphan candidates, publish runs, mart hashes and row counts.
- Each scenario is one pytest test. It snapshots state, injects the fault, snapshots again, asserts the invariants, and writes evidence JSON.
- Evidence goes to `data/reliability/{scenario_id}/{utc_timestamp}.json` (gitignored). Docs carry only summary tables.

**Tech Stack:** Python 3.12, DuckDB 1.5.5, dbt-duckdb 1.11 / dbt-core 1.12.3, psycopg 3, SeaweedFS S3 (boto3), pytest, uv, ruff (line length 100).

**Spec:** `docs/superpowers/specs/2026-09-20-phase8-reliability-scenarios-design.md` (PRD: `PRD_v1.13.md` §8, §9, §10, §16, §18)

## Global Constraints

- Put a short docstring directly below every new class and function. Write it in Korean unless it must be English (code, identifier, SQL).
- Write non-ASCII strings as literal UTF-8. Never use `\uXXXX` escapes.
- Run `uv run ruff check .` before each commit and keep it green. `line-length = 100` is the formatter target, not a lint rule: the repo's default rule set has no `E501`, and Phase 3–7 files already carry longer lines. Keep new lines near 100 where it reads better, but a long line is not a review blocker.
- Never print, cat, or commit `.env`. Read credentials only through `PostgresSettings.from_environment()` and `SeaweedFSSettings.from_environment()`.
- Every reliability test carries `pytestmark = [pytest.mark.integration, pytest.mark.reliability, pytest.mark.skipif(...)]` and skips unless `RUN_POSTGRES_INTEGRATION=1` (plus `RUN_SEAWEEDFS_INTEGRATION=1` when S3 is touched, plus `RUN_DBT_PUBLISH_INTEGRATION=1` when dbt runs). Do not invent new environment flags.
- Do not add fault-injection branches, flags or hooks to any file under `src/`. If a seam is missing, stop and ask architect before changing `src/`.
- Each scenario uses its own `batch_id` prefix (`r01-`, `r02-`, …) and cleans its metadata rows in a fixture, the same way `tests/integration/test_publish_gate_dbt_integration.py` does.
- Documents record `batch_id`, `run_id`, `table_batch_id`, `publish_run_id`, `reprocess_id` only. No credentials, no raw payloads, no absolute paths.
- Commit messages: use `/caveman:caveman-commit`. End each message with `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>`.
- Commit code, SQL, tests, scripts and README. **Do not commit anything under `docs/`** except `docs/architect-review/`.
- Run shell commands from the project root `/home/kang/projects/commerce-data-platform`.

## File Structure

| Path | Kind | Responsibility |
|---|---|---|
| `tests/reliability/__init__.py` | Create | Package marker |
| `tests/reliability/harness.py` | Create | `PlatformState`, `collect_state`, comparison helpers, evidence writer |
| `tests/reliability/faults.py` | Create | Fault injection context managers |
| `tests/reliability/test_r01_r07_ingestion_commit.py` | Create | R-01 … R-07 |
| `tests/reliability/test_r08_r10_late_and_history.py` | Create | R-08 … R-10 |
| `tests/reliability/test_r11_r15_reprocess_and_source.py` | Create | R-11 … R-15 |
| `pyproject.toml` | Modify | Register the `reliability` marker |
| `.gitignore` | Modify | Ignore `data/reliability/` |
| `README.md` | Modify | How to run the reliability suite |
| `docs/runbooks/*.md` | Create | One runbook per scenario + README index (not committed) |
| `docs/troubleshooting/*.md` | Create | Error-code reverse index (not committed) |
| `docs/phases/phase-08-reliability.md`, `docs/phases/ROADMAP.md` | Modify | R-11 definition, task checkoffs (not committed) |

---

### Task 1: Harness state collector

**Files:**
- Create: `tests/reliability/__init__.py`, `tests/reliability/harness.py`
- Modify: `pyproject.toml`, `.gitignore`
- Test: `tests/reliability/test_r01_r07_ingestion_commit.py` is not needed yet; verify by importing in a throwaway pytest run of Task 4.

**Interfaces:**
- Produces: `PlatformState` frozen dataclass with fields `run_rows: tuple[dict, ...]`, `watermarks: dict[str, str]`, `object_keys: tuple[str, ...]`, `orphan_candidates: tuple[str, ...]`, `publish_runs: tuple[dict, ...]`, `mart_hashes: dict[str, str]`, `mart_row_counts: dict[str, int]`.
- Produces: `collect_state(postgres, storage, *, warehouse_path: Path | None, marts: Sequence[str], object_prefix: str = "", publish_run_ids: Sequence[UUID] = ()) -> PlatformState`. When `warehouse_path` is `None` or missing, mart fields stay empty. `publish_run_ids` is unioned with the ids the observability SQL exposes, because `mart_publish_runs.batch_id` is nullable and the SQL drops rows where it is `NULL`.
- Produces: `assert_unchanged(before, after, fields: Sequence[str])` and `diff_state(before, after) -> dict[str, tuple[object, object]]`.
- Produces: `write_evidence(scenario_id: str, payload: Mapping[str, object]) -> Path` writing `data/reliability/{scenario_id}/{UTC ISO-8601 compact}.json`.

**Steps:**
- [x] Read `sql/validation/observability_run_status.sql` and reuse it verbatim for `run_rows`. Do not write new SQL.
- [x] Reuse `src.ingestion.storage.list_object_keys`, `src.ingestion.orphan.find_orphan_candidates`, `src.warehouse.publish_metadata.get_publish_run`, `src.warehouse.mart_hash.mart_logical_hashes` and `mart_row_counts`.
- [x] Make `collect_state` read-only. It must not create, migrate or mutate anything.
- [x] Register `reliability` in `pyproject.toml` markers; add `data/reliability/` to `.gitignore`.

**Verify:**
- [x] `uv run ruff check .`
- [x] `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run python -c "from tests.reliability.harness import collect_state"`

---

### Task 2: Fault injection context managers

**Files:**
- Create: `tests/reliability/faults.py`

**Interfaces:**
- Produces: `fail_object_upload(monkeypatch, *, on_call: int = 1)` — raises a botocore `ClientError` from `src.ingestion.storage.upload_new_file` on the nth call.
- Produces: `fail_metadata_commit(monkeypatch)` — raises after the bronze object is uploaded and verified, before the metadata transaction commits. The platform classifies the raised `RuntimeError` as `UNKNOWN_ERROR`; scenarios assert that code.
- Produces: `fail_source_connection(monkeypatch)` — raises `psycopg.OperationalError` when the extractor opens its source connection.
- All three are `@contextmanager` helpers that restore the patched attribute on exit. `fail_source_connection` patches the shared `PostgresSettings` class, so its window must not cover fixture setup, verification or the clean retry.
- Produces: `corrupt_manifest(storage, key, *, kind)` with `kind in {"CHECKSUM", "ROW_RANGE", "SCHEMA_VERSION"}` — rewrites the manifest object bytes in place.

**Steps:**
- [x] Read `src/ingestion/metadata.py` and `src/ingestion/extract.py` and pick the narrowest existing function boundary for `fail_metadata_commit` and `fail_source_connection`.
- [ ] If no boundary works without editing `src/`, **stop and report to architect** with the two candidate seams. Do not add a hook yourself.
- [x] Each helper raises the error type the platform already classifies. Do not introduce new exception classes.
- [x] `corrupt_manifest` must keep the object key and only change the payload, so the checksum check is what fails.

**Verify:**
- [x] `uv run ruff check .`
- [x] `uv run pytest tests/reliability -q --collect-only`

---

### Task 3: Runbook and troubleshooting templates

**Files:**
- Create: `docs/runbooks/README.md`, `docs/runbooks/_template.md`, `docs/troubleshooting/README.md`

**Steps:**
- [x] `_template.md` uses the six-part order: 문제 → 재현 조건과 명령 → 기대/실제 관측 → 원인과 불변 조건 → 복구 절차 → 재검증 명령과 결과.
- [x] Each template section states which identifiers must appear (`batch_id`, `run_id`, `table_batch_id`, `publish_run_id`, `reprocess_id` as applicable).
- [x] `docs/runbooks/README.md` holds the R-01…R-15 index table with links and shared preconditions (compose up, environment flags).
- [x] `docs/troubleshooting/README.md` holds the error-code reverse index with empty rows to fill as scenarios land.
- [x] Do not commit these files.

**Verify:**
- [x] Template renders with no absolute path and no credential placeholder.

---

### Task 4: R-01 Duplicate Batch and R-06 Orphan Object evidence

**Files:**
- Create: `tests/reliability/test_r01_r07_ingestion_commit.py`
- Create: `docs/runbooks/r01-duplicate-batch.md`, `docs/runbooks/r06-orphan-object.md`

**Interfaces:**
- Produces: `test_r01_duplicate_batch_is_idempotent_across_three_runs`, `test_r06_orphan_reconciliation_commits_a_match_and_rejects_a_mismatch`.

**Steps:**
- [ ] R-01: run the same standard batch three times. Assert object key count, committed row count and bronze object checksum are identical after runs 2 and 3, and the watermark never moves twice.
- [ ] R-06: reuse the reconciliation paths already proven in `tests/integration/test_orders_ingestion_service_integration.py`. Capture the accept case and the quarantine-object reject case in one evidence file.
- [ ] Write evidence for both and fill the two runbooks from it.

**Verify:**
- [ ] `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability/test_r01_r07_ingestion_commit.py -v`

---

### Task 5: R-02 Upload Failure

**Files:**
- Modify: `tests/reliability/test_r01_r07_ingestion_commit.py`
- Create: `docs/runbooks/r02-upload-failure.md`

**Interfaces:**
- Produces: `test_r02_upload_failure_keeps_the_watermark_and_commits_nothing`.

**Steps:**
- [ ] Inject `fail_object_upload` during a batch that would otherwise commit rows.
- [ ] Assert: the run row ends `FAILED`, no `COMMITTED` metadata exists for that `table_batch_id`, the watermark equals the before value, and no new final object key appears.
- [ ] Assert the observed error type is `UNKNOWN_ERROR`. `classify_error` has no branch for botocore `ClientError`, so every object-storage failure lands there. Record this in the runbook as an observed gap: a retryable 503 is reported as a non-retryable type. Do not change `src/ingestion/errors.py` in this task; architect rules on it from the evidence.
- [ ] Re-run the same batch without the fault and assert it commits normally with the same row count as the clean baseline.

**Verify:**
- [ ] `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability/test_r01_r07_ingestion_commit.py -k r02 -v`

---

### Task 6: R-03 Metadata Failure

**Files:**
- Modify: `tests/reliability/test_r01_r07_ingestion_commit.py`
- Create: `docs/runbooks/r03-metadata-failure.md`

**Interfaces:**
- Produces: `test_r03_a_commit_crash_leaves_a_reconcilable_orphan` (crash variant).
- Produces: `test_r03_a_handled_commit_failure_refuses_automatic_reconciliation` (handled variant).

R-03 has two distinct shapes and both are in scope. `src/ingestion/service.py` catches `Exception` and calls `record_failed_run`, while `reconcile_orphan` only accepts a `RUNNING` run. So the outcome depends on whether the process survived the failure.

| Variant | Injection | Run status after | Recovery |
|---|---|---|---|
| Crash | `BaseException` subclass raised at the commit boundary, like Phase 7's `_CrashAfterFinalObject` | stays `RUNNING` | `reconcile_orphan` commits the same object key |
| Handled | `fail_metadata_commit` (`RuntimeError`) | `FAILED`, `error_type` = `RuntimeError` (아래 참고) | `reconcile_orphan` refuses: `Only a crash-interrupted RUNNING pipeline run can be reconciled`; manual handling |

Handled variant의 `error_type`은 계약 코드가 아니라 Python 예외 클래스 이름 `RuntimeError`가 저장된다. `classify_error(error)`는 `UNKNOWN_ERROR`를 반환하지만 Ingestion 실패 경로가 그 함수를 호출하지 않기 때문이다. 근거와 판정은 [014_ingestion-error-type-vocabulary.md](../../architect-review/014_ingestion-error-type-vocabulary.md)에 있다. 이 Task는 저장된 실측값을 그대로 단언하고, 증적에 `stored_error_type`·`classified`·`exception`을 각각 실측값으로 남긴다. 계약 코드로의 교정은 Task 13A가 한다.

**Steps:**
- [ ] Crash variant: abort at the commit boundary with a `BaseException` subclass so `record_failed_run` never runs. Assert the VERIFIED object exists in S3, `find_orphan_candidates` lists it, the bronze catalog read for that batch returns 0 rows, and the watermark is unchanged. Then reconcile and assert the batch reaches `COMMITTED` with the same object key (no re-upload).
- [ ] Handled variant: inject `fail_metadata_commit`. Assert the run is `FAILED` with `UNKNOWN_ERROR`, the object is still an orphan candidate, catalog read is 0, the watermark is unchanged, and `reconcile_orphan` raises `OrphanReconciliationError` with the RUNNING-only message.
- [ ] Do not change `reconcile_orphan` to accept `FAILED` runs. The refusal is the designed behavior: a handled failure has no proof the run was interrupted mid-commit.

**Verify:**
- [ ] `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability/test_r01_r07_ingestion_commit.py -k r03 -v`

---

### Task 7: R-04 CAS Conflict and R-05 Expired Lease

**Files:**
- Modify: `tests/reliability/test_r01_r07_ingestion_commit.py`
- Create: `docs/runbooks/r04-watermark-cas-conflict.md`, `docs/runbooks/r05-expired-lease.md`

**Interfaces:**
- Produces: `test_r04_only_the_cas_winner_commits`, `test_r05_an_expired_lease_owner_cannot_commit`.

**Steps:**
- [x] R-04: drive two runs against the same table and watermark. Assert exactly one commits, the loser rolls back its object and success state, and the final watermark equals the winner's upper bound.
- [x] R-05: let a table lease expire, have a second owner take it, then make the stale owner attempt a commit. Assert the stale commit is fenced and metadata is untouched.
- [x] Record the timeline (acquire, expire, takeover, fenced commit) in the evidence payload; the runbooks show it as a table.
- [x] Note in `r05` that AC-21 (generator vs. warehouse source lock) is covered as a variant of this scenario, per the design decision.

**Verify:**
- [x] `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability/test_r01_r07_ingestion_commit.py -k "r04 or r05" -v`

---

### Task 8: R-07 Broken Manifest

**Files:**
- Modify: `tests/reliability/test_r01_r07_ingestion_commit.py`
- Create: `docs/runbooks/r07-broken-manifest.md`

**Interfaces:**
- Produces: `test_r07_broken_manifest_variants_block_automatic_commit` parametrized over `CHECKSUM`, `ROW_RANGE`, `SCHEMA_VERSION`.

**Steps:**
- [ ] For each variant: corrupt the manifest of an unreconciled VERIFIED object, then run orphan reconciliation.
- [ ] Assert reconciliation refuses with the variant's own exception, and no `COMMITTED` row or watermark move happens:

| Variant | Injection | Expected exception | Expected message |
|---|---|---|---|
| `CHECKSUM` | corrupt `content_sha256` | `OrphanReconciliationError` | `Object HEAD or checksum differs from the VERIFIED manifest` |
| `ROW_RANGE` | corrupt `extract_upper_bound` | `OrphanReconciliationError` | `Pipeline run range differs from the orphan manifest` |
| `SCHEMA_VERSION` | set `schema_version` to `4` | `SourceContractError` (from `src.ingestion.schema`) | `SOURCE_CONTRACT_ERROR: unsupported schema_version=4` |
- [ ] The `SCHEMA_VERSION` variant covers AC-24; state that mapping in the runbook, along with the blocking point (`assert_supported_schema_version`) and the error code `SOURCE_CONTRACT_ERROR`.
- [ ] Import `SourceContractError` from `src.ingestion.schema`, not `src.ingestion.validation` (a different class shares the name).
- [ ] Do not change `src/ingestion/orphan.py`. See [architect review 015](../../architect-review/015_r07-schema-version-variant.md).

**Verify:**
- [ ] `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability/test_r01_r07_ingestion_commit.py -k r07 -v`
- [ ] `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability/test_r01_r07_ingestion_commit.py -v` (whole file green)

---

### Task 7A: commit_table_run lease fencing fix

근거: [docs/architect-review/016_commit-table-run-lease-fencing.md](../../architect-review/016_commit-table-run-lease-fencing.md). Task 8 완료 뒤에 착수한다. R-05 증적 JSON과 Runbook의 '알려진 위험' 절은 수정 전 동작 기록이므로 지우지 않는다.

**Files:**
- Modify: `src/ingestion/metadata.py`, `src/ingestion/service.py`
- Modify: `tests/reliability/test_r01_r07_ingestion_commit.py`, `docs/runbooks/r05-expired-lease.md`
- Create: `docs/adr/0XX-commit-lease-fencing.md` (번호는 `docs/adr/` 최대값 + 1)

**Interfaces:**
- `TableCommit`에 `lease_owner: uuid.UUID` 필수 필드가 생긴다. 기본값을 주지 않는다.
- `commit_table_run`은 Lease 상실에 `TableLeaseOwnershipLostError("Table lease ownership was lost before metadata commit")`를, Version 충돌에 기존 `WatermarkConflictError`를 던진다. 두 오류를 합치지 않는다.

**Steps:**
- [ ] `commit_table_run`의 Transaction 맨 앞에 `SELECT lease_owner, lease_expires_at FROM watermarks WHERE pipeline_name = %s AND source_table = %s FOR UPDATE`를 넣는다.
- [ ] `lease_owner != commit.lease_owner` 또는 `lease_expires_at <= current_time`이면 `TableLeaseOwnershipLostError`를 던진다. Row가 없으면 같은 예외다.
- [ ] 기존 Watermark Version CAS는 조건을 바꾸지 않는다. `lease_owner`를 CAS의 `WHERE`에 합치지 않는다 — R-04가 기대하는 `WatermarkConflictError`와 구분이 사라진다.
- [ ] `TableCommit`에 `lease_owner`를 추가하고 `src/ingestion/service.py:308` 호출에 `table_lease.owner_id`를 넘긴다.
- [ ] `src/`에 Fault 주입 분기를 넣지 않는다.
- [ ] R-05의 `commit_without_lease_check` 실측 단언을 `SUCCEEDED`에서 `pytest.raises(TableLeaseOwnershipLostError)`로 바꾸고, `classify_error` 결과가 `LEASE_OWNERSHIP_LOST`인지 단언한다. Object 0건·Watermark version 불변도 함께 단언한다.
- [ ] 나머지 `TableCommit(` 호출 3곳에 `lease_owner`를 채운다. R-04 CAS 충돌 테스트는 유효한 Lease를 쥔 채 Version만 어긋나게 해서 `WatermarkConflictError`가 그대로 나오는지 확인한다.
- [ ] `docs/runbooks/r05-expired-lease.md`의 '알려진 위험' 절에 수정 완료 상태와 ADR 링크를 덧붙인다. 실측값은 지우지 않는다.
- [ ] ADR에 문제·결정·대안·영향을 적는다. 대안에는 "Lease 조건을 Version CAS의 WHERE에 합친다"를 포함하고 오류 구분 불능을 기각 사유로 적는다.

**Verify:**
- [ ] `uv run ruff check .`
- [ ] `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability/test_r01_r07_ingestion_commit.py -v` (파일 전체 green)
- [ ] `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest -q` — 기존 Ingestion 정상 경로가 그대로 통과한다.

---

### Task 9: R-15 Source Connection Failure and R-11 Missing Schedule

**Files:**
- Create: `tests/reliability/test_r11_r15_reprocess_and_source.py`
- Create: `docs/runbooks/r15-source-connection-failure.md`, `docs/runbooks/r11-missing-schedule.md`

**Interfaces:**
- Produces: `test_r15_source_connection_failure_creates_no_object_and_holds_the_watermark`, `test_r11_a_skipped_window_self_heals_and_rewind_only_restores_attribution`.

R-11 재구성 근거: [docs/architect-review/018_r11-missing-schedule-premise.md](../../architect-review/018_r11-missing-schedule-premise.md). 이 시스템에서 누락된 창은 Gap을 만들지 않는다. `extract_upper_bound`가 Source 최대 Cursor이고 Watermark가 연속이므로 다음 실행이 건너뛴 구간을 함께 쓸어 담는다.

**Steps:**
- [ ] R-15: inject `fail_source_connection`. Assert no object key is created, the watermark holds, the run is classified retryable, and an immediate retry without the fault succeeds.
- [ ] R-11: run window A, insert window B data but skip its ingest, insert window C data and run window C. Do not touch the watermark — `_set_watermark` is forbidden in this test.
- [ ] Assert no gap: C's `rows_extracted` covers the B rows, and C's `watermark_after` equals the source maximum cursor.
- [ ] Assert the mart hashes equal a full-refresh control that never skipped a window, **before** any recovery runs.
- [ ] Assert the one real consequence: the B rows live under `ingestion_date={C's logical_date}` / `batch_id={C's batch_id}`.
- [ ] Recovery restores attribution, not data: `rewind_tables` to the boundary before B, then an explicit batch with B's `logical_date`. The re-ingest sweeping C as well is expected; do not pin the watermark to narrow it.
- [ ] Assert after recovery: mart hashes still equal the control (idempotent), and the B rows now sit in B's `ingestion_date` partition.
- [ ] Both runbooks state that neither DAG uses `catchup`, so the recovery path is the explicit batch, not the scheduler.
- [ ] `docs/runbooks/r11-missing-schedule.md` leads with "a missed schedule causes no data loss" and its reason, then the attribution shift and the rewind procedure.

**Verify:**
- [ ] `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 RUN_DBT_PUBLISH_INTEGRATION=1 uv run pytest tests/reliability/test_r11_r15_reprocess_and_source.py -v`

---

### Task 10: R-12 Backfill Replay and R-13 Re-extract

**Files:**
- Modify: `tests/reliability/test_r11_r15_reprocess_and_source.py`
- Create: `docs/runbooks/r12-backfill-replay.md`, `docs/runbooks/r13-re-extract.md`

**Interfaces:**
- Produces: `test_r12_replay_matches_a_full_refresh_of_the_same_range`, `test_r13_re_extract_records_a_new_batch_id_and_keeps_prior_objects`.

**Steps:**
- [ ] R-12: replay from COMMITTED bronze with no source read. Assert source read count is 0 and per-key values plus logical hashes match a full refresh of the same range (AC-07). Reuse the comparison helpers in `tests/integration/test_incremental_full_refresh_hash_integration.py`.
- [ ] R-13: re-extract an explicit range after a rewind. The new identity is a new `batch_id` — `bronze_objects` has no `reprocess_id` column (`docs/architecture/08-bronze-replay-and-reextract-boundary.md` D-5). Assert the new `batch_id` is recorded and the prior committed objects stay byte-identical.
- [ ] The `r13` runbook states the limit: the source's current state may differ from the past snapshot, so re-extract is not hash-equal to replay by definition. Replay stays the default.

**Verify:**
- [ ] `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 RUN_DBT_PUBLISH_INTEGRATION=1 uv run pytest tests/reliability/test_r11_r15_reprocess_and_source.py -v`

---

### Task 11: R-08 Late Order and R-09 Late Payment

**Files:**
- Create: `tests/reliability/test_r08_r10_late_and_history.py`
- Create: `docs/runbooks/r08-late-order.md`, `docs/runbooks/r09-late-payment.md`

**Interfaces:**
- Produces: `test_r08_a_late_order_updates_the_past_business_date_mart_once`, `test_r09_a_late_payment_pulls_the_linked_order_purchase_date_into_the_affected_range`.

**Steps:**
- [ ] R-08: capture the mart hash and row count before and after the late order lands. Assert the past business date changes, the record is collected once by the mutation cursor, and no duplicate fact row appears.
- [ ] R-09: insert a payment whose linked order purchase date is older than the current window. Assert the affected-key range includes that purchase date and the fact measures reconcile after the rebuild.
- [ ] Record the before/after mart diff (relation, row count, hash prefix) in the evidence and the runbooks.

**Verify:**
- [ ] `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 RUN_DBT_PUBLISH_INTEGRATION=1 uv run pytest tests/reliability/test_r08_r10_late_and_history.py -k "r08 or r09" -v`

---

### Task 12: R-10 Customer history change

**Files:**
- Modify: `tests/reliability/test_r08_r10_late_and_history.py`
- Create: `docs/runbooks/r10-customer-history-change.md`

**Interfaces:**
- Produces: `test_r10_a_subscription_or_tier_change_opens_a_new_version_and_rebinds_events`.

**Steps:**
- [ ] Drive one subscription state change and one tier change through the generator transition path.
- [ ] Assert: a new SCD2 version opens, the prior version closes with no overlap, exactly one current version remains, and events after the change bind to the new version key while earlier events keep the old one.
- [ ] Assert the existing `dbt/tests/dim_customer_*` **and** `dbt/tests/dim_subscription_*` singular tests still pass in the same publish run (AC-09, AC-10). The scenario drives a subscription transition, so the subscription guards must be shown to have run, not merely not failed.

**Verify:**
- [ ] `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 RUN_DBT_PUBLISH_INTEGRATION=1 uv run pytest tests/reliability/test_r08_r10_late_and_history.py -v`

---

### Task 13: R-14 dbt Failure evidence

**Files:**
- Modify: `tests/reliability/test_r08_r10_late_and_history.py` (or a small `test_r14_*` module if the file grows past ~400 lines)
- Create: `docs/runbooks/r14-dbt-failure.md`, `docs/troubleshooting/warehouse-publish.md`

**Interfaces:**
- Produces: `test_r14_a_failed_build_holds_bronze_watermark_and_the_published_mart`.

**Steps:**
- [ ] Trigger the existing dbt canary failure path used by `tests/integration/test_publish_gate_dbt_integration.py`.
- [ ] Assert the published warehouse hash and row counts are byte-identical before and after, the failed build is isolated under `failed/`, bronze and watermark are unchanged, and the run row is `FAILED` with `DBT_TEST_ERROR`.
- [ ] Re-run clean and assert a new `PUBLISHED` run chains to the previous success.
- [ ] `warehouse-publish.md` maps `DBT_BUILD_ERROR`, `DBT_TEST_ERROR`, `UNKNOWN_ERROR` (abandoned run), `PublishInProgressError` and `PublishedWalError` to cause and action.

**Verify:**
- [ ] `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 RUN_DBT_PUBLISH_INTEGRATION=1 uv run pytest tests/reliability -k r14 -v`

---

### Task 13A: Ingestion error type classification fix

근거: [docs/architect-review/014_ingestion-error-type-vocabulary.md](../../architect-review/014_ingestion-error-type-vocabulary.md), [019_task9-r15-r11-verification.md](../../architect-review/019_task9-r15-r11-verification.md). 이 Task는 R-02·R-03·R-15 증적이 모두 확보된 뒤에 착수한다. Task 5·6·9는 수정 전 동작을 그대로 단언하고, 이 Task에서 그 단언을 계약 코드로 갱신한다.

**Files:**
- Modify: `src/ingestion/errors.py`, `src/ingestion/service.py`
- Modify: `tests/reliability/test_r02_*.py`, `test_r03_*.py`, `test_r15_*.py`, `docs/troubleshooting/ingestion-errors.md`
- Create: `docs/adr/0XX-ingestion-error-type-classification.md` (번호는 `docs/adr/` 최대값 + 1)

**Interfaces:**
- `classify_error`는 Object Storage 장애에 계약 코드를 반환한다. 코드 이름과 재시도 가능 여부는 R-02 증적의 실제 예외·상태 코드를 보고 정한다.
- `pipeline_runs.error_type`은 `errors.py`가 정의한 코드만 담는다. Python 예외 클래스 이름은 더 이상 들어가지 않는다.

**Steps:**
- [ ] R-02·R-03·R-15 증적 JSON에서 기록된 `error_type`과 예외 타입을 모아 표로 정리한다.
- [ ] `_record_failure_without_masking`과 `_record_lease_failure`가 `classify_error`를 거치도록 바꾼다. `_record_lease_failure`의 `SOURCE_MUTATION_CONFLICT` 분기는 `classify_error` 안으로 옮긴다.
- [ ] `classify_error`에 Object Storage 장애 분기를 추가하고 `RETRYABLE_ERROR_TYPES`를 갱신한다.
- [ ] `src/` 어디에도 Fault 주입 분기를 넣지 않는다. 이 Task도 예외가 아니다.
- [ ] 세 reliability 테스트의 `error_type` 단언을 계약 코드로 바꾼다. 예외 클래스 이름 단언은 남기지 않는다.
- [ ] ADR에 문제·결정·대안·영향을 적는다. 대안에는 "관측 컬럼을 둘로 나눈다"를 포함하고 기각 사유를 적는다.
- [ ] `docs/troubleshooting/ingestion-errors.md`의 R-02·R-03·R-15 행을 새 코드로 갱신한다.

**Verify:**
- [ ] `uv run ruff check .`
- [ ] `uv run pytest -q` — Warehouse Publish 경로의 기존 `error_type` 단언이 그대로 통과한다.
- [ ] `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability -k "r02 or r03 or r15" -v`
- [ ] Source 연결 실패가 `pipeline_runs`에 흔적을 남기지 않는 사각을 함께 고친다(019). `open_table_snapshot`이 `record_started_run`보다 앞서 연결을 열기 때문이다. `extract_upper_bound=None`으로 먼저 기록 후 갱신할지, 연결 실패 전용 기록 경로를 둘지는 R-02·R-03·R-15 증적을 함께 보고 이 Task에서 정한다.
- [ ] R-15 Test의 `run_count == 0` 단언을 새 동작으로 갱신한다.
- [ ] `docs/runbooks/r15-source-connection-failure.md`의 '알려진 한계' 줄을 수정 완료 상태로 갱신한다. 실측 표는 그대로 둔다.
- [ ] `grep -rn "error_type=" --include=*.py src/ | grep -v classify_error`로 `classify_error`를 우회해 임의 문자열을 쓰는 경로가 없는지 확인한다. `pipeline_runs` 조회는 근거로 쓰지 않는다 — Test가 자기 행을 지우므로 테이블이 비어 있고, 빈 테이블은 어휘 주장을 지지하지 않는다(판정 025).

---

### Task 14: Troubleshooting index and README

**Files:**
- Create: `docs/troubleshooting/ingestion-errors.md`, `docs/troubleshooting/commit-and-lease.md`
- Modify: `docs/troubleshooting/README.md`, `docs/runbooks/README.md`, `README.md`

**Steps:**
- [ ] `ingestion-errors.md`: every error code raised by `src/ingestion/rules.py` and `validation.py` that a scenario produced, with cause and action.
- [ ] `commit-and-lease.md`: CAS conflict, expired lease, orphan candidate, broken manifest.
- [ ] Fill the runbook index table with the evidence identifiers from Tasks 4–13.
- [ ] `README.md` gets one section: how to run the reliability suite and which flags it needs. This is the only committed doc change in this task.

**Verify:**
- [ ] Every runbook link in the index resolves.
- [ ] No absolute path, credential or raw payload appears in `docs/runbooks/` or `docs/troubleshooting/`.

---

### Task 15: Full suite, phase docs and closeout

**Files:**
- Modify: `docs/phases/phase-08-reliability.md`, `docs/phases/ROADMAP.md`

**Steps:**
- [ ] Run the full suite and record the counts.
- [ ] Check off `P8-01`…`P8-20` with the evidence line each one earned (command + result + identifiers).
- [ ] Update the R-11 row in both `phase-08-reliability.md` and `ROADMAP.md` to the explicit-batch definition from the design.
- [ ] Fill the Definition of Done and Portfolio Evidence sections.
- [ ] Do not commit `docs/` changes. Report to architect.

**Verify:**
- [ ] `uv run ruff check .`
- [ ] `uv run pytest -q`
- [ ] `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 RUN_DBT_PUBLISH_INTEGRATION=1 uv run pytest tests/reliability -v`
- [ ] `./scripts/verify_clean_clone.sh`

---

## Open questions for architect

- Task 2: if `metadata.py` or `extract.py` offers no clean monkeypatch boundary, which seam do we add to `src/`?
- Task 13: split `R-14` into its own module, or keep it with `R-08`…`R-10`? Decide by file length at that point.
