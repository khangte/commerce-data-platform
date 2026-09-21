# ADR 017. commit_table_run에 Lease Fencing을 추가한다

## Status

Accepted (2026-09-21, Phase 8 Task 7A)

## Context

`commit_table_run`(`src/ingestion/metadata.py`)의 Watermark UPDATE는 `pipeline_name`,
`source_table`, `version`, `watermark_timestamp`, `watermark_keys`만 검사했다. `lease_owner`,
`lease_expires_at` 조건이 없어, 만료된 Lease의 소유자가 `assert_table_lease` 없이(또는 그 직후
Lease를 다시 잃기 전에) `commit_table_run`을 직접 호출하면 Commit이 성공했다. 같은 모듈의
`rewind_watermark`는 같은 Table에 쓰면서 `lease_owner`·`lease_expires_at` 조건을 걸어 두 쓰기
경로가 서로 다른 보호를 썼다.

Phase 8 R-05 시나리오에서 이 창을 직접 호출로 실측했다(`data/reliability/r05/20260920T154707870235Z.json`
`commit_without_lease_check` 이벤트, `outcome: SUCCEEDED`). 판정 근거는
[016_commit-table-run-lease-fencing.md](../architect-review/016_commit-table-run-lease-fencing.md).

## Decision

- `commit_table_run`의 Transaction 맨 앞에서 `SELECT lease_owner, lease_expires_at FROM watermarks
  WHERE pipeline_name = %s AND source_table = %s FOR UPDATE`로 Row를 잠근다.
- `lease_owner != commit.lease_owner` 또는 `lease_expires_at <= current_time`이면(Row가 없어도 같다)
  `TableLeaseOwnershipLostError("Table lease ownership was lost before metadata commit")`를 던진다.
- 기존 Watermark Version CAS는 조건을 바꾸지 않는다. Lease 조건을 Version CAS의 `WHERE`에 합치지 않는다.
- `TableCommit`에 `lease_owner: uuid.UUID` 필드를 기본값 없이 필수로 추가한다.
- `src/ingestion/service.py`의 `commit_table_run` 호출에 `table_lease.owner_id`를 넘긴다.

## Alternatives

| 대안 | 기각 사유 |
| ---- | --------- |
| Lease 조건을 Version CAS의 `WHERE`에 합친다 | `rowcount != 1`의 원인이 Version 충돌인지 Lease 상실인지 구분할 수 없다. R-04가 기대하는 `WatermarkConflictError` 계약이 깨진다. |
| 별도 Advisory Lock 사용 | `FOR UPDATE`가 이미 Row를 원자적으로 잠그므로 불필요한 추가 조율 지점이다. |
| `TableCommit.lease_owner`에 기본값(예: `None`)을 준다 | 호출자가 빠뜨려도 조용히 통과해 Fencing이 다시 비활성화된다. |

## Consequences

`commit_table_run`을 직접 호출하는 모든 코드가 유효한 `lease_owner`를 쥐고 있어야 한다. Lease를
전제하지 않고 직접 Commit을 호출하던 `tests/integration/test_ingestion_metadata_integration.py`의
두 테스트는 `acquire_table_lease`로 Lease를 먼저 획득하도록 바뀌었다(범위는 016이 지정한 1곳(src)·
4곳(reliability 테스트)에 포함되지 않았던 추가 2곳으로, 구현 중 실측으로 드러났다). R-05 증적 JSON과
Runbook의 '알려진 위험' 절은 수정 전 동작 기록으로 그대로 둔다.

## Validation

| 근거 | 결과 |
| ---- | ---- |
| `uv run ruff check .` | 통과 |
| `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability/test_r01_r07_ingestion_commit.py -v` | `10 passed` |
| `RUN_POSTGRES_INTEGRATION=1 uv run pytest tests/integration/test_ingestion_metadata_integration.py -v` | `2 passed` |
| R-05 `commit_without_lease_check`(인수자 존재) 재실측 | `pytest.raises(TableLeaseOwnershipLostError)`, `classify_error` == `LEASE_OWNERSHIP_LOST`, Object 0건, Watermark version 불변 |
| R-05 `commit_after_expiry_no_takeover`(인수자 없이 자연 만료) 실측 | 소유자는 그대로라 `lease_owner` 불일치가 아니라 `lease_expires_at <= current_time` 분기가 직접 걸림을 확인. `TableLeaseOwnershipLostError`, `classify_error` == `LEASE_OWNERSHIP_LOST`, Object 0건, Watermark version 불변 |
| `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest -q` (전체) | `269 passed, 0 failed, 6 skipped`(연속 2회 solo 재현). 중간 1회 `3 failed`는 Task 7A 코드와 무관한 자기 유발 문제였다 — 전체 스위트를 Background에서 실수로 두 번 동시 실행해 `source_mutation_leases`의 전역 단일 Row(`RESOURCE_NAME`)를 두 실행이 경합했고, 한쪽 실행의 Warehouse Owner가 다른 쪽에 "Lease is held by WAREHOUSE/..."로 관측됐다. Solo 실행에서는 재현되지 않는다. |
