# R-05 만료된 Lease

## 문제

Table Lease가 만료된 뒤 새 소유자가 인수하면, 이전(Stale) 소유자는 이후 어떤 Metadata
쓰기 시도에서도 Fencing되어야 한다. AC-21(Generator/Warehouse 소스 Lock 충돌)도 이
Table Lease Fencing 메커니즘의 변형이라 별도 시나리오를 만들지 않는다.

## 재현 조건과 명령

`RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability/test_r01_r07_ingestion_commit.py -k r05 -v`를 실행한다.
Stale Owner가 Lease를 획득(TTL 30분) → 만료 → 새 Owner가 인수 → Stale Owner가 `assert_table_lease`와
Commit 경로를 재시도하는 순서로 진행한다.

## 기대/실제 관측

| 시점 | 기대 | 실제 |
|---|---|---|
| 인수 전 | Stale Owner의 `assert_table_lease` 통과 | 통과 |
| 인수 후 | Stale Owner의 `assert_table_lease` → `TableLeaseOwnershipLostError` | 일치 |
| Stale Owner Commit 시도 | Object 미생성, `watermarks.lease_owner` = 새 Owner 유지 | 일치 |

## 원인과 불변 조건

`watermarks` Table의 `(pipeline_name, source_table)` 행마다 `lease_owner`/`lease_expires_at`가 하나만
존재한다. `acquire_table_lease`는 `lease_owner IS NULL OR lease_expires_at <= now OR lease_owner = 자신`
조건에서만 Owner를 교체하므로, 만료 뒤 인수는 원자적이다. `assert_table_lease`는 저장된 `lease_owner`와
`watermark_version`을 Stale Snapshot과 비교해 둘 중 하나라도 다르면 Fencing한다. Ingestion 서비스는
Snapshot·Upload·Commit 직전마다 이를 호출하므로(`src/ingestion/service.py:307-308`) 정상 경로에서는
인수 이후 Stale Owner의 쓰기가 통과하지 못한다.

## 알려진 위험: 사전 검사와 Commit 사이 창

`commit_table_run`(`src/ingestion/metadata.py:288`) 자체는 `lease_owner`를 확인하지 않는다. Watermark
CAS는 `version`·`watermark_timestamp`·`watermark_keys`만 검사한다. 인수 직후 새 Owner가 아직 아무것도
Commit하지 않은 상태라면 `version`은 인수 전과 같으므로, Stale Owner가 `assert_table_lease` 없이(또는
그 검사를 통과한 직후 Lease를 다시 잃기 전에) `commit_table_run`을 직접 호출하면 Commit이 **성공한다**.

R-05 테스트에서 이를 직접 호출해 실측했다(`commit_without_lease_check` 이벤트, `outcome: SUCCEEDED`,
`bronze_objects_count: 1`, `run_status: SUCCESS`, 증적 `data/reliability/r05/20260920T154707870235Z.json`).
Fencing은 `commit_table_run` 자신이 아니라 호출자가 그 직전에 `assert_table_lease`를 부르는 관례에만
의존했다. 이 관례를 벗어나는 어떤 호출 경로도 이 창을 통과할 수 있었다. 위 실측값은 수정 전 동작
기록이므로 고치지 않는다.

**수정 완료(Task 7A, 2026-09-21).** `commit_table_run`의 Transaction 맨 앞에서
`SELECT lease_owner, lease_expires_at FROM watermarks ... FOR UPDATE`로 Row를 잠그고 검사해,
Lease를 잃었으면 `TableLeaseOwnershipLostError("Table lease ownership was lost before metadata
commit")`를 던진다. 이제 같은 `commit_without_lease_check` 호출은 `SUCCEEDED`가 아니라
`TableLeaseOwnershipLostError`(`classify_error` == `LEASE_OWNERSHIP_LOST`)로 거부되고, Object는
0건, Watermark version은 그대로 유지된다. 판정·구현 근거는
[ADR 017](../adr/017-commit-table-run-lease-fencing.md)을 따른다.

## 복구 절차

Stale Owner Run은 `record_failed_run`으로 FAILED 처리한다. `watermarks.lease_owner`가 새 Owner인지,
Stale Owner의 `table_batch_id`로 커밋된 `bronze_objects` 행이 없는지 확인한다. 필요하면 새 Owner의
Run이 정상 진행 중인지 관찰 SQL로 확인하고 별도 개입 없이 완료를 기다린다.

## 재검증 명령과 결과

R-05 증적의 `timeline`에서 `acquire → expire → takeover → fenced_commit_attempt` 순서와 각 단계의
`owner_id`, `result: TableLeaseOwnershipLostError`를 확인하고 위 pytest 명령이 통과하는지 검증한다.
