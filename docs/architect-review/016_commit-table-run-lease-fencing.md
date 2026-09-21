# 016. commit_table_run이 Lease 소유권을 확인하지 않는다

> 판정: architect, 2026-09-21 (Phase 8 Task 7 R-05 추가 실측 중 발견)
> 관련: PRD Table Lease Fencing, AC-11, [Phase 8 계획](../superpowers/plans/2026-09-20-phase8-reliability-scenarios.md) Task 7, [014 판정](014_ingestion-error-type-vocabulary.md)

## 발견

developer가 R-05 시나리오에서 `assert_table_lease` 없이 만료된 Lease 소유자(`raw_run`)로 `commit_table_run`을 직접 호출했다. 결과는 **SUCCEEDED**다. `bronze_objects_count=1`, `run_status=SUCCESS`, Watermark version은 인수 전과 같은 1이라 CAS가 통과했다. 예외는 없었다. 증적은 `data/reliability/r05/20260920T154707870235Z.json`의 `commit_without_lease_check` 이벤트다.

코드로 확인된다. `src/ingestion/metadata.py:288` `commit_table_run`의 Watermark UPDATE 조건은 `pipeline_name`, `source_table`, `version`, `watermark_timestamp`, `watermark_keys`뿐이다. `lease_owner`도 `lease_expires_at`도 없다.

같은 모듈의 `rewind_watermark`(`src/ingestion/metadata.py:204`)는 같은 테이블에 쓰면서 `AND lease_owner = %s AND lease_expires_at > %s`를 건다. 두 쓰기 경로가 서로 다른 보호를 쓴다. Commit 쪽만 비어 있다.

## 영향

Fencing이 실효성을 잃는다. `src/ingestion/service.py:307-308`은 `assert_table_lease` 직후 `commit_table_run`을 호출하지만 두 호출은 서로 다른 Transaction이다. 그 사이에 Lease가 만료되고 다른 Run이 인수해도 Commit은 막히지 않는다. `acquire_table_lease`는 `lease_owner`만 바꾸고 `version`은 올리지 않으므로, 인수자가 아직 Commit하지 않은 동안 Version CAS는 통과한다.

결과로 **엉뚱한 쪽이 실패한다.**

| 주체 | 현재 동작 | 있어야 할 동작 |
| ---- | --------- | -------------- |
| 만료된 Lease의 Run A | Commit 성공, Watermark 전진 | `LEASE_OWNERSHIP_LOST`로 거부 |
| 정당한 인수자 Run B | 뒤이은 Commit이 Version CAS에서 `WatermarkConflictError` | Commit 성공 |

B가 업로드한 Object는 Orphan이 되고 R-06 경로로 넘어간다. Watermark 단조성은 Version CAS가 지키므로 **데이터 손실이나 이중 Commit은 없다.** 깨지는 것은 "Lease를 잃은 Run은 쓰지 못한다"는 Fencing 계약 자체다. 심각도는 데이터 정합성이 아니라 계약 위반이다.

## 판정

**수정한다. 단 Task 7 안에서 하지 않는다. Task 7A를 신설해 R-05 증적 확정 뒤에 착수한다.**

- [014](014_ingestion-error-type-vocabulary.md)와 같은 원칙이다. Phase 8은 관측이 먼저다. R-05 증적 JSON과 Runbook은 **수정 전 동작**을 기록한 상태로 확정한다. 그 증적이 이 수정의 근거가 된다.
- 다만 014와 달리 대기 이유가 짧다. 이 창에 의존하는 다른 R 시나리오가 없고 R-05 실측이 이미 끝났다. Task 8 완료 직후 착수해도 된다. Task 13A까지 미루지 않는다.
- developer가 `src`를 건드리지 않고 판정을 기다린 것은 옳다.

### 구현 방향

Version CAS 하나에 Lease 조건을 합치면 안 된다. 합치면 `rowcount != 1`의 원인이 Version 충돌인지 Lease 상실인지 구분되지 않고, `WatermarkConflictError`를 기대하는 R-04의 오류 계약이 깨진다.

Commit Transaction 안에서 두 단계로 나눈다.

1. `SELECT lease_owner, lease_expires_at FROM watermarks WHERE pipeline_name = %s AND source_table = %s FOR UPDATE`로 Row를 잠근다.
2. `lease_owner != commit.lease_owner` 또는 `lease_expires_at <= current_time`이면 `TableLeaseOwnershipLostError("Table lease ownership was lost before metadata commit")`를 던진다.
3. 기존 Version CAS는 그대로 두고, 실패 시 `WatermarkConflictError`를 그대로 던진다.

`FOR UPDATE`가 Row를 잡고 있으므로 두 단계는 원자적이다. 별도 Advisory Lock은 필요 없다.

`TableCommit`에 `lease_owner: uuid.UUID` 필드를 필수로 추가한다. 기본값을 주지 않는다. 기본값을 주면 호출자가 빠뜨려도 조용히 통과한다. `__post_init__`에 검증은 넣지 않는다 — DB가 유일한 진실이다.

호출 지점은 좁다. `src/ingestion/service.py:308` 하나, 그리고 `tests/reliability/test_r01_r07_ingestion_commit.py`의 4곳이다.

## 조치

| 대상 | 조치 |
| ---- | ---- |
| Phase 8 계획 | Task 7A를 신설한다. Task 8 뒤, Task 9 앞에 둔다. |
| `docs/runbooks/r05-expired-lease.md` | developer가 추가한 '알려진 위험' 절을 유지한다. Task 7A 완료 시 '수정 완료' 상태와 ADR 링크를 덧붙인다. 실측값은 지우지 않는다. |
| `data/reliability/r05/*.json` | 수정하지 않는다. 수정 전 동작의 증적이다. |
| `src/ingestion/service.py` | Task 7A 전까지 변경하지 않는다. |

## Phase 8이 증명한 것

014에 이어 두 번째다. Phase 3~7의 정상 경로 테스트는 `assert_table_lease`와 `commit_table_run`을 항상 붙여서 호출했기 때문에 이 창을 볼 수 없었다. 두 호출을 의도적으로 떼어낸 실측이 Fencing 계약의 구멍을 드러냈다. 계약을 검증하려면 계약을 지키지 않는 호출자를 만들어 봐야 한다.
