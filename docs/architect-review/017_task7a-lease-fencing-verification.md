# 017. Task 7A Lease Fencing 구현 검수

> 판정: architect, 2026-09-21 (Phase 8 Task 7A developer 구현 검수)
> 관련: [016 판정](016_commit-table-run-lease-fencing.md), [ADR 017](../adr/017-commit-table-run-lease-fencing.md)

## 판정

**구조는 승인한다. 수정 3건 뒤 Task 7A를 닫는다.**

016이 지시한 구현 방향은 정확히 지켜졌다.

| 지시 | 구현 | 확인 |
| ---- | ---- | ---- |
| Transaction 선두 `SELECT ... FOR UPDATE` | `src/ingestion/metadata.py:292` | 통과 |
| Lease 조건을 Version CAS `WHERE`에 병합 금지 | Version CAS 조건 무변경 | 통과 |
| Lease 상실과 Version 충돌의 오류 분리 | `TableLeaseOwnershipLostError` / `WatermarkConflictError` | 통과 |
| `TableCommit.lease_owner` 기본값 없는 필수 필드 | `quarantine` 앞에 배치 | 통과 |
| `service.py`가 `table_lease.owner_id` 전달 | `src/ingestion/service.py:318` | 통과 |
| Quarantine INSERT보다 먼저 검사 | Lease 검사가 Transaction 첫 문장 | 통과 |

`lease_owner` 불일치를 먼저 보고 `or`로 단락하므로, Lease 해제 상태(`lease_owner IS NULL`,
`lease_expires_at IS NULL`)에서 `None <= datetime` TypeError는 발생하지 않는다. 의도된 순서다.

016이 "4곳"이라고 적은 테스트 호출 범위 밖에서 `tests/integration/test_ingestion_metadata_integration.py`
2곳이 추가로 드러난 것은 developer의 실측이 맞다. 내 범위 산정이 `tests/reliability`만 센 탓이다.
Lease를 전제하지 않고 Commit하던 테스트를 `acquire_table_lease` 뒤로 옮긴 처리도 옳다. 그 두 테스트는
이제 "Lease를 쥔 Commit"을 검증하며, 이는 Service의 실제 호출 형태와 일치한다.

## 수정 요청

### 1. 만료 분기가 검증되지 않았다 (필수)

`test_r05_an_expired_lease_owner_cannot_commit`의 직접 호출 증적은 `lease_owner=stale_owner_id`로
Commit하지만, 그 시점 `watermarks.lease_owner`는 이미 `new_owner_id`다. 따라서 실제로 걸리는 조건은
`lease_row[0] != commit.lease_owner`, 즉 **소유자 불일치**다. `lease_row[1] <= current_time`
(**만료**) 분기는 한 번도 실행되지 않는다.

Test 이름과 Runbook은 "만료된 Lease 소유자가 Commit에서 Fencing된다"고 말하는데, 증명된 것은
"인수된 Lease의 이전 소유자가 Fencing된다"다. 둘은 다른 분기다. 인수자가 없는 상태 — Lease가 만료됐지만
아무도 가져가지 않은 경우 — 는 실제 운영에서 더 흔하다. Crash 뒤 다음 Schedule까지의 구간이 그렇다.

**조치**: 인수 없는 만료 Case를 추가한다. `acquire_table_lease`로 Lease를 얻고, 인수 없이
`now = expires_at + 1초`로 `commit_table_run`을 같은 `lease_owner`로 호출한다.
`TableLeaseOwnershipLostError`, Object 0건, Watermark version 불변을 단언한다. 별도 Test 함수로 쪼개지
말고 같은 R-05 Test 안의 세 번째 Probe로 넣고, 증적 timeline에 `commit_after_expiry_no_takeover`
이벤트로 기록한다.

### 2. ADR 번호 오기 (필수)

`tests/reliability/test_r01_r07_ingestion_commit.py:667`의 증적 문자열이 `(ADR 016)`이다. 이 수정의
ADR은 **017**이다. `docs/adr/016-publish-mart-via-warehouse-file-swap.md`는 Warehouse Publish 건으로
무관하다. `016`은 architect-review 번호이고 ADR 번호와 우연히 겹쳤다.

**조치**: 문자열을 `(ADR 017)`로 고친다. 이미 쓰인 `data/reliability/r05/*.json` 5건은 고치지 않는다 —
증적은 실행 시점 기록이고, 재실행으로 새 증적이 쌓인다.

### 3. ADR Validation 표의 미완 항목 (필수)

`docs/adr/017-commit-table-run-lease-fencing.md`의 전체 스위트 행이 `진행 중`이다. developer 보고에
따르면 단독 재실행 2회 연속 `269 passed / 0 failed / 6 skipped`다.

**조치**: 실제 결과로 갱신한다. 중간 1회 3건 실패가 Background 이중 실행 자기충돌이었다는 사실도 한 줄
남긴다. 나중에 같은 증상을 본 사람이 코드를 의심하지 않게 한다.

## 보류하지 않는 것

`TableCommit.__post_init__`에 `lease_owner` 검증을 넣지 않는다. Lease의 유일한 진실은 DB Row이고
`FOR UPDATE` 검사가 그것을 본다. Dataclass 수준 검증은 같은 것을 두 곳에서 주장하게 만든다.

`orphan.reconcile_orphan`의 Metadata 쓰기는 이번 범위가 아니다. Orphan 재조정은 Lease 없이 도는 보수
경로이고, 별도의 전제(RUNNING Run, Watermark 일치, Reject 0건)로 스스로를 방어한다. Task 7A에서 건드리지
않는다.

## 재검수 (2026-09-21)

수정 3건 모두 확인했다. **Task 7A를 닫는다.**

| 항목 | 확인 |
| ---- | ---- |
| 만료 분기 Probe | `commit_after_expiry_no_takeover`. 별도 `pipeline_name_expiry`에 인수자를 두지 않아 `lease_owner`는 일치하고 `lease_expires_at <= current_time`만 단독으로 걸린다. 의도한 분리다. |
| 단언 | `TableLeaseOwnershipLostError`, `classify_error == LEASE_OWNERSHIP_LOST`, Object 0건, Watermark version 불변 |
| Test 구조 | 같은 함수 안 세 번째 Probe. Test 수 불변(10 passed) |
| ADR 번호 | `(ADR 017)`로 정정. 기존 증적 JSON 5건은 유지 |
| ADR Validation | `269 passed, 0 failed, 6 skipped`. 중간 3건 실패 원인(Background 이중 실행이 `source_mutation_leases` 전역 Row를 경합)도 기록됨 |
| 범위 유지 | `TableCommit.__post_init__` 검증 없음, `orphan.reconcile_orphan` 무변경 |

만료 Probe가 Lease를 해제하지 않고 끝나지만 `_cleanup_metadata_only`가 `watermarks` Row를 지우므로
잔여물은 없다. 추가 조치는 필요 없다.

016이 지적한 Fencing 구멍은 닫혔다. 이제 `commit_table_run`은 소유자 불일치와 만료를 각각 독립적으로
거부하고, 두 분기 모두 실측 증적을 가진다.
