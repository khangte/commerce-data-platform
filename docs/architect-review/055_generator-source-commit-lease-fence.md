# 055. Generator 최종 Lease 확인과 Source COMMIT 사이의 창

> 판정: architect, 2026-10-06 (reviewer 설계 판단 요청, generator 복구·lease fencing 리뷰)
> 관련: [016](016_commit-table-run-lease-fencing.md) 같은 계열, PRD Global Source Mutation Lease, `src/generator/lease.py`, `src/generator/service.py`

## 발견

`src/generator/service.py:218`의 최종 `assert_source_mutation_lease`는 Pipeline DB에 새 커넥션을 열어 잠금 없이 Lease Row를 읽는다(`src/generator/lease.py:127` `_read_lease`). 확인이 끝나면 커넥션을 닫는다. Source 트랜잭션 COMMIT은 그 뒤에 다른 DB에서 일어난다.

확인과 COMMIT 사이에 Lease가 만료되고 Warehouse(Ingestion)가 인수하면, Ingestion이 Source를 읽기 시작한 뒤에 Generator 변경이 커밋될 수 있다. 016이 지적한 "확인과 커밋이 다른 트랜잭션" 창과 같은 구조다. 창은 밀리초 수준으로 좁지만, 계약상 막혀 있지 않다.

## 영향

Global Source Mutation Lease가 지키는 것은 "Ingestion이 Source를 읽는 동안 Generator는 Source를 바꾸지 않는다"이다. 창이 열리면 Ingestion은 이 Generator 실행 전의 Snapshot으로 Watermark를 전진시킨다. 이후 커밋된 행의 커서 값이 그 Watermark 이하이면 다음 수집에서도 잡히지 않는다. 결과는 조용한 누락이다. 016은 계약 위반에 그쳤지만, 이 창은 데이터 누락으로 이어질 수 있다.

## 선택지 검토

| 안 | 판단 | 근거 |
| -- | ---- | ---- |
| (a) Source COMMIT 동안 Pipeline 트랜잭션에서 Lease Row를 `FOR SHARE`로 잡는다 | **채택** | `acquire`·`renew`·`release`는 모두 `FOR UPDATE`로 Row를 잠근다. `FOR SHARE`를 잡은 동안 인수자는 대기한다. 인수는 Source COMMIT이 끝난 뒤에만 일어난다. 두 DB에 걸친 순서를 잠금 하나로 보장한다. 2PC가 필요 없다. |
| (b) Lease Version을 Source 측 Fencing Token으로 기록·검증한다 | 반려 | 상대 Lease 보유자인 Ingestion은 Source를 읽기만 한다. Token을 쓰지 않는 Reader는 Writer를 Fencing할 수 없다. Source 측 비교 기준도 결국 Pipeline DB의 Version을 읽어야 하므로 같은 창이 다시 생긴다. |
| (c) 잔여 창을 수용하고 문서화한다 | 반려 | 영향이 조용한 데이터 누락이다. (a)의 비용은 컨텍스트 매니저 하나와 호출 구조 변경 하나로 작다. |

### (a)가 닫는 경우

- 인수자가 `FOR SHARE` 이전에 인수를 끝냈다: `FOR SHARE` 뒤 확인에서 Owner 또는 Version 불일치를 본다. `LeaseOwnershipLostError`를 던지고 Source는 롤백된다.
- 인수자가 `FOR UPDATE`를 잡은 채 커밋 전이다: `FOR SHARE`가 대기한다. 인수 커밋 뒤 불일치를 보고 롤백한다.
- 인수자가 `FOR SHARE` 이후에 왔다: 인수자의 `FOR UPDATE`가 대기한다. Generator의 Source COMMIT과 Pipeline 트랜잭션 종료 뒤에 인수가 진행된다. Ingestion은 커밋된 Source를 읽는다.
- 확인 시점에는 유효했고 COMMIT 도중 `lease_expires_at`이 지났다: 인수자는 Row를 잡을 수 없으므로 순서는 여전히 보장된다. 만료 판정은 시계가 아니라 잠금 순서로 직렬화된다.
- Source COMMIT 성공 뒤 Pipeline 트랜잭션 종료 전에 프로세스가 죽었다: Pipeline 트랜잭션에는 쓴 것이 없다. 잠금만 풀린다. 재시도는 `generator_commits` 마커 복구 경로가 처리한다.

## 판정

**수정한다. (a)로 구현한다.**

### 구현 방향

1. `src/generator/lease.py`에 `fenced_source_commit(settings, lease, *, now=None)` 컨텍스트 매니저를 추가한다.
   - Pipeline 커넥션과 트랜잭션을 연다.
   - `SELECT owner_type, owner_id, lease_expires_at, version FROM source_mutation_leases WHERE resource_name = %s FOR SHARE`로 Row를 잡는다. 기존 `_locked_lease`에 잠금 모드 인자를 주거나 `_shared_locked_lease`를 따로 둔다. 어느 쪽이든 SQL 중복은 최소로 한다.
   - `_assert_current_lease`로 Owner·Version·만료를 확인한다.
   - `yield`한다. 블록 종료 시 Pipeline 트랜잭션을 끝낸다. 쓰기는 하지 않는다.
   - 잠금 모드는 `FOR SHARE`다. `FOR KEY SHARE`는 쓰지 않는다. `FOR KEY SHARE`는 `FOR UPDATE` 없이 실행되는 일반 `UPDATE`(`FOR NO KEY UPDATE`)를 막지 못한다.
2. `src/generator/service.py`의 Source 트랜잭션 블록을 아래 구조로 바꾼다. Fence는 Source 트랜잭션 **바깥에서** 닫혀야 한다. 그래야 Source COMMIT이 Fence 안에서 일어난다.

   ```python
   with (
       settings.source_connection() as connection,
       ExitStack() as fence,
       connection.transaction(),
   ):
       ...  # 기존 변경 로직
       record_source_commit(
           connection, generator_run_id, config, result_counts, logical_content_hash
       )
       fence.enter_context(fenced_source_commit(settings, lease))
   # transaction 종료(Source COMMIT) → fence 종료(Pipeline 트랜잭션 종료) 순서
   ```

   - `with` 다중 대상은 역순으로 닫힌다. `connection.transaction()`이 먼저 커밋하고, 그다음 `ExitStack`이 Fence를 닫는다.
   - Fence 진입 시 확인이 실패하면 예외가 Source 트랜잭션 안에서 발생하므로 Source는 롤백된다.
   - Source COMMIT이 실패하면 예외가 Fence를 통과하며 Pipeline 트랜잭션도 롤백된다.
3. `service.py:218`의 최종 `assert_source_mutation_lease` 호출은 Fence로 대체하고 지운다.
4. 주문 루프(`service.py:160`)와 전이(`service.py:334`)의 중간 `assert_source_mutation_lease`는 유지한다. 계약 보장이 아니라 조기 중단용이다. Fence 대기 시간을 늘리지 않도록 Fence는 COMMIT 직전에만 잡는다. 실행 전체에 걸쳐 `FOR SHARE`를 잡지 않는다. 그렇게 하면 인수자가 `LeaseUnavailableError` 대신 실행 시간만큼 대기한다.
5. `assert_source_mutation_lease`는 Ingestion이 계속 쓰므로 지우지 않는다.

### 테스트

- 단위: 가짜 Settings로 호출 순서를 검증한다. Fence 진입은 `record_source_commit` 뒤, Source COMMIT 전이다. Fence 종료는 Source COMMIT 뒤다.
- 단위: Fence 진입에서 `LeaseOwnershipLostError`가 나면 Source가 롤백되고 `generator_commits` 마커가 남지 않는다. 기존 "소유권 상실 시 롤백" 테스트가 최종 확인 지점을 대상으로 하면 Fence 경로로 옮긴다.
- 통합(PostgreSQL): Fence를 잡은 상태에서 다른 커넥션이 `SELECT ... FROM source_mutation_leases WHERE resource_name = 'commerce_source' FOR UPDATE NOWAIT`를 실행하면 `LockNotAvailable`이 난다. Fence 종료 뒤에는 같은 쿼리가 성공한다. 스레드 경합 테스트는 만들지 않는다. 잠금 충돌 확인으로 충분하다.

## Ingestion 쪽은 해당 없음

`src/ingestion/service.py:305`도 확인 뒤 별도 트랜잭션에서 커밋한다. 다만 Ingestion은 Source를 읽기만 한다. 확인 이후 Generator가 인수해도 Generator 변경은 Ingestion의 읽기 뒤에 일어난다. 그 행은 다음 수집이 잡는다. 같은 창이 누락으로 이어지지 않으므로 이번 범위에 넣지 않는다.

## 미결

PostgreSQL 통합 테스트는 Docker 부재로 아직 실행하지 못했다. 이 수정의 핵심 보장은 잠금 동작이므로 통합 테스트 통과 전에는 완료로 보지 않는다.

## 검수 (2026-10-06)

developer 반영본을 검수했다. 판정은 **코드 승인, 통합 게이트 미통과**다.

| 항목 | 결과 |
| ---- | ---- |
| `fenced_source_commit` | Pipeline 트랜잭션에서 `FOR SHARE`로 잠그고 확인한 뒤 `yield`한다. 쓰기는 없다. 설계와 같다. |
| `service.py` 구조 | `source_connection`, `ExitStack`, `transaction()` 순서다. Source COMMIT이 Fence 종료보다 먼저 일어난다. 최종 단순 확인은 제거됐고 중간 확인은 유지됐다. |
| `_locked_lease(mode=...)` | f-string으로 잠금 모드를 넣는다. 호출자는 모듈 내부 리터럴(`UPDATE`, `SHARE`)뿐이라 주입 경로는 없다. 수용한다. |
| 단위 테스트 | `tests/generator` 67 passed, `ruff check` 통과. architect가 직접 실행했다. |
| 통합 테스트 | 8건 모두 skip이다. 잠금 충돌 테스트 1건, 스캔 롤백 테스트, 기존 Lease·Generator 통합 테스트가 포함된다. |

### 통합 게이트

이 수정의 보장은 PostgreSQL 행 잠금 동작에 있다. 단위 테스트는 호출 순서만 증명한다. 잠금 충돌은 증명하지 않는다. 따라서 통합 테스트 통과 전에는 이 항목을 완료로 닫지 않는다.

- 코드 커밋은 허용한다. 동작이 단위 수준에서 검증됐고, 통합 미실행 상태가 Phase 2 문서에 표시돼 있다.
- Phase 2 체크 항목의 완료 표시는 통합 통과 뒤에 확정한다.
- 실행 환경: WSL에는 Docker CLI가 없다. Windows Docker Desktop은 설치돼 있다(`/mnt/c/Program Files/Docker/...`). Docker Desktop의 WSL Integration을 이 배포판에 켜면 Phase 1 PostgreSQL 컨테이너를 띄울 수 있다. 이 설정은 사용자 조치다.
- 실행 명령: 컨테이너 기동 후 `RUN_POSTGRES_INTEGRATION=1 pytest tests/integration/test_source_mutation_lease_integration.py tests/integration/test_generator_service_integration.py`.

### 경미 사항

`test_fenced_source_commit_blocks_lease_takeover_until_source_commit`가 모듈의 `pytestmark = pytest.mark.integration` 선언보다 위에 있다. 동작에는 영향이 없다. 기존 배치 관례에 맞게 선언 아래로 옮긴다.

## 통합 게이트 종결 (2026-10-06)

**종결한다.** 통합 게이트를 통과했다.

- Docker Desktop WSL Integration이 활성화됐고 Phase 1 PostgreSQL 컨테이너가 healthy 상태다.
- architect 직접 실행: Lease·Generator·구독 결제 불변식 통합 9건 `9 passed`. `RUN_POSTGRES_INTEGRATION=1` 전체 `tests`는 `329 passed, 54 skipped`다.
- `test_fenced_source_commit_blocks_lease_takeover_until_source_commit`가 통과했다. Fence 보유 중 `FOR UPDATE NOWAIT`는 `LockNotAvailable`이고, 종료 뒤에는 성공한다. 이 수정의 핵심 보장이 실제 PostgreSQL에서 확인됐다.
- reviewer 최종 승인(지적 0건)을 받았다.
- `tests/integration/test_subscription_billing_invariant_integration.py`의 격리 수정을 수용한다. 이 테스트는 임시 테이블로 결제 불변식만 검증한다. 새로 생긴 `fenced_source_commit`, `committed_result`, `record_source_commit`, `ensure_generator_commits`를 다른 Lease·Metadata 스텁과 같은 방식으로 막았다. 검증 대상은 바뀌지 않았다.
- Phase 2 체크 항목의 완료 표시를 확정한다.
