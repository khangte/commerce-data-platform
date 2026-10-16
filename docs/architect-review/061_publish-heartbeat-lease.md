# 061. Publish Run Heartbeat Lease와 안전한 인수

- 날짜: 2026-10-16
- 상태: **채택·구현 완료.** lead 지시(2026-10-16). warehouse-publish-audit·058에서 미룬 항목이다.
  - reviewer가 재리뷰 후 승인했다(§2.1.1 반영).
  - 실환경 검증 결과:
    - 이전 Run `5d25b265-ef74-4c0c-8274-af1697c609f7`: dbt 실행 중 프로세스 그룹 SIGKILL. 즉시 재실행은 차단됐고, 마지막 Heartbeat 뒤 301.8초에 인수돼 `FAILED/UNKNOWN_ERROR/abandoned`로 닫혔다.
    - 새 Run `f9960f3b-f5ac-40e2-bb8a-14cb0891ce00`: `PUBLISHED`.
    - Export `7e0deae2-ce49-41cf-ab65-445dcbdabe8e`: 재지정 `SUCCESS`, `fresh=true`.
- 판정:
  - 시작 시각 기준 1시간 Stale 규칙을 `heartbeat_at` 기준 5분 Lease로 바꾼다.
  - 인수는 상태와 Heartbeat를 함께 조건으로 거는 단일 UPDATE로 먼저 확정한다. 파일 처리는 그 뒤에 한다.
- 관련:
  - ADR 016(Build-then-swap, 부분 Unique Index Mutex), 058(파일 식별값 복구), 059(CLI Publish 뒤 Export).
  - PRD v1.26 §12.8. 변경 계약은 PRD v1.27에 반영한다.

## 1. 문제

`recover_incomplete_publishes`는 `started_at < now - 1h`인 `BUILDING`·`PUBLISHING` Row만 복구한다.

1. **Crash 뒤 최대 1시간 차단.** Process가 `BUILDING` 중에 죽으면 Row가 활성으로 남는다. 부분 Unique Index `mart_publish_runs_single_active_idx` 때문에 1시간 동안 모든 Publish가 `PublishInProgressError`로 실패한다.
   - DAG `prepare_warehouse_build`의 재시도(60초·120초)도 같은 이유로 모두 실패한다.
2. **살아 있는 긴 Build의 인수.** 시작 시각만 보므로 1시간 넘게 정상 진행 중인 Build도 다음 실행이 abandoned로 닫는다.
3. **인수 순서.** 복구가 Build 파일을 먼저 `failed/`로 옮기고 그 뒤에 `mark_failed`를 실행한다. 판단과 파일 처리 사이에 원자성이 없다.

Serving 손상 경로는 없다. Run마다 Build 파일·target 경로가 분리되어 있고, `mark_publishing`이 `BUILDING` 상태를 조건으로 걸기 때문이다. 문제는 가용성이다.

## 2. 판정

### 2.1 소유 생존 신호: `heartbeat_at` Lease — 채택

- `mart_publish_runs.heartbeat_at TIMESTAMPTZ` 컬럼을 추가한다. 기존 Row는 NULL이다.
  - `start_publish_run`이 `started_at`과 같은 값으로 채운다.
- Run을 다루는 세 단계에서 소유 Process가 갱신한다. 세 단계는 `prepare_warehouse_build`, `build_warehouse`의 dbt 실행, `publish_build`다.
  - 갱신 주기는 30초(`PUBLISH_HEARTBEAT_INTERVAL`)이고, 갱신 주체는 daemon Thread다.
  - 단계가 끝날 때 한 번 더 갱신한다. DAG Task 사이의 공백을 마지막 단계 종료 시점부터 재기 위해서다.
  - 갱신 UPDATE는 활성 상태를 조건으로 건다. 0행이면 Lease를 잃은 것이다. 경고를 남기고 Thread를 멈춘다.
  - 일시적인 DB 오류는 경고만 남기고 다음 주기에 다시 시도한다.
- **종료 시 갱신의 0행은 Lease 상실이 아니다** (§2.1.1).
- Stale 기준은 `COALESCE(heartbeat_at, started_at) < now - 5분`(`PUBLISH_LEASE`)이다.
  - 30초 주기의 10배다. DAG Task 사이 Scheduler 공백도 이 안에 들어간다.
- 시각은 지금처럼 Client의 `datetime.now(UTC)`를 쓴다. Airflow·호스트 CLI·Postgres는 같은 WSL2 VM의 Clock을 쓴다. 이미 같은 파일 시스템과 Hardlink를 전제로 하고 있다.

#### 2.1.1 종료 시 갱신 결과 처리 (reviewer 지적, 2026-10-16) — 채택

- 현상: `publish_build` 본문이 `mark_published`로 Run을 종료 상태로 바꾼다. 그 뒤 컨텍스트 종료 시 갱신이 항상 0행이 되어, 성공한 Publish마다 "소유권 상실" 경고가 남는다.
- 판정: **(c) 종료 시 갱신의 0행은 경고하지 않는다.**
  - 주기 갱신 0행만 Lease 상실로 보고 경고한 뒤 Thread를 멈춘다.
  - 종료 시 갱신의 목적은 Run이 활성으로 남을 때(DAG Task 사이) 공백의 기준점을 남기는 것이다. 0행이면 본문이 Run을 종료했다는 뜻이고, 기준점이 필요 없다.
  - 마지막 주기 뒤에 실제로 인수당한 경우는 본문의 조건부 전이가 `PublishStateError`로 드러낸다. 경고가 없어도 놓치지 않는다.
  - 종료 시 갱신에서 난 DB 예외는 지금처럼 경고한다.
- 기각:
  - (a) `publish_build`만 종료 시 갱신 생략: 인자가 늘고, Run을 종료하는 다른 본문이 생기면 같은 문제가 반복된다.
  - (b) `mark_published` 전에 갱신: 종료 시 갱신이 여전히 `mark_published` 뒤에 실행되므로 경고가 없어지지 않는다.

**기각한 대안**

| 대안 | 기각 사유 |
| --- | --- |
| PID·Host 생존 확인 | DAG Run 하나가 세 Task Process에 걸쳐 있어 Task 사이에는 소유 PID가 없다. 컨테이너 PID는 호스트 CLI에서 보이지 않는다. |
| Postgres Advisory Lock(Session) | 연결이 끊기면 즉시 풀린다는 장점이 있다. 그러나 Task 경계를 넘지 못해 공백 구간에는 결국 시간 규칙이 필요하다. 이중 장치가 된다. |
| Stale 시간만 단축(예: 10분) | 2번 문제(살아 있는 긴 Build 인수)가 오히려 커진다. dbt 전체 Build가 10분을 넘으면 정상 Run을 닫는다. |
| Airflow DagRun 상태 조회 | Publish 모듈이 Airflow에 의존하게 된다. CLI Run에는 쓸 수 없다. |
| 같은 DAG의 이전 Row 즉시 인수(`max_active_runs=1` 근거) | Lease로 차단 상한이 5분이 되어 이득이 작다. Airflow 의미를 Publish 모듈에 넣게 된다. §5 재검토 조건으로 둔다. |

### 2.2 인수 = 조건부 상태 전이 먼저, 파일 처리 나중 — 채택

- 복구는 Stale 후보를 조회한 뒤 Row마다 단일 UPDATE로 닫는다. 조건은 `status`가 활성이고 `COALESCE(heartbeat_at, started_at) < stale_before`인 것이다.
  - 0행이면 그 사이 Heartbeat가 갱신된 것이다. 건너뛰고 파일도 건드리지 않는다.
- 결과 상태는 058 규칙 그대로다.
  - `PUBLISHING`이고 파일 식별값이 같으면 `PUBLISHED`로 닫고 Build 잔여물을 지운다.
  - 그 외에는 `FAILED(UNKNOWN_ERROR, "abandoned")`로 닫는다.
- `FAILED`로 닫은 뒤에만 Build를 `failed/`로 격리한다. `failed_path`는 별도 UPDATE로 기록한다.

### 2.3 동시 실행 안전성(Fencing)

| 장치 | 보장 |
| --- | --- |
| 부분 Unique Index(유지) | 활성 Row는 항상 1개다. 인수 Run은 이전 Row가 닫힌 뒤에만 `BUILDING`을 얻는다. |
| 인수가 먼저 상태를 바꿈 | 인수 뒤 살아 돌아온 Zombie의 모든 전이(`record_dbt_result`·`mark_publishing`·`mark_published`)는 `PublishStateError`로 거부된다. |
| `mark_publishing`이 `os.replace` 앞에 있음(유지) | Zombie는 상태 전이를 통과하지 못하므로 Published 파일을 교체할 수 없다. |
| Run별 Build·target 경로(유지) | Zombie와 인수 Run이 같은 파일을 쓰지 않는다. |
| Serving Export는 Published 파일 식별값을 검사(058, 유지) | 남은 경합(§4)이 생겨도 기록과 다른 파일은 Export되지 않는다. |

## 3. 효과

- Crash 뒤 차단 시간: 최대 1시간에서 마지막 Heartbeat 뒤 5분으로 줄어든다.
- 정상 진행 중인 Run은 소요 시간과 관계없이 인수되지 않는다.
- DAG `prepare_warehouse_build` 재시도는 5분 안에 끝나므로 여전히 막힐 수 있다. 다음 DagRun이나 수동 실행은 5분 뒤 정상 진행된다.

## 4. 남은 위험

- **Heartbeat Thread가 5분 넘게 멈춘 경우.** Process가 SIGSTOP이나 VM 정지로 살아 있는 채 멈추면 인수될 수 있다. 재개된 Zombie는 §2.3에 따라 Published 파일을 교체하지 못한다.
  - 예외는 `PUBLISHING` 전이 직후 멈췄다가 인수 직후 `os.replace`가 성공하는 경우다. 이때는 식별값 비교가 일치 여부에 따라 Row를 결정하고, 058 검사가 Export 단계에서 불일치를 막는다.
- **고아 dbt 프로세스.** Python만 죽고 자식 dbt가 남으면 격리된 Build 파일에 계속 쓴다. 그 파일은 `failed/` 격리본이라 Publish 경로와 무관하다.

## 5. 재검토 조건

- 정상 Run이 Lease 만료로 인수된 사례가 관측되면 Lease를 늘리거나 갱신 경로를 점검한다.
- DAG 재시도가 Lease 때문에 반복 실패하는 일이 잦으면 같은 `pipeline_name` Row의 즉시 인수를 다시 검토한다.
- 여러 Host에서 Publish하게 되면 Clock 기준을 Postgres `clock_timestamp()`로 바꾼다.

## 6. 완료 조건

- Crash 모의 Test(Postgres 통합)가 통과한다.
  - 자식 Process가 `BUILDING`과 Heartbeat를 잡은 상태에서 SIGKILL을 받는다.
  - Lease 안에서는 새 Publish가 차단된다.
  - Lease가 지나면 새 Publish가 이전 Row를 abandoned로 닫고 진행한다.
- 살아 있는 Run은 `started_at`이 오래되어도 인수되지 않는다.
- 인수 뒤 Zombie의 `mark_publishing`이 거부되고 Published 파일이 그대로다.
- 실환경에서 CLI Publish를 dbt 실행 중 SIGKILL로 중단한다. 즉시 재실행은 차단되고, 5분 뒤 재실행은 `PUBLISHED`가 된다. 이어서 `src.serving export`와 `status fresh=true`를 확인한다.
- 전체 Test Suite와 `ruff check`가 통과하고, reviewer가 승인한다.
