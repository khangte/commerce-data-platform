# Generator → Warehouse DAG 자동 트리거 설계

> 작성: 2026-09-11
> 관련 Phase: [Phase 4. Airflow Orchestration](../../phases/phase-04-airflow-orchestration.md)
> 관련 DAG: `airflow/dags/source_simulation_dag.py`, `airflow/dags/warehouse_pipeline_dag.py`

## 배경

`source_simulation_dag`(Generator)와 `warehouse_pipeline_dag`(Warehouse)는 각각
`schedule=None`으로 독립 수동 트리거된다. 두 DAG 모두 같은 `logical_date` 개념을
쓰지만, Generator 완료 뒤 Warehouse를 실행하려면 지금은 사람이 직접 두 번째 DAG를
트리거해야 한다. 이 문서는 Generator 성공 뒤 같은 논리 시각으로 Warehouse DAG를
자동 트리거하는 연결을 설계한다.

## 목표

- Generator(`source_simulation_dag`) 성공 직후, 같은 논리 시각(`logical_date`)으로
  Warehouse(`warehouse_pipeline_dag`)를 자동 트리거한다.
- Generator 실패 시 Warehouse는 실행되지 않는다.
- 같은 논리 시각으로 Generator가 중복 실행되어도 Warehouse가 중복으로 처리되지
  않는다(이미 있는 멱등 계약을 재사용하며 새 잠금/락을 추가하지 않는다).
- Phase 4 문서의 Table 수 서술을 실제 코드 기준(9개)으로 동기화한다.

## 범위 밖

- Warehouse DAG 내부 Task 구조 변경
- Generator DAG의 Param 구조 변경
- 새로운 동시성 잠금/락 메커니즘 도입
- dbt Build 활성화(Phase 5 범위, 이미 별도로 존재)

## 설계

### 1. Trigger 연결

`source_simulation_dag`의 `run_source_simulation` Task 뒤에
`airflow.providers.standard.operators.trigger_dagrun.TriggerDagRunOperator` Task를
추가한다.

- `trigger_dag_id="warehouse_pipeline_dag"`
- `logical_date="{{ params.logical_date or logical_date }}"`: `run_source_simulation`이
  실제 사용하는 논리 시각 결정 규칙(Param 우선, 없으면 DagRun의 `logical_date`)을
  Jinja로 그대로 재현한다. XCom을 거치지 않으므로 기존 XCom 계약(Phase 4 문서
  5절 허용 목록)을 바꾸지 않는다.
- `wait_for_completion=False`: Warehouse 실행 상태는 Warehouse 자신의 DagRun으로
  관측하며, Generator DAG가 Warehouse 완료까지 대기하지 않는다.
- `skip_when_already_exists=True`: 중복 트리거 방지. 3절 참고.
- `fail_when_dag_is_paused=True`: Warehouse DAG가 paused일 때 조용히 무시되는
  무증상 미실행 대신 Trigger Task를 명시적으로 실패시킨다.
- `trigger_rule`은 Airflow 기본값(`all_success`)을 그대로 쓴다. 별도 조건 분기
  코드를 추가하지 않는다.
- `run_after`는 지정하지 않는다. Airflow 3의 `TriggerDagRunOperator`는 `run_after`
  미지정 시 `logical_date`를 그대로 사용한다.

### 2. 실패 시 Warehouse 미실행

새 로직을 추가하지 않는다. `run_source_simulation`이 실패하면(Retryable 소진 뒤
`AirflowFailException` 또는 예외 전파) 기본 `trigger_rule=all_success`에 의해
Trigger Task가 `upstream_failed`로 건너뛰어진다. 이는 Airflow의 기본 동작이므로
Trigger Task에 별도 실패 검사 코드를 넣지 않는다.

### 3. 중복 트리거 처리

직접 작성하는 dedup 로직은 없다. Airflow 3의 기존 동작과 Operator Param만 쓴다.

**1차 방어 — Operator의 `skip_when_already_exists=True`**

Airflow 3의 `TriggerDagRunOperator`는 `trigger_run_id`를 지정하지 않으면
`DagRun.generate_run_id(run_type=MANUAL, logical_date, run_after)`로 **결정적
run_id**를 만든다. 따라서 같은 `logical_date`로 두 번째 트리거를 시도하면 run_id가
충돌한다. 기본값(`skip_when_already_exists=False`)에서는 이 충돌이 Trigger Task
실패로 이어져 Generator DAG가 불필요하게 빨간불이 된다.

`skip_when_already_exists=True`를 주면 이미 해당 run_id의 DagRun이 있을 때 Trigger
Task가 `skipped`로 끝나고, Warehouse DagRun은 1개로 유지된다.

**2차 방어 — 기존 Warehouse 멱등 계약**

Operator 단계를 통과해 Warehouse가 실제로 다시 돌더라도(예: 앞선 DagRun을 사람이
지운 뒤 재트리거), 아래 기존 계약이 중복 처리를 막는다. 이 계층에는 변경이 없다.

- Warehouse DAG의 `max_active_runs=1`로 동시 실행이 직렬화된다.
- `extract_validate_load`가 `COMMITTED` Table Batch를 `SKIPPED_ALREADY_COMMITTED`로
  재사용한다(P4-16, 기존 구현).

**Generator 쪽 중복 실행**

같은 `logical_date`로 Generator가 두 번 실행되면 Generator 자체가
`reused_successful_run=true`로 기존 결과를 재사용한다(Phase 2 계약, 변경 없음).
Trigger Task는 이 값을 분기 조건으로 쓰지 않는다 — 위 1차 방어가 run_id 수준에서
이미 처리하므로 Task 안에 조건 분기를 넣지 않는다.

### 4. 테스트

`tests/test_airflow_dags.py`의 기존 `RUN_AIRFLOW_SMOKE_TEST=1` opt-in 패턴을
따라 추가한다(무거운 Compose 기반 테스트라 기본 실행에서는 skip 유지):

1. **실행 순서**: Generator DagRun 성공 뒤 Trigger Task가 `warehouse_pipeline_dag`의
   새 DagRun을 같은 `logical_date`로 생성함을 확인한다.
2. **중복 트리거**: 같은 `logical_date`로 Generator를 2회 실행한 뒤, 두 번째
   Generator DagRun의 Trigger Task가 `skipped`로 끝나고 `warehouse_pipeline_dag`의
   DagRun이 여전히 1개임을 확인한다. (Warehouse가 두 번 도는 것이 아니라, 애초에
   두 번째 DagRun이 생성되지 않는 것이 기대 동작이다.)
3. **실패 시 미실행**: `run_source_simulation`을 강제 실패시켜 Trigger Task가
   `upstream_failed`로 끝나고 `warehouse_pipeline_dag`의 새 DagRun이 생성되지
   않음을 확인한다.

### 5. Phase 4 문서 동기화

`docs/phases/phase-04-airflow-orchestration.md`에 남아있는 "6개 Table"/"7개
Table" 서술을 실제 `SOURCE_TABLES`(9개: `customers`, `customer_subscriptions`,
`customer_membership_tiers`, `subscription_payments`, `products`, `sellers`,
`orders`, `order_items`, `order_payments`) 기준으로 갱신한다.

- 원천 데이터 동시성 잠금 설명 문단의 "6개 Table"
- P4-08/P4-09 본문의 Table 수와 옛 `customer_memberships` 단일축 이름
- Task Graph 블록의 Table 나열
- XCom 검증 문단의 Dynamic Task Mapping 입력 순서 서술
- "구독·등급 전환으로 재작업할 범위" 섹션: 이미 target 9개를 설명하고 있으므로
  본문에 흡수하고 완료 표시로 정리(중복 서술 제거)
- 산출물 / 파일·폴더별 변경 요약 표에 이번 Trigger 연동 변경 파일 추가

## 영향 파일

| 경로 | 변경 |
| --- | --- |
| `airflow/dags/source_simulation_dag.py` | `TriggerDagRunOperator` Task 추가 |
| `tests/test_airflow_dags.py` | 순서/중복/실패 시나리오 테스트 3건 추가 |
| `docs/phases/phase-04-airflow-orchestration.md` | Table 수 9개로 동기화, 신규 Trigger 반영 |

## Definition of Done

- [ ] Generator 성공 시 Warehouse가 같은 `logical_date`로 자동 트리거된다.
- [ ] Generator 실패 시 Warehouse DagRun이 생성되지 않는다.
- [ ] 같은 `logical_date` 중복 Generator 실행이 Warehouse DagRun을 중복 생성하지
      않는다(Trigger Task가 `skipped`).
- [ ] Warehouse DAG가 paused일 때 Trigger Task가 명시적으로 실패한다.
- [ ] `RUN_AIRFLOW_SMOKE_TEST=1`로 신규 테스트 3건이 통과한다.
- [ ] Phase 4 문서의 Table 수 서술이 9개로 일치한다.
