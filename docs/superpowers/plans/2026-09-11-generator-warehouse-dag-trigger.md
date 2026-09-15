# Generator → Warehouse DAG 자동 트리거 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generator(`source_simulation_dag`) 성공 직후 같은 `logical_date`로 Warehouse(`warehouse_pipeline_dag`)를 자동 트리거하고, Phase 4 문서의 Table 수 서술을 실제 9-Table 기준으로 동기화한다.

**Architecture:** `source_simulation_dag`의 마지막 Task 뒤에 `TriggerDagRunOperator`를 추가한다. `logical_date`는 Jinja 템플릿으로 Generator Task와 동일한 결정 규칙(`params.logical_date` 우선, 없으면 DagRun `logical_date`)을 재현해 전달한다. `skip_when_already_exists=True`로 같은 논리 시각 재트리거 시 결정적 `run_id` 충돌을 에러가 아닌 skip으로 처리하고, `fail_when_dag_is_paused=True`로 Warehouse가 paused 상태일 때 무증상 미실행을 막는다. `trigger_rule`은 기본값(`all_success`)을 그대로 써서 Generator 실패 시 Trigger Task가 `upstream_failed`로 건너뛰게 한다.

**Tech Stack:** Apache Airflow 3.3.1 (`airflow.providers.standard.operators.trigger_dagrun.TriggerDagRunOperator`), pytest, Docker Compose `airflow` profile

**Spec:** [docs/superpowers/specs/2026-09-11-generator-warehouse-dag-trigger-design.md](../specs/2026-09-11-generator-warehouse-dag-trigger-design.md)

## Global Constraints

- XCom 계약을 바꾸지 않는다 — `logical_date`는 Jinja 템플릿으로 직접 참조하며 새 XCom 필드를 추가하지 않는다.
- Warehouse DAG 내부 Task 구조는 건드리지 않는다.
- Generator DAG의 Param 구조는 건드리지 않는다.
- 새 동시성 잠금/락을 추가하지 않는다 — Airflow의 기존 `TriggerDagRunOperator` Param과 Warehouse의 기존 `max_active_runs=1`/`SKIPPED_ALREADY_COMMITTED` 계약만 쓴다.
- 신규 테스트는 `tests/test_airflow_dags.py`의 기존 `RUN_AIRFLOW_SMOKE_TEST=1` opt-in 패턴(`pytestmark`)을 따른다 — 기본 `pytest` 실행에서 무거운 Compose 테스트가 돌지 않는다.
- 한글 문자열은 유니코드 이스케이프(`\uXXXX`)가 아닌 리터럴 UTF-8로 작성한다.
- 함수/클래스 선언 바로 아래 한 줄 한글 docstring을 단다 (코드·쿼리·식별자 등 기술 용어는 예외).

---

### Task 1: `source_simulation_dag`에 Warehouse Trigger Task 추가

**Files:**
- Modify: `airflow/dags/source_simulation_dag.py`
- Test: `tests/test_airflow_dags.py` (신규 함수 추가)

**Interfaces:**
- Consumes: 기존 `run_source_simulation` Task (동일 파일, params 구조 변경 없음)
- Produces: `trigger_warehouse_pipeline` Task — 이후 Task에서 참조할 이름. Warehouse DagRun을 `logical_date="{{ params.logical_date or logical_date }}"`로 생성한다.

현재 `airflow/dags/source_simulation_dag.py` 마지막 줄은 `run_source_simulation()` 호출로 끝난다(83번째 줄). 이 Task 뒤에 Import를 추가하고 `TriggerDagRunOperator` Task를 이어 붙인다.

- [ ] **Step 1: Import 추가**

`airflow/dags/source_simulation_dag.py` 상단 Import 블록에 추가:

```python
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
```

전체 상단 Import 블록은 아래와 같아야 한다:

```python
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, task
from airflow.sdk.definitions.param import Param
from airflow.sdk.exceptions import AirflowFailException

from src.common.database import PostgresSettings
from src.generator.config import EXECUTABLE_ANOMALY_PROFILES, GENERATOR_VERSION, GeneratorConfig
from src.generator.service import resolve_source_snapshot_id, run_generator
from src.ingestion.errors import classify_error, is_retryable
```

- [ ] **Step 2: Trigger Task 추가**

`airflow/dags/source_simulation_dag.py`의 마지막 줄 `run_source_simulation()`을 아래로 교체:

```python
    generator_result = run_source_simulation()

    trigger_warehouse_pipeline = TriggerDagRunOperator(
        task_id="trigger_warehouse_pipeline",
        trigger_dag_id="warehouse_pipeline_dag",
        logical_date="{{ params.logical_date or logical_date }}",
        wait_for_completion=False,
        skip_when_already_exists=True,
        fail_when_dag_is_paused=True,
    )

    generator_result >> trigger_warehouse_pipeline
```

`skip_when_already_exists=True`: 같은 `logical_date`로 재트리거 시 Airflow가 결정적으로 생성하는 `run_id`가 이미 존재하면 Task를 `skipped`로 끝낸다(Task 실패 아님). `fail_when_dag_is_paused=True`: `warehouse_pipeline_dag`가 paused 상태면 Trigger Task를 명시적으로 실패시켜 무증상 미실행을 막는다. `trigger_rule`은 지정하지 않아 기본값 `all_success`가 적용되며, `run_source_simulation`이 실패하면 이 Task는 `upstream_failed`로 자동 건너뛴다.

- [ ] **Step 3: DAG Import 검증 (컨테이너)**

```bash
docker compose --profile airflow up -d
docker compose --profile airflow exec -T airflow-scheduler airflow dags list-import-errors
```

Expected: 빈 출력 또는 "No data found" — Import Error 0건.

```bash
docker compose --profile airflow exec -T airflow-scheduler airflow dags list
```

Expected: `source_simulation_dag`, `warehouse_pipeline_dag` 둘 다 출력에 포함.

- [ ] **Step 4: Task Graph 확인**

```bash
docker compose --profile airflow exec -T airflow-scheduler airflow tasks list source_simulation_dag
```

Expected: `run_source_simulation`, `trigger_warehouse_pipeline` 두 Task ID 출력.

- [ ] **Step 5: Commit**

```bash
git add airflow/dags/source_simulation_dag.py
git commit -m "$(cat <<'EOF'
feat: Generator 성공 후 Warehouse DAG 자동 트리거

같은 logical_date로 Warehouse를 트리거하되, skip_when_already_exists로
중복 트리거 시 run_id 충돌 실패 대신 skip 처리하고
fail_when_dag_is_paused로 Warehouse paused 상태의 무증상 미실행을 막는다.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: 실행 순서·중복 트리거·실패 전파 테스트

**Files:**
- Modify: `tests/test_airflow_dags.py`

**Interfaces:**
- Consumes: Task 1에서 만든 `trigger_warehouse_pipeline` Task ID, 기존 `_run_compose()` 헬퍼(같은 파일 26-33번째 줄)
- Produces: 없음 (테스트 파일, 이후 Task가 참조하지 않음)

세 시나리오 모두 같은 `RUN_AIRFLOW_SMOKE_TEST=1` opt-in 아래, `airflow dags test` CLI로 검증한다. 기존 파일의 `pytestmark`(17-23번째 줄)를 그대로 재사용하므로 새 파일을 만들지 않는다.

- [ ] **Step 1: 세 테스트 함수를 `tests/test_airflow_dags.py`에 추가**

기존 파일 끝(59번째 줄, `test_dags_import_without_errors` 함수 다음)에 이어 붙인다:

```python
def test_generator_success_triggers_warehouse_with_same_logical_date() -> None:
    """Generator DagRun 성공 뒤 같은 logical_date로 Warehouse DagRun이 생성되는지 검증한다."""
    build = _run_compose("build", "airflow-scheduler")
    assert build.returncode == 0, build.stderr

    up = _run_compose("up", "-d")
    assert up.returncode == 0, up.stderr

    try:
        logical_date = "2026-09-11T00:00:00+00:00"
        trigger = _run_compose(
            "exec",
            "-T",
            "airflow-scheduler",
            "airflow",
            "dags",
            "test",
            "source_simulation_dag",
            logical_date,
            "--conf",
            '{"logical_date": "2026-09-11T00:00:00+00:00", "orders": 1}',
        )
        assert trigger.returncode == 0, trigger.stdout + trigger.stderr

        listed = _run_compose(
            "exec",
            "-T",
            "airflow-scheduler",
            "airflow",
            "dags",
            "list-runs",
            "--dag-id",
            "warehouse_pipeline_dag",
        )
        assert listed.returncode == 0, listed.stderr
        assert logical_date in listed.stdout, (
            f"Warehouse DagRun with logical_date={logical_date} not found:\n{listed.stdout}"
        )
    finally:
        _run_compose("down")


def test_duplicate_generator_run_skips_second_warehouse_trigger() -> None:
    """같은 logical_date로 Generator를 두 번 실행하면 두 번째 Trigger Task는 skip되고 Warehouse DagRun은 1개로 유지되는지 검증한다."""
    build = _run_compose("build", "airflow-scheduler")
    assert build.returncode == 0, build.stderr

    up = _run_compose("up", "-d")
    assert up.returncode == 0, up.stderr

    try:
        logical_date = "2026-09-11T00:00:00+00:00"
        conf = '{"logical_date": "2026-09-11T00:00:00+00:00", "orders": 1}'

        first = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "test", "source_simulation_dag", logical_date, "--conf", conf,
        )
        assert first.returncode == 0, first.stdout + first.stderr

        second = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "test", "source_simulation_dag", logical_date, "--conf", conf,
        )
        assert second.returncode == 0, second.stdout + second.stderr
        assert "trigger_warehouse_pipeline" in second.stdout

        listed = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "list-runs", "--dag-id", "warehouse_pipeline_dag",
        )
        assert listed.returncode == 0, listed.stderr
        run_count = listed.stdout.count(logical_date)
        assert run_count == 1, (
            f"Expected exactly 1 warehouse_pipeline_dag run for {logical_date}, found {run_count}:\n{listed.stdout}"
        )
    finally:
        _run_compose("down")


def test_paused_warehouse_dag_fails_trigger_task() -> None:
    """warehouse_pipeline_dag가 paused 상태면 Trigger Task가 명시적으로 실패하는지 검증한다."""
    build = _run_compose("build", "airflow-scheduler")
    assert build.returncode == 0, build.stderr

    up = _run_compose("up", "-d")
    assert up.returncode == 0, up.stderr

    try:
        pause = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "pause", "warehouse_pipeline_dag",
        )
        assert pause.returncode == 0, pause.stdout + pause.stderr

        logical_date = "2026-09-11T06:00:00+00:00"
        conf = '{"logical_date": "2026-09-11T06:00:00+00:00", "orders": 1}'
        result = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "test", "source_simulation_dag", logical_date, "--conf", conf,
        )
        assert result.returncode != 0, (
            "warehouse_pipeline_dag가 paused일 때 trigger_warehouse_pipeline Task는 실패해야 한다"
        )
        assert "trigger_warehouse_pipeline" in result.stdout + result.stderr
    finally:
        unpause = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "unpause", "warehouse_pipeline_dag",
        )
        assert unpause.returncode == 0, unpause.stdout + unpause.stderr
        _run_compose("down")


def test_generator_failure_does_not_trigger_warehouse() -> None:
    """Generator 실행이 실패하면 Trigger Task가 upstream_failed로 건너뛰고 Warehouse DagRun이 생성되지 않는지 검증한다."""
    build = _run_compose("build", "airflow-scheduler")
    assert build.returncode == 0, build.stderr

    up = _run_compose("up", "-d")
    assert up.returncode == 0, up.stderr

    try:
        logical_date = "2026-09-11T12:00:00+00:00"
        failing_conf = '{"logical_date": "2026-09-11T12:00:00+00:00", "orders": -1}'

        result = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "test", "source_simulation_dag", logical_date, "--conf", failing_conf,
        )
        assert result.returncode != 0, "orders=-1은 ParamValidationError로 실행이 차단되어야 한다"

        listed = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "list-runs", "--dag-id", "warehouse_pipeline_dag",
        )
        assert listed.returncode == 0, listed.stderr
        assert logical_date not in listed.stdout, (
            f"Generator 실패에도 warehouse_pipeline_dag DagRun이 생성됨:\n{listed.stdout}"
        )
    finally:
        _run_compose("down")
```

- [ ] **Step 2: 테스트 실행 전 컨테이너 정리 확인**

```bash
docker compose --profile airflow ps
```

Expected: `airflow-*` 컨테이너가 없거나 정지 상태(테스트 각자가 `up`/`down`을 관리하므로 겹치면 안 됨).

- [ ] **Step 3: 세 테스트 실행**

```bash
RUN_AIRFLOW_SMOKE_TEST=1 uv run pytest tests/test_airflow_dags.py -v
```

Expected: 5 passed (기존 `test_dags_import_without_errors` + 신규 4건).

- [ ] **Step 4: 기본 pytest 실행에서 skip 유지 확인**

```bash
uv run pytest tests/test_airflow_dags.py -v
```

Expected: 5 skipped, 환경변수 미설정 사유로 전부 skip.

- [ ] **Step 5: Commit**

```bash
git add tests/test_airflow_dags.py
git commit -m "$(cat <<'EOF'
test: Generator-Warehouse 트리거 순서·중복·paused·실패 전파 검증 추가

같은 logical_date 트리거 순서, 중복 실행 시 Warehouse DagRun 1개 유지,
Warehouse paused 시 Trigger Task 명시적 실패, Generator 실패 시 Warehouse
미실행을 RUN_AIRFLOW_SMOKE_TEST opt-in으로 검증한다.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Phase 4 문서 Table 수 동기화

**Files:**
- Modify: `docs/phases/phase-04-airflow-orchestration.md`

**Interfaces:**
- Consumes: `airflow/dags/warehouse_pipeline_dag.py`의 `SOURCE_TABLES`(42-52번째 줄, 이미 9개로 구현됨 — 이 Task는 문서만 수정)
- Produces: 없음 (문서 전용)

아래 각 위치는 코드(`SOURCE_TABLES`, 9개)와 문서 서술이 불일치하는 지점이다. "6개"/"7개"가 전부 Table 수를 가리키는 것은 아니므로 — 328번째 줄의 "6개 Non-retryable 예외"와 127번째 줄의 "최대 6개 Process 병렬도"는 Table 수와 무관하니 건드리지 않는다.

- [ ] **Step 1: 원천 데이터 동시성 잠금 설명 (17번째 줄)**

`docs/phases/phase-04-airflow-orchestration.md`:

```
- old: Warehouse가 6개 Table의 Snapshot을 읽는 동안 Generator의 원천 변경을 막아, Table 사이에 서로 다른
+ new: Warehouse가 9개 Table의 Snapshot을 읽는 동안 Generator의 원천 변경을 막아, Table 사이에 서로 다른
```

- [ ] **Step 2: 선행 조건의 Bronze Object 수 (99번째 줄)**

```
- old: - 기존 6개 Bronze Object는 최초 Warehouse DAG 전에 `sync_bronze_catalog`로 1회 동기화하거나,
+ new: - 기존 9개 Bronze Object는 최초 Warehouse DAG 전에 `sync_bronze_catalog`로 1회 동기화하거나,
```

- [ ] **Step 3: P4-08 체크리스트 항목 (209번째 줄)**

```
- old: - [x] `P4-08` 6개 Table `extract_validate_load` Dynamic Task Mapping
+ new: - [x] `P4-08` 9개 Table `extract_validate_load` Dynamic Task Mapping
```

- [ ] **Step 4: P4-08 본문과 Table 나열 (216-221번째 줄)**

```
- old: `P4-08`은 `partial(...).expand(...)`로 고정된 아래 6개 Table만 확장한다. 병렬도는 Source와
  Object Storage 용량을 고려해 `max_active_tis_per_dag`로 제한하고, Map 입력 순서는 고정한다.

  ```text
  customers → products → sellers → orders → order_items → order_payments
  ```
+ new: `P4-08`은 `partial(...).expand(...)`로 고정된 아래 9개 Table만 확장한다. 병렬도는 Source와
  Object Storage 용량을 고려해 `max_active_tis_per_dag`로 제한하고, Map 입력 순서는 고정한다.

  ```text
  customers → customer_subscriptions → customer_membership_tiers → subscription_payments →
  products → sellers → orders → order_items → order_payments
  ```
```

- [ ] **Step 5: P4-09 본문 (224번째 줄)**

```
- old: Batch의 6개 Table이 모두 `COMMITTED`인지 확인하고, Manifest/Object/Hash/Row Count/Watermark를
+ new: Batch의 9개 Table이 모두 `COMMITTED`인지 확인하고, Manifest/Object/Hash/Row Count/Watermark를
```

- [ ] **Step 6: Task Graph 블록 (229-241번째 줄)**

```
- old: ```text
  initialize_run
      ↓
  acquire_source_snapshot_lease
      ↓
  extract_validate_load[
      customers,
      products,
      sellers,
      orders,
      order_items,
      order_payments
  ]
+ new: ```text
  initialize_run
      ↓
  acquire_source_snapshot_lease
      ↓
  extract_validate_load[
      customers,
      customer_subscriptions,
      customer_membership_tiers,
      subscription_payments,
      products,
      sellers,
      orders,
      order_items,
      order_payments
  ]
```

- [ ] **Step 7: 검증 문단의 실행 로그 서술 (268번째 줄)**

```
- old: - `airflow dags test warehouse_pipeline_dag`로 6개 Table Dynamic Task Mapping이 전부 실행되고,
+ new: - `airflow dags test warehouse_pipeline_dag`로 9개 Table Dynamic Task Mapping이 전부 실행되고,
```

- [ ] **Step 8: XCom 검증 문단의 Table 순서 서술 (375-376번째 줄)**

```
- old: - Dynamic Task Mapping 입력 순서가 `customers, customer_memberships, products, sellers, orders, order_items, order_payments`
  고정임을 `SOURCE_TABLES` 튜플로 확인했다.
+ new: - Dynamic Task Mapping 입력 순서가 `customers, customer_subscriptions, customer_membership_tiers,
  subscription_payments, products, sellers, orders, order_items, order_payments` 9개 고정임을
  `SOURCE_TABLES` 튜플로 확인했다.
```

- [ ] **Step 9: DAG 검증 섹션 (393번째 줄)**

```
- old: - Dynamic Task가 정확히 6개 Table로 확장
+ new: - Dynamic Task가 정확히 9개 Table로 확장
```

- [ ] **Step 10: "구독·등급 전환으로 재작업할 범위" 섹션을 완료 서술로 정리 (459-471번째 줄)**

이 섹션은 이미 target 9개를 설명하고 있었고, 위 Step 1-9로 본문이 그 내용을 실제로 반영했으므로 "재작업할 범위"(미래형)가 아닌 "완료됨"(과거형) 서술로 교체한다:

```
- old: ## 구독·등급 전환으로 재작업할 범위

  Dynamic Task Mapping 입력이 7개 Table 기준이다. PRD v1.8의 구독·등급 분리로 아래를
  갱신한다.

  - `airflow/dags/warehouse_pipeline_dag.py`의 Dynamic Mapping 입력 순서를
    `customers, customer_subscriptions, customer_membership_tiers, subscription_payments,
    products, sellers, orders, order_items, order_payments` 9개로 바꾼다.
  - Generator DAG가 새 구독 Profile과 만료 스캔을 호출하도록 Task 인자를 넓힌다.
  - DAG Parse Smoke Test를 9개 Table 기준으로 다시 통과시킨다.

  세부 순서는 [전환 계획](../architecture/02-subscription-membership-transition-plan.md) 5절에
  있다.
+ new: ## 구독·등급 전환 반영 완료

  Dynamic Task Mapping 입력을 `customers, customer_subscriptions, customer_membership_tiers,
  subscription_payments, products, sellers, orders, order_items, order_payments` 9개로
  갱신했다. Generator DAG는 Phase 2 API(`run_generator`)를 그대로 호출하며 별도 구독
  Profile 인자 확장 없이 기존 Param 구조를 유지한다. DAG Parse Smoke Test는 9개 Table
  기준으로 통과한다.

  전환 세부 배경은 [전환 계획](../architecture/02-subscription-membership-transition-plan.md)
  5절에 있다.
```

- [ ] **Step 11: 파일·폴더별 변경 요약 표에 Trigger 연동 행 추가 (491-515번째 줄, 표 마지막에 추가)**

`docs/phases/phase-04-airflow-orchestration.md`의 "파일·폴더별 변경 요약" 표(494번째 줄부터 시작) 마지막 행 뒤에 추가:

```
| `airflow/dags/source_simulation_dag.py`         | 수정 | Generator 성공 뒤 같은 `logical_date`로 `warehouse_pipeline_dag`를 자동 트리거하는 `TriggerDagRunOperator` Task를 추가했다. `skip_when_already_exists`로 중복 트리거를 skip 처리하고 `fail_when_dag_is_paused`로 Warehouse paused 상태의 무증상 미실행을 막는다. |
| `tests/test_airflow_dags.py`                    | 수정 | Generator-Warehouse 트리거 순서, 중복 트리거 시 Warehouse DagRun 1개 유지, Generator 실패 시 Warehouse 미실행을 검증하는 테스트 3건을 `RUN_AIRFLOW_SMOKE_TEST=1` opt-in으로 추가했다. |
```

- [ ] **Step 12: Table 수 불일치 재검색으로 누락 확인**

```bash
grep -n "6개 Table\|7개 Table\|customer_memberships" docs/phases/phase-04-airflow-orchestration.md
```

Expected: 빈 출력 (모든 옛 Table 수/이름 서술 제거 확인).

- [ ] **Step 13: Commit**

```bash
git add docs/phases/phase-04-airflow-orchestration.md
git commit -m "$(cat <<'EOF'
docs: Phase 4 문서의 Table 수 서술을 실제 9개 기준으로 동기화

SOURCE_TABLES가 이미 9-Table(구독·등급 분리)로 구현된 상태에서 남아있던
6개/7개 Table 서술과 옛 customer_memberships 단일축 이름을 정리하고,
Generator-Warehouse 트리거 연동을 변경 요약에 반영한다.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: 전체 회귀 테스트

**Files:**
- 없음 (검증 전용, 파일 변경 없음)

**Interfaces:**
- Consumes: Task 1-3의 모든 변경
- Produces: 없음

- [ ] **Step 1: 기본 테스트 스위트 실행**

```bash
uv run pytest tests/ -v
```

Expected: 기존 통과 건수 유지, 신규 실패 없음. `test_airflow_dags.py`의 4건은 skip으로 표시.

- [ ] **Step 2: Airflow Smoke Test 전체 재실행**

```bash
RUN_AIRFLOW_SMOKE_TEST=1 uv run pytest tests/test_airflow_dags.py -v
```

Expected: 5 passed.

- [ ] **Step 3: 컨테이너 정리 확인**

```bash
docker compose --profile airflow ps
```

Expected: `airflow-*` 컨테이너 없음(각 테스트의 `finally` 블록이 `down` 처리).

- [ ] **Step 4: 최종 git log 확인**

```bash
git log --oneline -4
```

Expected: Task 1-3의 커밋 3건이 순서대로 보임.
