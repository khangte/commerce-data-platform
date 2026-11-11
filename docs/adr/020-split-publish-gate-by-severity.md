# ADR 020. Publish Gate를 Test 심각도로 나눈다

## Status

Accepted (2026-11-10). ADR 016을 보완한다. 016의 Build-then-swap 구조와 파일 단위 원자 교체는 그대로다.

## Context

ADR 016은 복사한 Build 파일에서 `dbt build`를 돌리고, 모든 Test가 통과한 Build만 Published 파일로 교체한다. 어떤 Test든 실패하면 마지막 성공 Mart가 남는다. 게시본의 모든 Table이 같은 Build에서 나오고 서로 어긋나지 않는다는 보장을 얻기 위한, 일관성 우선 설계다. 이 선택은 지금도 유효하며 Fact·Dimension에는 그대로 적용한다.

운영 시나리오를 가정해 보면 이 보장의 비용이 드러난다. Gate에는 성격이 다른 Test가 섞여 있다.

- Fact·Dimension Test(PK·FK·SCD2·금액 정합성): 실패한 채 게시되면 하위 지표 전부가 오염된다.
- 지표 계층(`rpt_*`) Test: Fact에서 다시 계산할 수 있는 Leaf 파생물의 검증이다. 하위 Model이 없다.

전체 Gate에서는 두 번째 종류 하나만 실패해도 멀쩡한 주문 Fact까지 게시되지 못한다. 재현 실험([07](../benchmarks/07-publish-gate-split.md))에서 지표 Canary 하나를 실패시키자 Fact·Dimension Test는 모두 통과했는데도 `fct_order` 5행(주문 시각 600초분)이 게시본에 도달하지 못했다. 원천 주기 `@hourly`에서 실패가 k회 반복되면 주문 Fact 지연은 약 k시간이 되고, 2~3회째부터 `DATA_STALE`이 켜진다(계산값).

## Decision

1. **분류 기준은 계층 경로다.** `marts/metrics`에 붙은 Test(Generic 9개, `rpt_*` Singular 3개, 지표 Canary 1개)는 `severity: warn`, 그 외(staging·intermediate·dimensions·facts)는 기본값 `error`를 유지한다.
2. **Gate 판정 코드는 바꾸지 않는다.** `parse_run_results`는 원래 `error`·`fail`만 실패로 본다. 설정만으로 지표 Test가 Gate에서 빠진다.
3. **dbt var `metrics_test_severity`** (기본 `warn`). `error`를 주면 016의 전체 Gate와 같은 동작이다. 재현 실험의 전/후 비교와 롤백에 쓴다.
4. **경고 게시는 기록한다.** `mart_publish_runs.tests_warned`에 건수를 남기고, Run report 판정에 `DBT_TEST_WARN`을 추가해 해당 실행을 `ok=false`로 만든다.
5. `tests_warned`는 공용 Record 조회에 넣지 않고 전용 조회로 읽는다. 마이그레이션 없이 읽는 Serving Reader를 깨뜨리지 않기 위해서다([063](../architect-review/063_publish-gate-split-tests-warned-reader-compat.md)).

## Alternatives

| 대안 | 기각 사유 |
| ---- | --------- |
| Test별로 심각도를 따로 판단 | 기준이 사람마다 달라지고 Test가 늘 때마다 다시 정해야 한다. 계층 경로는 Model의 역할(Leaf 파생물)에서 바로 나온다. |
| `dbt build --exclude`로 지표 Test 제외 | 실패가 기록조차 되지 않는다. 경고로 남겨야 보완할 수 있다. |
| 지표 Model을 별도 dbt 실행으로 분리 | Publish 절차와 Task Graph가 바뀐다. 설정 변경만으로 같은 효과를 얻는다. |
| 도메인별 게시 분리로 바로 이동 | 파일 1개 통째 교체 구조를 건드려야 한다. 비용이 작은 단계부터 수치로 확인한다(설계: `docs/superpowers/specs/2026-11-10-publish-gate-domain-split-design.md`). |

## Consequences

| 구분 | 내용 |
| ---- | ---- |
| 포기 | 게시 파일 안에서 `rpt_*`가 Fact와 어긋날 수 있다(계층 간 정합성). |
| 유지 | 파일 단위 원자 교체(모든 Table이 같은 Build 출신), Fact·Dimension 무결성 Gate, 실패 Build 격리. |
| 보완 | `tests_warned`와 `DBT_TEST_WARN`으로 경고 게시를 드러낸다. `metrics_test_severity=error`로 전체 Gate에 돌아갈 수 있다. |
| 미보완 | BI 사용자는 수정 전까지 어긋난 지표를 본다. Dashboard에는 표시되지 않는다. |

알려진 한계:

- **어떤 Test가 경고였는지는 저장되지 않는다.** 건수만 남는다. dbt 출력은 실패 때만 기록되고 `run_results.json`은 게시 성공 뒤 지워진다. 식별 절차는 [R-14 Runbook](../runbooks/r14-dbt-failure.md)에 있다. 이후 [ADR 021](021-split-publish-gate-by-domain.md) 구현에서 경고 Node 이름을 `dbt_build` 로그에 남기게 했다.
- DAG에는 dbt var를 넘기는 Param이 없다. 운영 DAG에서 전체 Gate로 돌아가려면 `dbt/dbt_project.yml`의 기본값을 바꿔야 한다. CLI 게시는 `--dbt-vars`로 즉시 가능하다.
- 분리 단위가 계층 전체다. Fact·Dimension Test 실패는 여전히 관계없는 도메인까지 막는다. 이것이 도메인별 분리([ADR 021](021-split-publish-gate-by-domain.md))의 동기다.

## Validation

| 근거 | 결과 |
| ---- | ---- |
| 재현 실험 `tests/reliability/test_gate_split_experiment.py` | v1(`error`): `FAILED`/`DBT_TEST_ERROR`, 게시본 불변, 주문 5행 묶임. v2(`warn`): `PUBLISHED`, `tests_warned = 1`, 묶인 주문 0행, `DBT_TEST_WARN` |
| R-14·기존 Canary Gate 회귀 | 통과. `error` Gate 동작은 그대로다 |
| `dbt ls --select config.severity:warn` | 기본 13개, `metrics_test_severity=error`에서 0개 |
