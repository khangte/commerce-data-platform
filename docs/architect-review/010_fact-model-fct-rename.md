# 010. Fact Model 이름을 `fct_*` 계약으로 맞춘다

- 판정: **변경 확정 — 구현을 계약에 맞춘다 (사용자 결정)**
- 선행: `002_mart-grain-contract-drift.md` 2장의 "Fact Model 이름" 항목
- 기록일: 2026-09-18
- 브랜치: `feature/phase6-remaining`

## 1. 결정

Fact Model 이름은 `docs/reference/mart-grain.md` 3장의 계약 이름을 따른다.

| 현재 구현 | 변경 후 |
| --------- | ------- |
| `fact_orders` | `fct_order` |
| `fact_order_items` | `fct_order_item` |
| `fact_payments` | `fct_order_payment` |
| `fact_subscription_payments` | `fct_subscription_payment` |

002 4장 마지막 문단은 "Fact 이름은 문서를 구현에 맞춘다"고 제안했다. 사용자가 반대 방향으로 결정했다. 이 문서가 002의 그 제안을 대체한다.

## 2. 범위

이름만 바꾼다. 컬럼, Grain, `unique_key`, Incremental 전략, Test 조건은 바꾸지 않는다.

범위 밖 (002에 보류로 남는다):

- `fct_order_payment`의 `payment_sequential`(계약) vs `payment_sequence`(구현) 컬럼명 불일치

## 3. 변경 목록

### 3.1 dbt Model (`git mv`로 이름 변경)

| 현재 경로 | 변경 경로 |
| --------- | --------- |
| `dbt/models/marts/facts/fact_orders.sql` | `dbt/models/marts/facts/fct_order.sql` |
| `dbt/models/marts/facts/fact_order_items.sql` | `dbt/models/marts/facts/fct_order_item.sql` |
| `dbt/models/marts/facts/fact_payments.sql` | `dbt/models/marts/facts/fct_order_payment.sql` |
| `dbt/models/marts/facts/fact_subscription_payments.sql` | `dbt/models/marts/facts/fct_subscription_payment.sql` |

`fct_order_item.sql`, `fct_order_payment.sql` 9행 주석의 Test 경로도 3.2의 새 이름으로 바꾼다.

### 3.2 dbt Singular Test

| 현재 경로 | 변경 |
| --------- | ---- |
| `dbt/tests/fact_order_items_unique.sql` | `git mv` → `dbt/tests/fct_order_item_unique.sql`, `ref('fct_order_item')` |
| `dbt/tests/fact_payments_unique.sql` | `git mv` → `dbt/tests/fct_order_payment_unique.sql`, `ref('fct_order_payment')` |
| `dbt/tests/fct_subscription_payment_completed_value_consistent.sql` | 파일명 유지, `ref('fct_subscription_payment')` |
| `dbt/tests/fct_subscription_payment_cycle_attempt_unique.sql` | 파일명 유지, `ref('fct_subscription_payment')` |
| `dbt/tests/fct_subscription_payment_failure_code_consistent.sql` | 파일명 유지, `ref('fct_subscription_payment')` |

새 Test 파일명은 기존 `fct_subscription_payment_*` Test의 명명 규칙을 따른다.

### 3.3 dbt `schema.yml`과 Downstream

- `dbt/models/marts/facts/schema.yml`: `models[].name` 4개와 description 안의 Test 경로 2개
- `dbt/models/marts/metrics/rpt_membership_tier_performance.sql`: `ref('fct_order')`. 테이블 별칭 `fact_orders`도 `fct_order`로 바꾼다.
- `dbt/models/marts/metrics/rpt_subscription_payment_outcomes_daily.sql`: `ref('fct_subscription_payment')`

### 3.4 Watermark Macro (누락 시 조용히 고장난다)

`dbt/macros/processed_batch_watermark.sql` 40–45행 `required_facts`의 `unique_id` 4개를 새 이름으로 바꾼다.

이 목록은 문자열 비교다. 바꾸지 않으면 dbt 컴파일은 통과한다. 대신 매 Build마다 "facts not in this selection" 로그만 남고 Watermark가 영원히 전진하지 않는다.

### 3.5 Python

- `src/warehouse/mart_hash.py` 41–44행: `MartTarget` Relation 이름 4개
- `tests/test_fact_incremental_contract.py`: Model 경로 4개
- `tests/test_fact_layer_contract.py`: Model 경로 2개, 함수명 `test_fact_orders_projects_the_fact_ready_intermediate` → `test_fct_order_projects_the_fact_ready_intermediate`
- `tests/test_mart_hash.py` 113–116, 137행: `facts.fact_orders` → `facts.fct_order`, `facts.fact_payments` → `facts.fct_order_payment`
- `tests/integration/test_order_e2e_and_late_order_mart_integration.py`: `facts.fact_orders` 2곳
- `tests/integration/test_subscription_payment_temporal_join_integration.py`: `facts.fact_subscription_payments` 8곳

### 3.6 문서

- `docs/reference/mart-grain.md`: 이미 `fct_*`를 쓴다. 수정하지 않는다. 3.2의 새 Test 파일명과 충돌하는 경로 언급이 없는지만 확인한다.
- `docs/phases/phase-07-data-quality-publish.md` 31행: 계획 중인 Test 이름 `fact_subscription_payments_missing_customer_key.sql` → `fct_subscription_payment_missing_customer_key.sql`. Phase 7은 아직 Planned 상태이므로 다음 구현자가 옛 이름을 따라 쓰지 않게 한다.
- `docs/architect-review/002`: 종결 후 architect가 후속 줄을 추가한다.

손대지 않는 문서:

- `docs/architect-review/003`–`009`, `docs/architecture/02`–`07`, `docs/phases/phase-06-*`, `docs/superpowers/plans/*`: 당시 판정·설계 기록이다. 옛 이름은 기록 시점의 사실이다. 이름 대응은 이 문서 1장이 정본이다.
- `PRD_v1.12.md`: 93, 102, 489, 1479, 1483행이 `fact_*`를 쓴다. PRD 수정은 버전 갱신 절차가 따르므로 이번 작업에 넣지 않고 lead에 별도로 올린다.

## 4. 로컬 Warehouse

`data/warehouse/warehouse.duckdb`에는 옛 Relation `facts.fact_orders` 등 4개가 남는다. dbt는 이름이 바뀐 Model의 옛 Relation을 지우지 않는다.

- 새 `fct_*` Relation은 처음 Build에서 없는 상태이므로 전체 적재된다. 별도 `--full-refresh`가 필요 없다.
- 옛 Relation 4개는 로컬에서 `DROP TABLE IF EXISTS facts.fact_*`로 지운다. Warehouse 파일은 git에 들어가지 않으므로 커밋 대상이 아니다.
- `mart_hash`는 `MartTarget` 목록만 읽으므로 옛 Relation이 남아 있어도 비교 결과에 영향이 없다. 그래도 혼동을 막기 위해 지운다.

## 5. 완료 조건

1. 잔존 검색이 0건이다.

   ```bash
   git grep -nE "fact_(orders|order_items|payments|subscription_payments)" -- dbt src tests docs/reference docs/phases/phase-07-data-quality-publish.md
   ```

2. `dbt build`가 전체 성공한다. 로그에 `Skipping watermark advance: facts not in this selection`이 없다.
3. `pytest` 전체(통합 Test 포함)가 통과한다.
4. 로컬 Warehouse의 `facts` Schema에 `fct_*` 4개만 있고 `fact_*`가 없다.
