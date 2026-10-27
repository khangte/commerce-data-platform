# Phase 6. Dimensional Modeling

> 상태: Done  
> Milestone: 2 — Data Platform Core  
> 선행 Phase: [Phase 5. Bronze Catalog + Staging](phase-05-bronze-catalog-and-staging.md)  
> 기준 문서: [ROADMAP](ROADMAP.md), [PRD v1.22](../../PRD.bak/PRD_v1.22.md), [Mart Grain 계약](../reference/mart-grain.md)
> 참고: [데이터 변환 흐름](../reference/data-transformation-flow.md) — 계층별 변환의 근거

## 목표

Staging을 입력으로 받아 Grain을 바꾸는 Intermediate와, Grain을 확정하는 Mart를 만든다. 분석가와 BI가 조회하는 Dimension, Fact, Report Model이 이 Phase의 산출물이다.

Phase 5와 분리한 이유는 설계 입력이 다르기 때문이다. Staging은 Source Schema만 있으면 만들 수 있지만, Intermediate와 Mart는 Grain 계약 없이 시작할 수 없다. 어떤 Dimension과 Fact를 둘지, 각 Model의 한 행이 무엇을 나타낼지가 먼저 정해져야 한다.

## 선행 조건

- Phase 5의 Staging Model이 원천 Grain을 보존한 채 분석 Naming으로 노출된다.
- **[Mart Grain 계약](../reference/mart-grain.md)이 확정됐다.** 이 문서 없이는 이 Phase를 시작하지 않는다.
- Phase 2의 구독 상태 전이·등급 변경·Address 변경 Fixture가 존재한다.

### Grain 계약이 확정돼야 하는 항목

Phase 6 착수 전에 아래가 [Mart Grain 계약](../reference/mart-grain.md)에 정의돼 있어야 한다.

| 항목               | 없으면 생기는 문제                                     |
| ------------------ | ------------------------------------------------------ |
| Model 목록         | 무엇을 만들지 모른다                                   |
| Model별 Grain 문장 | 새 컬럼이 그 Model에 속하는지 판정할 수 없다           |
| Unique Key         | Grain 문장을 데이터로 강제할 수단이 없다               |
| Measure 계약       | 같은 이름의 지표가 Model마다 다른 값을 갖는다          |
| 이력 추적 방식     | 과거 사실을 현재 값으로 잘못 해석한다                  |
| Fan-out 방지 규칙  | Grain이 다른 입력을 결합해 Measure가 조용히 부풀려진다 |

## 핵심 계약

- Intermediate/Mart는 Raw Source Prefix를 직접 참조하지 않는다. 입력은 Staging이다.
- 모든 Mart는 문서화된 Grain과 `unique_key`를 가진다.
- Grain이 다른 입력은 Intermediate에서 목표 Grain으로 먼저 접는다. Mart는 준비된 행을 투영만 하고 집계·파생·시점 결합을 수행하지 않는다.
- 이력은 시점별로 겹치지 않는 구간을 이루고, Business Key별 Current Version은 정확히 하나다.
- 사건은 발생 시점에 유효했던 Version을 참조한다. 현재 Version을 참조하지 않는다.
- Incremental 결과는 동일 입력의 Full Refresh와 Logical Hash가 같아야 한다.

## Warehouse 구조

```text
data/warehouse/warehouse.duckdb

control       table
staging       view
intermediate  view
marts         table 또는 incremental
```

Mart 하위 Schema 구성은 Grain 계약에서 결정한 Model 분류를 따른다.

## Phase 6A. Grain 설계 확정

- [x] `P6-01` Dimension 후보와 각 Dimension의 Grain 문장 결정
- [x] `P6-02` Fact 후보와 각 Fact의 Grain 문장·Unique Key 결정
- [x] `P6-03` Measure 목록과 계산식, Additive 여부 결정
- [x] `P6-04` 이력 추적 대상 속성과 Version 생성 규칙 결정
- [x] `P6-05` 결정 사항을 [Mart Grain 계약](../reference/mart-grain.md)에 기록
- [x] `P6-26` Mart Grain 계약 §2·§3 컬럼표를 구현과 정합
- [x] `P6-27` Mart Grain 계약 §4 Report 계약 작성 (기존 `rpt_*` 3개)

Model을 만들기 전에 Grain 문장을 먼저 쓴다. 컬럼 목록만으로는 중복의 의미를 판정할 수 없다.

## Phase 6B. Intermediate

- [x] `P6-06` Staging Join으로 Mart 입력 준비
- [x] `P6-07` Grain이 다른 입력의 사전 집계
- [x] `P6-08` 이력 구간 생성
- [x] `P6-09` Mart가 투영할 행 준비 (파생 계산 포함)
- [x] `P6-10` Late Arrival 영향 범위 계산

집계·파생·이력 구간 생성은 Intermediate에서 끝낸다. Late Arrival 영향 범위는 주문 구매일, 연결 주문 구매일, 고객 변경 구간, Product/Seller 사용 주문일을 기준으로 계산한다.

## Phase 6C. Dimension

- [x] `P6-11` 단순 Dimension 구현
- [x] `P6-12` 이력 추적 Dimension 구현
- [x] `P6-13` Dimension Grain/Unique Key 검증 Test

이력 추적 Dimension은 Surrogate Key를 사용한다. 자연키는 Version마다 여러 행으로 존재해 Table 안에서 유일하지 않다. Surrogate Key는 재계산해도 같은 값이 나오는 결정적 방식으로 만든다.

최초 Version의 유효 시작 시각을 잘못 잡으면 그 이전에 발생한 사건이 유효한 Version을 찾지 못해 Unknown Key가 발생한다. AC-12를 위반한다.

## Phase 6D. Fact

- [x] `P6-14` Fact 구현
- [x] `P6-15` 모든 Fact의 `unique_key`와 Incremental 교체 구현
- [x] `P6-16` 사건 시점 기준 Dimension Version 결합
- [x] `P6-17` Fact Grain/Measure/Fan-out 방지 Test
- [x] `P6-18` 정상 E2E Unknown Key 0 검증
- [x] 056 승인에 따라 주문 결제 Fact에 UTC 주문 구매일 키를 추가하고, 날짜 FK·주문 Fact와의 일치·결제 행 보존·증분 영향 주문 필터를 로컬에서 검증했다.

Fact는 Intermediate가 준비한 행을 투영한다. Fact 안에서 집계하거나 시점 결합을 수행하면 Grain·Key·Measure 제공 책임과 계산 책임이 섞인다.

## Phase 6E. Report Model

- [x] `P6-19` BI가 소비할 Report Model 구현
- [x] `P6-20` Report Model Grain/Unique Key 검증 Test
- [x] dbt 정리: 주문 항목의 미사용 차원 결합과 재가입 계산의 도달 불가능한 `TRIAL` 분기를 제거했다. architect 판정에 따라 가산 측정값 `order_count`(주문 건수)는 유지했다. 운영·실패 보관 파일 4개의 신규 감사 스키마 확인에 따라 구 테이블 감지·삭제 분기도 제거했다.

Report Model은 Mart 위에서 파생된다. BI가 Source·Bronze·Staging을 직접 조회하지 않도록 Mart만 읽는 소비 계층을 제공한다. 날짜 축은 필요한 가장 낮은 Grain으로 유지한다. 접힌 Grain은 되돌릴 수 없지만 펼쳐진 Grain은 BI가 roll-up할 수 있다.

## Phase 6F. Incremental과 Late Arrival

- [x] `P6-21` 변경 Key 기반 Transactional `DELETE + INSERT` 또는 검증된 `MERGE`
- [x] `P6-22` 영향 Key/Business Date 재계산
- [x] `P6-23` Incremental과 Full Refresh Logical Hash 비교
- [x] `P6-24` Bronze Replay와 Re-extract 입력 경계 제공
- [x] `P6-25` Phase 4 Warehouse DAG의 `P4-11` dbt Build 호출 경계 활성화

## 현재 구현 현황

현재 Model은 [Mart Grain 계약](../reference/mart-grain.md)을 기준으로 구현·검증한다. v1.12에서
구독 계약 이력은 `dim_subscription`으로 분리했고, `dim_customer`에는 거래 실적 등급 이력만 남긴다.

| 계층         | Model                                                                                                                                                                                                                                              |
| ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Intermediate | `int_orders_enriched`, `int_order_items_enriched`, `int_order_item_totals`, `int_payment_summary`, `int_order_fact_ready`, `int_subscription_payments_enriched`, `int_subscription_history`, `int_customer_history`, `int_affected_business_dates` |
| Dimension    | `dim_customer`, `dim_date`, `dim_product`, `dim_seller`, `dim_subscription`                                                                                                                                                                        |
| Fact         | `fact_orders`, `fact_order_items`, `fact_payments`, `fact_subscription_payments`                                                                                                                                                                   |
| Report       | `rpt_subscription_funnel_daily`, `rpt_subscription_payment_outcomes_daily`, `rpt_membership_tier_performance`                                                                                                                                      |

### 기존 구현을 다루는 방식

- Mart 범위는 현행 계약으로 고정한다. Phase 6 완료 전에는 새 Fact·Dimension을 추가하지 않는다.
- 계약과 어긋난 사람 단위 구독 상태·재가입 파생 컬럼과 Test는 제거했고, 계약 단위 Model·Test로 교체했다.
- 남은 작업은 모델 범위 확장이 아니라 Late Arrival, Incremental 동등성, Bronze Replay, DAG 실행 경계의 운영 검증이다.

`warehouse.duckdb`의 현재 결과는 위 계약과 검증을 만족할 때만 다음 Phase의 입력으로 사용한다.

## 범위 밖

- Staging Model 구현과 Source Naming 변환
- 최종 Publish Swap과 전체 품질 Gate
- 장애 Runbook 작성
- BI Dashboard 구성

## 테스트와 Gate

| AC    | 시나리오                      | 합격 증거                                     |
| ----- | ----------------------------- | --------------------------------------------- |
| AC-01 | E2E Fact 도달                 | 고정 주문의 Source→Bronze→Fact Count/Key 추적 |
| AC-09 | 이력 Version 생성             | Version 수, Overlap 0, Current 1              |
| AC-10 | 구간별 사건                   | 사건 시점에 유효한 Version 참조               |
| AC-11 | 3일 전 Late Order의 모델링 측 | 과거 Business Date Fact 재계산                |
| AC-12 | Referential Integrity         | Fact FK/Unique 통과, 정상 Unknown 0           |

추가 검증:

- Model별 Grain/Unique Key 중복 0
- Grain이 다른 입력 결합의 Fan-out 방지 SQL Test
- Measure 계산 Test
- 최초 Version 유효 시작 시각 규칙 Test
- 동일 Timestamp/다른 Attribute Hash Contract Error
- Incremental/Full Refresh Key별 값과 Logical Hash 일치

## 요구사항 추적

| 구분 | 연결 항목                                 |
| ---- | ----------------------------------------- |
| PRD  | Section 14.2 Intermediate                 |
| PRD  | Section 14.3 Mart Grain과 Measure 계약    |
| PRD  | Section 14.4 Incremental                  |
| PRD  | Section 15 이력 추적 요구사항             |
| PRD  | Section 16 Late Arrival, Backfill, Re-run |
| ADR  | [ADR-009 관측 기반 고객 이력](../adr/009-use-observed-history-for-customer-scd2.md) |
| ADR  | [ADR-011 Late Arrival 재처리 전략](../adr/011-late-arrival-reprocessing-strategy.md) |
| FR   | FR-10 dbt Intermediate/Mart               |
| FR   | FR-11 Star Schema/Fact Grain              |
| FR   | FR-14 Late Arrival 재처리                 |
| FR   | FR-15 이력 추적/시점 결합                 |

## 산출물

- 확정된 [Mart Grain 계약](../reference/mart-grain.md)
- Intermediate Model
- Dimension과 Fact Model
- Report Model
- Grain, Measure, 이력, Full Refresh 비교 Test
- Affected Key/Date 기반 Incremental 재계산

## 062 주문 구매 시각 보존 (2026-10-27)

- [x] 승인된 범위 변경에 따라 `int_order_fact_ready`(주문 Fact 입력 준비)와 `fct_order`(주문 Fact)에 원본 구매 시각 `purchase_at`을 그대로 투영하고 `not_null`(필수값) 검사를 추가했다.
- [x] 실패 테스트를 먼저 확인한 뒤 구현했고, Fact·품질 계약 테스트와 원천 주기·DAG 테스트 42개 통과·5개 생략을 확인했다. Mart Grain 계약에 타입·의미·지연 판정 용도를 반영했다.
- [x] 전체 새로고침 Run `ceddd951-b9e2-4aee-b0d5-dad6e2ceaf6a`가 `PUBLISHED`로 완료됐다. dbt 검사 144개 통과·0개 실패, 주문 99,829행의 구매 시각 NULL 0건, 최신 시각 `2026-10-19T11:50:00Z`를 확인했다. 변경된 관계는 `facts.fct_order` 하나다.

## 파일·폴더별 변경 요약

- [x] 주문 변경 이력 계획 Task 5: 현재 상태 Fact 네 개의 `incremental_strategy='delete+insert'`(교체 단위 삭제 후 삽입)를 계약 테스트로 고정했다.
- `tests/test_fact_incremental_contract.py`: `CURRENT_STATE_FACTS` 상수와 `test_current_state_facts_use_delete_insert`를 추가했다. 기존 테스트와 Fact SQL·설정은 유지한다. 주문 항목 전략을 일시적으로 `merge`로 바꿨을 때 해당 경로와 함께 실패함을 확인하고 원복했다.
- [x] Task 5 계약 테스트 4건 통과. 전체 오프라인 회귀 371건 통과(9건 생략, 통합 97건 제외). `merge` 변이 실패 근거: `/tmp/order-items-t45/strategy-red.txt`.
- [x] Task 5 reviewer 승인: 지적 0건. reviewer가 Ruff, 오프라인 371건 통과·9건 생략, 집중 테스트 7건 통과와 재추출 통합 테스트 통과를 독립 확인했다. 커밋은 lead가 수행한다.

- 구독 결제 상태 전환과 최종 테스트 결과는 [Phase 7 검증 기록](phase-07-data-quality-publish.md)을 참조한다.



- [x] 구독 결제 상태 표준화: Staging의 대문자 상태에 맞춰 성공 금액 계산과 Fact·Report 허용값을 변경했다.
- `dbt/models/intermediate/int_subscription_payments_enriched.sql`: `COMPLETED`일 때만 성공 결제 금액을 계산한다.
- `dbt/models/marts/facts/schema.yml`, `dbt/models/marts/metrics/schema.yml`: 구독 결제 Fact와 일별 결제 결과 Report의 허용값을 `COMPLETED`·`FAILED`로 변경했다.
- `docs/reference/mart-grain.md`, `docs/architecture/03-subscription-lifecycle-requirements.md`: 분석 계층의 상태·성공 금액·실패율 정의를 대문자 기준으로 맞췄다. 원천 스키마의 소문자 계약은 유지했다.
- 운영 반영: 코드 병합 뒤 첫 게시에는 반드시 `python -m src.warehouse.publish --full-refresh`를 사용한다. 게시 파일을 복사해 시작하는 증분 빌드는 기존 소문자 행을 보존하므로 허용값 검증에서 차단된다. 전체 새로고침은 모든 구독 결제 행과 Report를 재계산한다. `fct_subscription_payment` 해시는 한 번 바뀌며 변경 관계로 기록된다. 롤백 시에는 이전 코드로 복귀한 뒤 전체 새로고침으로 소문자 분석 데이터를 다시 만든다. 이 작업에서는 운영 게시를 수행하지 않는다.

- `dbt/models/intermediate/int_order_items_enriched.sql`: 사용하지 않는 상품·판매자 차원 결합을 제거하고 주문 Staging 결합은 유지했다.
- `dbt/models/marts/metrics/rpt_subscription_funnel_daily.sql`: 재가입을 이탈 후 활성 상태 전이로 한정했다.
- `dbt/macros/record_affected_keys.sql`, `tests/test_dbt_affected_keys_macro.py`: architect가 운영 1개·실패 보관 3개 파일의 신규 스키마 적용을 확인하여 구 `order_id` 열 감지·테이블 삭제 분기와 전용 테스트를 제거했다. 신규 도메인 스키마 선언과 양쪽 도메인 기록 검증은 유지했다.
- `tests/test_dbt_cleanup.py`: 차원 없이 다중 주문 항목·참조 주문 누락 행 보존, `TRIAL`의 NULL 처리와 재가입 계산을 검증한다.
- `docs/reference/mart-grain.md`: 재가입 정의와 그룹화로 보장하는 Report 유일성 설명을 실제 구현에 맞췄다.
- 검증: 변경 전 주문 항목 회귀 테스트에서 차원 의존성 실패를 확인했다. 최종 상태에서 `dbt parse`, `pytest -q tests/test_dbt_*.py tests/test_warehouse_quality_contract.py` 34건, 실제 PostgreSQL·SeaweedFS를 사용하는 `tests/integration/test_publish_gate_dbt_integration.py` 1건이 통과했다. 통합 테스트는 임시 Warehouse의 실제 dbt build 성공·Canary 실패 격리·재게시 성공과 해시 보존을 확인했다. Ruff와 `git diff --check`도 통과했다. 운영 Warehouse 파일은 변경하지 않았다.

| 경로                                                                                                    | 변경 내용                                                                                                                                  |
| ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `dbt/models/intermediate/int_order_fact_ready.sql`, `dbt/models/marts/facts/fct_order.sql`, `dbt/models/marts/facts/schema.yml`, `tests/test_fact_layer_contract.py`, `docs/reference/mart-grain.md` | 062: 주문 구매 시각을 Fact까지 보존하고 필수값·투영 계약과 컬럼 설명을 추가했다. 기존 증분 테이블은 `on_schema_change=ignore`이므로 전체 새로고침 1회가 필요하다. |
| `dbt/models/intermediate/int_customer_history.sql`                                                      | 고객 SCD2 입력을 거래 실적 등급 축만으로 축소했다.                                                                                         |
| `dbt/models/intermediate/int_subscription_history.sql`                                                  | 계약별 상태 관측을 SCD2 유효 구간으로 변환했다.                                                                                            |
| `dbt/models/marts/dimensions/dim_customer.sql`                                                          | 구독 상태·재가입 파생 컬럼을 제거하고 등급 Version만 투영하도록 축소했다.                                                                  |
| `dbt/models/marts/dimensions/dim_subscription.sql`, `dim_date.sql`                                      | `subscription_id` Business Key와 결정적 Version Key를 가진 계약 SCD2 Dimension을 추가하고, 계약 결제일까지 날짜 Dimension 범위를 확장했다. |
| `dbt/models/intermediate/int_subscription_payments_enriched.sql`                                        | 결제 시점의 계약·고객 Version과 날짜 Key를 결합해 Fact 입력을 준비하도록 수정했다.                                                         |
| `dbt/models/intermediate/int_order_payments_enriched.sql` | 056 승인에 따라 결제를 주문 Staging과 왼쪽 결합해 UTC 주문 구매일 `purchase_date_key`를 계산한다. 주문이 없는 결제도 보존해 NULL 키 검증에 잡히도록 하며, 고객 이력 모델은 참조하지 않는다. |
| `dbt/models/marts/facts/fct_order_payment.sql` | 새 Intermediate를 투영하고 `payment_sequence` 다음에 주문 구매일 키를 추가했다. 기존 결제 컬럼 순서, 주문 단위 증분 교체와 영향 주문 필터를 유지했다. |
| `dbt/models/marts/facts/schema.yml`, `dbt/tests/fct_order_payment_date_matches_order.sql` | 결제 구매일의 NULL 금지·날짜 Dimension 참조와 같은 주문 Fact의 구매일 일치를 품질 Gate로 강제했다. |
| `tests/test_order_payment_date_key.py`, `tests/test_warehouse_quality_contract.py` | 격리된 메모리 DuckDB로 UTC 경계·결제 사건 시각 부재·다중 결제·주문 참조 누락·증분 필터·날짜 불일치 탐지를 검증하고, 날짜 FK·Singular Test 선언을 계약 테스트에 추가했다. |
| `dbt/models/marts/facts/fct_subscription_payment.sql`                                                 | `payment_id` Grain의 계약별 구독 결제 시도 Fact를 구현했다.                                                                                |
| `dbt/models/marts/*/schema.yml`, `dbt/tests/dim_subscription_*`, `dbt/tests/fct_subscription_payment_*` | 계약 SCD2, 열린 계약, 청구 회차·시도, 성공 금액, 실패 코드 계약을 Schema 및 Singular Test로 강제했다.                                      |
| `tests/integration/test_subscription_payment_temporal_join_integration.py`                              | 계약 키·청구 회차·시도 순번 기준으로 Source→Bronze→dbt Fact의 계약·고객 Temporal Join을 E2E 검증하도록 갱신했다.                           |
| `docs/reference/mart-grain.md`                                                                          | P6-26으로 Dimension·Fact 컬럼표를 현행 dbt 출력과 정합시키고, P6-27으로 기존 Report 3개의 Grain·키·컬럼·제약을 기록했다. 056 승인으로 §3.3의 출처와 날짜 Dimension 참조·구매일 키 계약을 개정했다. |
| `README.md` | 056의 결제 구매일 FK를 ER 그림과 Fact 표에 반영했다. 작업 전부터 있던 트러블슈팅 표 변경은 보존했다. |
| `docs/phases/phase-06-dimensional-modeling.md`, `docs/phases/phase-07-data-quality-publish.md` | 056 구현 체크리스트, 변경 파일·동작과 로컬 검증 결과를 기록했다. |
| `data/warehouse/warehouse.duckdb`, `data/serving/mart.duckdb`, `data/serving/exports/` | 056 운영 승인 후 전체 새로고침으로 결제 Fact의 구매일 키를 반영했다. 이후 정기 DAG가 새 Serving 버전 파일을 만들고 Metabase 연결을 재지정했으며, 아래 운영 체크리스트에 Run·Export 식별자와 검증 결과를 기록했다. |
| `dbt/models/marts/metrics/rpt_subscription_payment_outcomes_daily.sql` | 기존 SQL Model의 Grain 설명을 실제 그룹화 기준으로 바로잡았다. 함께 설명을 고쳤던 등급 성과 유일성 테스트는 이후 dbt 정리에서 그룹화와 중복되어 제거했다. |
| `docs/adr/009-use-observed-history-for-customer-scd2.md`, `docs/adr/011-late-arrival-reprocessing-strategy.md` | 관측 기반 고객 이력과 Late Arrival 재처리 결정을 ADR로 기록했다. |

### v1.12 구독 Mart 전환

- [x] `P6-08` 계약 상태 관측의 SCD2 유효 구간 생성을 구현했다.
- [x] `P6-12` `dim_subscription` 계약 SCD2 Dimension을 구현했다.
- [x] `P6-14` `fct_subscription_payment` 결제 시도 Fact를 구현했다.
- [x] `P6-16` 결제 시점 기준 계약·고객 Version 결합을 Intermediate에서 구현했다.
- [x] `P6-17` Mart Grain·SCD2·Measure 계약 Singular Test 8개를 추가했다.
- [x] 고객 SCD2에서 구독 축과 재가입 파생 값을 제거하고 거래 실적 등급만 유지했다.
- [x] v1.12 Bronze 재기록 뒤 `dbt build --full-refresh`를 실행해 Model·Test 131개가 통과했다.
- [x] PostgreSQL·SeaweedFS 통합 테스트로 `ACTIVE` 계약과 첫 결제 시도의 계약·고객 FK가 모두 해소되는지 검증했다.

### 056 주문 결제 구매일 키 (2026-10-12)

[056 판정](../architect-review/056_fct-order-payment-dimension-keys.md)에 따라
`fct_order_payment.purchase_date_key`(결제가 속한 주문의 구매일 키)를 추가했다. 날짜 계산은
`stg_orders.purchase_at`의 UTC 일자를 사용하며 결제 사건일과 구분한다. 결제의 Grain과 금액,
`unique_key='order_id'`(주문 단위 교체), 영향 주문 필터와 기존 컬럼의 상대 순서를 유지한다.

검증은 운영 파일을 열지 않는 메모리 DuckDB에서 진행했다. 구현 전 신규 검증은 모델·FK·SQL
Test 부재로 `6 failed, 2 passed`였고, 구현 후 구매일 동작·품질 계약·Fact 계층·증분 계약·Mart
해시 테스트는 `28 passed`였다. 두 변경 Python 테스트 파일의 Ruff 검사도 통과했다. 임시
Warehouse·로그·출력 경로를 지정한 `dbt parse --no-partial-parse`가 통과했고, 생성된 Manifest에서
새 Intermediate의 Staging 2개 의존, 기존 증분 교체 설정과 구매일 NULL 금지·날짜 FK·Singular
Test 등록을 확인했다. README는 작업 전 파일과 대조해 ER 관계와 Fact 표의 두 변경만
적용했음을 확인했다. `git diff --check`도 통과했다.

`mart_hash`의 정렬 키 `(order_id, payment_sequence)`는 유지한다. 컬럼 추가로 Logical Hash
값이 달라지는 것은 계약 개정이며, 045 규칙에 따라 Phase 8·9의 과거 해시 증거는 재계산하지
않는다. `customer_key`와 Metabase 카드는 추가하지 않았다. 최초 구현 시에는 운영 반영을
수행하지 않았고, 사용자 승인 이후 진행한 운영 반영 결과는 아래에 기록한다.

### 056 운영 반영 (2026-10-12, `a20af83`)

- [x] `source_simulation_dag`를 정지하고 Source·Warehouse의 실행 중·대기 중 Run이 모두 0건임을 확인했다.
- [x] Airflow scheduler·API server·DAG processor의 런타임 파일 163개가 각각 커밋 `a20af83`와 일치함을 확인했다.
- [x] 직전 Published Run `0e47d012-7e8b-40b8-9744-fe2f9c33d9ff`의 기록 해시·행 수가 Published 파일과 일치함을 확인했다.
- [x] Publish CLI의 전체 새로고침으로 Run `3520e201-f49a-4aa6-9d67-377fbcd44e73`이 `PUBLISHED`로 완료됐다. 직전 Run 연결이 일치하고 변경 Relation은 `facts.fct_order_payment` 하나다.
- [x] 결제 Fact는 반영 전후 104,008행·금액 합계 16,140,072.44로 동일하다. 구매일 NULL·주문 Fact 날짜 불일치·날짜 FK 누락은 모두 0건이고, 구매일 일치는 104,008건이다. dbt 품질 Test 144건과 Published 외부 의존 없는 조회 Test 1건이 통과했다.
- [x] Source DAG를 재개했다. 재개 확인 시각은 08:34:30 UTC이며 다음 정기 실행은 09:00 UTC다.
- [x] 재개 후 Source `scheduled__2026-10-12T09:00:00+00:00`와 Warehouse `manual__2026-10-12T09:00:00+00:00`가 모두 성공했다. Publish Run `467bb050-ec75-4f69-a1bb-4ec1f56e962c`의 직전 Run은 전체 새로고침 Run이며, 품질 Test 144건이 통과했다.
- [x] 다음 정기 Run의 Serving Export와 Metabase 재지정은 모두 `SUCCESS`다. Export 식별자는 `0717263e-89c1-48be-b8f4-83554cc9298c`이며, Metabase API에서 새 버전 파일 경로와 Manifest의 Export·Publish 식별자 일치를 확인했다. Serving 결제 Fact 104,009행의 구매일 NULL·주문 날짜 불일치는 모두 0건이다.

재개 후 정기 Source 입력이 반영되어 주문·상품 항목·결제 Fact와 고객 Dimension이 갱신됐다.
이 정기 Run의 결제 Fact는 104,009행이다. 수동 전체 새로고침 Run에서 검증한 변경 Relation은
`facts.fct_order_payment` 하나이며, 당시 결제 행 수·금액 합계는 반영 전과 동일하다. 최종
`source_simulation_dag`는 재개 상태(`is_paused = false`)로 확인했다.

Source·Bronze 데이터 삭제와 Phase 8·9 과거 해시 증거 재계산은 수행하지 않았다. 운영 단계별
확인 자료는 `/tmp/contract-056-operation/`에 보존한다.

## Definition of Done

- [x] 모든 `P6-*` Task가 완료됐다.
- [x] [Mart Grain 계약](../reference/mart-grain.md)이 확정되고 구현이 그 계약을 따른다.
- [x] 모든 Mart의 Grain과 Unique Key가 검증된다.
- [x] Intermediate/Mart가 Raw Source Prefix를 직접 참조하지 않는다.
- [x] 이력 구간 중첩이 0이고 Current Version이 정확히 하나다.
- [x] 사건이 발생 시점에 유효한 Version을 참조한다.
- [x] Incremental과 Full Refresh의 Logical Hash가 같다.
- [x] Phase 4 Warehouse DAG의 `dbt_build` 호출 경계가 활성화된다.
- [x] AC-01, 09, 10, 11, 12가 통과한다.

- [x] 요구사항 추적표가 참조하는 ADR-009·ADR-011 문서가 존재한다.

[042 Mart Grain 계약 컬럼 불일치 판정](../architect-review/042_mart-grain-column-drift-ruling.md)에 따라 2026-09-22에 P6-26·P6-27을 위해 Phase 6을 다시 열었다. 2026-09-23에 다시 닫았다. 근거: reviewer가 문서 정합을 통과시켰고(2026-09-22), 계약 컬럼표 12개 Mart의 컬럼 집합이 Published Run `a10d06c3`의 실제 컬럼과 모두 일치한다.

## Portfolio Evidence

- Grain 설계 결정 과정과 Grain 계약 문서
- Fact Grain/Measure Contract 및 Fan-out 방지 Test
- 이력 Timeline과 시점 결합 결과
- Late Arrival 전후 Affected Date와 Fact Diff
- Incremental/Full Refresh Hash 비교

## 권장 Commit

```text
feat: build duckdb marts with dbt
```

## 다음 Phase 인계

Phase 7은 이 Phase의 Model Test를 통합 품질 Gate로 묶고, 실패한 Build가 마지막 성공 Mart를 훼손하지 않는 Publish 절차를 완성한다. 이 Phase가 `P6-25`로 Phase 4의 `P4-11`을 채우므로, Phase 7 시작 시점에는 모든 `P4-*` Task가 완료된 상태여야 한다.
