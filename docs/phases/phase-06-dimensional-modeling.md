# Phase 6. Dimensional Modeling

> 상태: In Progress
> Milestone: 2 — Data Platform Core  
> 선행 Phase: [Phase 5. Bronze Catalog + Staging](phase-05-bronze-catalog-and-staging.md)  
> 기준 문서: [ROADMAP](ROADMAP.md), [PRD v1.12](../../PRD_v1.12.md), [Mart Grain 계약](../reference/mart-grain.md)
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

Fact는 Intermediate가 준비한 행을 투영한다. Fact 안에서 집계하거나 시점 결합을 수행하면 Grain·Key·Measure 제공 책임과 계산 책임이 섞인다.

## Phase 6E. Report Model

- [x] `P6-19` BI가 소비할 Report Model 구현
- [x] `P6-20` Report Model Grain/Unique Key 검증 Test

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
| ADR  | ADR-009 관측 기반 고객 이력               |
| ADR  | ADR-011 Late Arrival 재처리 전략          |
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

## 파일·폴더별 변경 요약

| 경로                                                                                                    | 변경 내용                                                                                                                                  |
| ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `dbt/models/intermediate/int_customer_history.sql`                                                      | 고객 SCD2 입력을 거래 실적 등급 축만으로 축소했다.                                                                                         |
| `dbt/models/intermediate/int_subscription_history.sql`                                                  | 계약별 상태 관측을 SCD2 유효 구간으로 변환했다.                                                                                            |
| `dbt/models/marts/dimensions/dim_customer.sql`                                                          | 구독 상태·재가입 파생 컬럼을 제거하고 등급 Version만 투영하도록 축소했다.                                                                  |
| `dbt/models/marts/dimensions/dim_subscription.sql`, `dim_date.sql`                                      | `subscription_id` Business Key와 결정적 Version Key를 가진 계약 SCD2 Dimension을 추가하고, 계약 결제일까지 날짜 Dimension 범위를 확장했다. |
| `dbt/models/intermediate/int_subscription_payments_enriched.sql`                                        | 결제 시점의 계약·고객 Version과 날짜 Key를 결합해 Fact 입력을 준비하도록 수정했다.                                                         |
| `dbt/models/marts/facts/fact_subscription_payments.sql`                                                 | `payment_id` Grain의 계약별 구독 결제 시도 Fact를 구현했다.                                                                                |
| `dbt/models/marts/*/schema.yml`, `dbt/tests/dim_subscription_*`, `dbt/tests/fct_subscription_payment_*` | 계약 SCD2, 열린 계약, 청구 회차·시도, 성공 금액, 실패 코드 계약을 Schema 및 Singular Test로 강제했다.                                      |
| `tests/integration/test_subscription_payment_temporal_join_integration.py`                              | 계약 키·청구 회차·시도 순번 기준으로 Source→Bronze→dbt Fact의 계약·고객 Temporal Join을 E2E 검증하도록 갱신했다.                           |

### v1.12 구독 Mart 전환

- [x] `P6-08` 계약 상태 관측의 SCD2 유효 구간 생성을 구현했다.
- [x] `P6-12` `dim_subscription` 계약 SCD2 Dimension을 구현했다.
- [x] `P6-14` `fct_subscription_payment` 결제 시도 Fact를 구현했다.
- [x] `P6-16` 결제 시점 기준 계약·고객 Version 결합을 Intermediate에서 구현했다.
- [x] `P6-17` Mart Grain·SCD2·Measure 계약 Singular Test 8개를 추가했다.
- [x] 고객 SCD2에서 구독 축과 재가입 파생 값을 제거하고 거래 실적 등급만 유지했다.
- [x] v1.12 Bronze 재기록 뒤 `dbt build --full-refresh`를 실행해 Model·Test 131개가 통과했다.
- [x] PostgreSQL·SeaweedFS 통합 테스트로 `ACTIVE` 계약과 첫 결제 시도의 계약·고객 FK가 모두 해소되는지 검증했다.

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
