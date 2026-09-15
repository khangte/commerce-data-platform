# Phase 6. Dimensional Modeling

> 상태: Planned  
> Milestone: 2 — Data Platform Core  
> 선행 Phase: [Phase 5. Bronze Catalog + Staging](phase-05-bronze-catalog-and-staging.md)  
> 기준 문서: [ROADMAP](ROADMAP.md), [PRD v1.9](../../PRD_v1.9.md), [Mart Grain 계약](../reference/mart-grain.md)
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

| 항목                  | 없으면 생기는 문제                                             |
| --------------------- | -------------------------------------------------------------- |
| Model 목록            | 무엇을 만들지 모른다                                           |
| Model별 Grain 문장    | 새 컬럼이 그 Model에 속하는지 판정할 수 없다                   |
| Unique Key            | Grain 문장을 데이터로 강제할 수단이 없다                       |
| Measure 계약          | 같은 이름의 지표가 Model마다 다른 값을 갖는다                  |
| 이력 추적 방식        | 과거 사실을 현재 값으로 잘못 해석한다                          |
| Fan-out 방지 규칙     | Grain이 다른 입력을 결합해 Measure가 조용히 부풀려진다         |

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

- [ ] `P6-01` Dimension 후보와 각 Dimension의 Grain 문장 결정
- [ ] `P6-02` Fact 후보와 각 Fact의 Grain 문장·Unique Key 결정
- [ ] `P6-03` Measure 목록과 계산식, Additive 여부 결정
- [ ] `P6-04` 이력 추적 대상 속성과 Version 생성 규칙 결정
- [ ] `P6-05` 결정 사항을 [Mart Grain 계약](../reference/mart-grain.md)에 기록

Model을 만들기 전에 Grain 문장을 먼저 쓴다. 컬럼 목록만으로는 중복의 의미를 판정할 수 없다.

## Phase 6B. Intermediate

- [ ] `P6-06` Staging Join으로 Mart 입력 준비
- [ ] `P6-07` Grain이 다른 입력의 사전 집계
- [ ] `P6-08` 이력 구간 생성
- [ ] `P6-09` Mart가 투영할 행 준비 (파생 계산 포함)
- [ ] `P6-10` Late Arrival 영향 범위 계산

집계·파생·이력 구간 생성은 Intermediate에서 끝낸다. Late Arrival 영향 범위는 주문 구매일, 연결 주문 구매일, 고객 변경 구간, Product/Seller 사용 주문일을 기준으로 계산한다.

## Phase 6C. Dimension

- [ ] `P6-11` 단순 Dimension 구현
- [ ] `P6-12` 이력 추적 Dimension 구현
- [ ] `P6-13` Dimension Grain/Unique Key 검증 Test

이력 추적 Dimension은 Surrogate Key를 사용한다. 자연키는 Version마다 여러 행으로 존재해 Table 안에서 유일하지 않다. Surrogate Key는 재계산해도 같은 값이 나오는 결정적 방식으로 만든다.

최초 Version의 유효 시작 시각을 잘못 잡으면 그 이전에 발생한 사건이 유효한 Version을 찾지 못해 Unknown Key가 발생한다. AC-12를 위반한다.

## Phase 6D. Fact

- [ ] `P6-14` Fact 구현
- [ ] `P6-15` 모든 Fact의 `unique_key`와 Incremental 교체 구현
- [ ] `P6-16` 사건 시점 기준 Dimension Version 결합
- [ ] `P6-17` Fact Grain/Measure/Fan-out 방지 Test
- [ ] `P6-18` 정상 E2E Unknown Key 0 검증

Fact는 Intermediate가 준비한 행을 투영한다. Fact 안에서 집계하거나 시점 결합을 수행하면 Grain·Key·Measure 제공 책임과 계산 책임이 섞인다.

## Phase 6E. Report Model

- [ ] `P6-19` BI가 소비할 Report Model 구현
- [ ] `P6-20` Report Model Grain/Unique Key 검증 Test

Report Model은 Mart 위에서 파생된다. BI가 Source·Bronze·Staging을 직접 조회하지 않도록 Mart만 읽는 소비 계층을 제공한다. 날짜 축은 필요한 가장 낮은 Grain으로 유지한다. 접힌 Grain은 되돌릴 수 없지만 펼쳐진 Grain은 BI가 roll-up할 수 있다.

## Phase 6F. Incremental과 Late Arrival

- [ ] `P6-21` 변경 Key 기반 Transactional `DELETE + INSERT` 또는 검증된 `MERGE`
- [ ] `P6-22` 영향 Key/Business Date 재계산
- [ ] `P6-23` Incremental과 Full Refresh Logical Hash 비교
- [ ] `P6-24` Bronze Replay와 Re-extract 입력 경계 제공
- [ ] `P6-25` Phase 4 Warehouse DAG의 `P4-11` dbt Build 호출 경계 활성화

## 기존 구현 현황

Phase 5와 Phase 6을 분리하기 전에 만든 Intermediate/Mart Model이 이미 존재하고, `warehouse.duckdb`에 빌드되어 데이터까지 채워져 있다. 그러나 이 Model들은 Grain 계약을 먼저 확정하지 않고 만들었다.

**이 Phase의 상태가 `Planned`인 이유가 여기 있다.** 아래 Model은 참고 자료이며 이 Phase의 완료 근거가 아니다. Grain을 직접 설계한 뒤 그 계약에 맞지 않는 Model은 폐기한다. 기존 구현이 존재한다는 사실이 Task 완료를 뜻하지 않는다.

| 계층         | 기존 Model                                                                                                                                             | 실측 행 수 |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------- |
| Intermediate | `int_orders_enriched`, `int_order_items_enriched`, `int_order_item_totals`, `int_payment_summary`, `int_order_fact_ready`, `int_subscription_payments_enriched`, `int_customer_history`, `int_affected_business_dates` | View 8개   |
| Dimension    | `dim_customer` 96,097 / `dim_product` 32,951 / `dim_seller` 3,095 / `dim_date` 774                                                                     | Table 4개  |
| Fact         | `fact_orders` 99,441 / `fact_order_items` 112,650 / `fact_payments` 103,886 / `fact_subscription_payments` 1                                            | Table 4개  |
| Report       | `rpt_subscription_funnel_daily`, `rpt_subscription_payment_outcomes_daily`, `rpt_membership_tier_performance`                                          | View 3개   |

관련 dbt Test는 `dbt/tests/`의 `dim_customer_*`, `fact_*`, `rpt_*`, `int_customer_history_no_conflicting_hash`다. 이 Test들도 확정된 Grain 계약 기준으로 다시 검토한다.

### 기존 구현을 다루는 방식

- `P6-01`~`P6-05`로 Grain 계약을 먼저 확정한다. 이때 기존 Model의 Grain을 근거로 삼지 않는다. 지금 그렇게 되어 있다는 사실은 그렇게 해야 한다는 근거가 아니다.
- 계약 확정 후 Model을 하나씩 대조한다. 계약과 일치하면 유지하고, 어긋나면 수정하거나 폐기한다.
- 폐기 결정한 Model은 `dbt/models/`에서 제거하고 관련 Test와 `warehouse.duckdb` Table도 함께 정리한다.
- 계약에 있으나 구현이 없는 Model은 새로 만든다.

기존 구현이 `warehouse.duckdb`를 채우고 있으므로 Phase 7 이후 단계는 당장 깨지지 않는다. 다만 이 Phase의 Definition of Done은 직접 확정한 Grain 계약만을 기준으로 판단한다.

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

| 경로 | 변경 내용 |
| ---- | --------- |
|      |           |

## Definition of Done

- [ ] 모든 `P6-*` Task가 완료됐다.
- [ ] [Mart Grain 계약](../reference/mart-grain.md)이 확정되고 구현이 그 계약을 따른다.
- [ ] 모든 Mart의 Grain과 Unique Key가 검증된다.
- [ ] Intermediate/Mart가 Raw Source Prefix를 직접 참조하지 않는다.
- [ ] 이력 구간 중첩이 0이고 Current Version이 정확히 하나다.
- [ ] 사건이 발생 시점에 유효한 Version을 참조한다.
- [ ] Incremental과 Full Refresh의 Logical Hash가 같다.
- [ ] Phase 4 Warehouse DAG의 `dbt_build` 호출 경계가 활성화된다.
- [ ] AC-01, 09, 10, 11, 12가 통과한다.

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
