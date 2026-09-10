# Phase 9. BI

> 상태: Planned  
> Milestone: 3 — Portfolio Evidence  
> 선행 Phase: [Phase 8. Benchmark](phase-08-benchmark.md)  
> 기준 문서: [ROADMAP](ROADMAP.md), [PRD v1.7](../../PRD_v1.7.md)

## 목표

Metabase에서 검증 완료된 Mart만 사용해 Sales, Product, Customer Dashboard를 재현하고, Data Mart가 실제 소비 가능한 Grain과 Metric을 제공하는지 확인한다. BI 자체는 프로젝트의 중심 기능으로 확장하지 않는다.

## 핵심 계약

- Dashboard는 Source, Bronze, Staging을 직접 조회하지 않는다.
- Phase 5에서 정의한 Fact Grain과 Measure 의미를 변경하지 않는다.
- 기본 GMV는 `DELIVERED` 주문의 `gross_order_value`를 사용한다.
- `payment_total`을 Revenue와 동일시하지 않는다.
- Metabase/DuckDB 연결이 불안정하면 PostgreSQL Serving DB 대안을 검증하고 ADR로 결정한다.
- Dashboard 재현에 필요한 Query, Filter, Metric 정의를 문서화한다.

## 선행 조건

- Phase 6의 Published Mart 품질 Gate가 통과한다.
- Phase 8에서 사용할 Dataset과 Mart Result Hash가 고정됐다.
- Metabase 0.63.16.1 Image와 Credential Template이 준비됐다.

## 구현 순서

### 1. Connection Gate

- [ ] `P9-01` Metabase Compose Service와 Health Check 구성
- [ ] `P9-02` Metabase → DuckDB Driver 설치/Version/Lock 검증
- [ ] `P9-03` Read-only 권한과 Published Mart만 노출되는지 검증
- [ ] `P9-04` 재기동 후 Connection/Dashboard 지속성 확인
- [ ] `P9-05` 연결 방식과 제한을 ADR-012에 기록

기본 경로:

```text
Metabase
    ↓
DuckDB Published Mart
```

Driver 또는 File Lock 문제가 V1 운영 조건에서 해결되지 않으면 다음 대안을 사용한다.

```text
DuckDB Published Mart
    ↓ controlled sync
PostgreSQL Serving DB
    ↓ read-only
Metabase
```

대안을 선택할 경우 동기화 시점, 원자성, Serving Schema, 추가 운영 비용을 ADR에 기록한다.

### 2. Semantic 정의

- [ ] `P9-06` Order/Customer/Product/Date Model 관계 설정
- [ ] `P9-07` GMV, Orders, AOV 정의 등록
- [ ] `P9-08` Category/Product/Customer Metric 정의 등록
- [ ] `P9-09` UTC Date와 Filter 기본값 검증

핵심 Metric:

```text
GMV    = SUM(gross_order_value), order_status = 'DELIVERED'
Orders = SUM(order_count)
AOV    = GMV / Delivered Orders
```

### 3. Sales Dashboard

- [ ] `P9-10` Daily GMV
- [ ] `P9-11` Daily Orders
- [ ] `P9-12` AOV
- [ ] Date/Order Status Filter와 합계 검증

### 4. Product Dashboard

- [ ] `P9-13` Category GMV
- [ ] `P9-14` Top Products
- [ ] `P9-15` Sales Volume
- [ ] Item Grain과 Order Grain 혼합으로 인한 Fan-out이 없는지 검증

### 5. Customer Dashboard

- [ ] `P9-16` New Customers
- [ ] `P9-17` Repeat Customers
- [ ] `P9-18` Membership 분포/추이
- [ ] `P9-19` Region 분석
- [ ] 현재 속성과 주문 시점 SCD2 속성의 사용 목적을 명시

### 6. 재현성과 검증

- [ ] `P9-20` Dashboard Export 또는 재생성 가능한 설정 보존
- [ ] `P9-21` Dashboard별 Source Model/Query/Filter 문서화
- [ ] `P9-22` dbt 기준 Query와 Dashboard Total 대조
- [ ] `P9-23` Screenshot과 Dataset/Run/Commit 식별자 기록

## 범위 밖

- Raw Source 탐색용 Dashboard
- 새로운 Metric을 위해 Mart Grain을 임의 변경하는 작업
- BI Embedded Application 개발
- 실시간 Refresh/SLA
- 세밀한 UI Branding

## 검증과 Gate

### Connection

- Metabase가 재기동 후 Published Mart를 읽을 수 있다.
- BI 계정은 Source/Bronze/Staging에 접근할 수 없다.
- DuckDB Driver/Lock 실패 시 대안 경로가 재현 가능하다.

### Dashboard

| Dashboard | 최소 항목                          | 검증 기준                   |
| --------- | ---------------------------------- | --------------------------- |
| Sales     | Daily GMV, Orders, AOV             | dbt 기준 Query와 Total 일치 |
| Product   | Category GMV, Top Products, Volume | Grain Fan-out 0             |
| Customer  | New/Repeat, Membership, Region     | 고객 정의와 SCD2 시점 명시  |

## 요구사항 추적

| 구분 | 연결 항목                         | 증거                      |
| ---- | --------------------------------- | ------------------------- |
| PRD  | Section 19 BI                     | Connection과 Dashboard    |
| PRD  | ADR-012 Metabase Serving Strategy | 선택 근거와 검증 결과     |
| FR   | FR-18 Metabase                    | 3개 Dashboard와 재현 문서 |

Phase 9에는 별도 AC 번호가 없으므로 ROADMAP의 Connection/Dashboard Gate를 Release Gate로 사용한다.

## 산출물

- Metabase Service와 Connection 설정
- Connection Strategy ADR
- Sales/Product/Customer Dashboard
- Metric Dictionary와 Model 관계 문서
- Dashboard 재현/Export 자료
- dbt 기준 Query와 Dashboard Total 비교 결과

## Definition of Done

- [ ] 모든 `P9-*` Task가 완료됐다.
- [ ] Metabase가 Mart만 조회한다.
- [ ] 연결 전략과 Driver/Lock 관측 결과가 ADR에 기록됐다.
- [ ] Sales/Product/Customer Dashboard가 모두 재현된다.
- [ ] Dashboard 합계가 dbt 기준 Query와 일치한다.
- [ ] Grain Fan-out과 Metric 의미 혼동이 없다.
- [ ] Screenshot에 Dataset/Run/Commit 식별 정보가 연결된다.

## Portfolio Evidence

- Metabase Connection과 Serving Architecture
- 세 Dashboard Screenshot
- Metric Dictionary
- dbt 기준 Query와 Dashboard Total 대조표
- DuckDB 직접 연결 또는 Serving DB 선택 ADR

## 권장 Commit

```text
feat: publish marts through metabase dashboards
```

## 프로젝트 완료 인계

Phase 9 완료 후 README의 Architecture, 실행 절차, Acceptance Test, Benchmark, Runbook, Dashboard Evidence 링크를 최종 점검한다. V2 후보인 CDC, Delete/Tombstone, Cloud PoC는 V1 완료 조건에 포함하지 않는다.
