# Phase 10. BI

> 상태: Planned  
> Milestone: 3 — Portfolio Evidence  
> 선행 Phase: [Phase 9. Benchmark](phase-09-benchmark.md)  
> 기준 문서: [ROADMAP](ROADMAP.md), [PRD v1.11](../../PRD_v1.11.md)

## 목표

Metabase에서 검증 완료된 Mart만 사용해 Sales, Product, Customer Dashboard를 재현하고, Data Mart가 실제 소비 가능한 Grain과 Metric을 제공하는지 확인한다. BI 자체는 프로젝트의 중심 기능으로 확장하지 않는다.

## 핵심 계약

- Dashboard는 Source, Bronze, Staging을 직접 조회하지 않는다.
- Phase 6에서 정의한 Fact Grain과 Measure 의미를 변경하지 않는다.
- Metric 정의는 [Mart Grain 계약](../reference/mart-grain.md)의 Measure 계약을 따른다. 주문 금액과 실제 결제 금액을 같은 것으로 취급하지 않는다.
- Metabase/DuckDB 연결이 불안정하면 PostgreSQL Serving DB 대안을 검증하고 ADR로 결정한다.
- Dashboard 재현에 필요한 Query, Filter, Metric 정의를 문서화한다.

## 구독·등급 전환 Step 7 사전 지표

Metabase Dashboard 구현 전에도 BI가 Source·Bronze가 아닌 Mart만 읽도록, 아래 `metrics`
Schema Table을 제공한다. 이는 Phase 10의 연결·Dashboard·스크린샷 완료를 뜻하지 않는다.

| View | 용도 |
| ---- | ---- |
| `rpt_subscription_funnel_daily` | 체험 시작, 활성화, 결제 실패, 해지 신청, 이탈, 재가입 퍼널 |
| `rpt_subscription_payment_outcomes_daily` | 구독 결제 성공·실패 건수와 결제 금액 |
| `rpt_membership_tier_performance` | 등급별 주문 수, GMV, Delivered AOV |

세 View의 Grain·Unique Key와 알려진 제약은 [Mart Grain 계약](../reference/mart-grain.md) 4절이
정본이다. 세 View는 모두 사건 발생 시점의 고객 속성을 사용한다. 현재 고객 분포는 최신
Version만 사용해야 하며, 두 관점을 같은 지표로 합치지 않는다.

- [x] 전환 계획 7단계의 구독 퍼널·결제 실패·해지·재가입·등급별 지표 View를 구현했다.
- [ ] Metabase Connection과 Sales/Product/Customer Dashboard는 Phase 10 구현 순서에서 진행한다.

## 선행 조건

이 Phase는 Grain 계약을 필요로 하는 Task와 그렇지 않은 Task로 나뉜다. 골격 Task는 Phase 6 완료 전에 착수할 수 있다.

### 골격 Task 선행 조건 (10-1)

- Phase 7의 골격 Task(Publish Safety)가 완료되어 Published Mart 경계가 존재한다.
- Metabase 0.63.16.1 Image와 Credential Template이 준비됐다.

### 적용 Task 선행 조건 (10-2)

- Phase 6, 7, 8, 9의 모든 Task가 완료된다.
- [Mart Grain 계약](../reference/mart-grain.md)이 확정되고 Published Mart 품질 Gate가 통과한다.
- Phase 9에서 사용할 Dataset과 Mart Result Hash가 고정됐다.

## 10-1. 골격: Grain 계약 없이 진행 가능

Metabase 연결과 권한 검증은 조회 대상 Model과 독립적이다. 어떤 Mart가 있는지 몰라도 Driver, Lock, Read-only 권한, 재기동 지속성은 검증할 수 있다.

### 10-1-1. Connection Gate

- [x] `P10-01` Metabase Compose Service와 Health Check 구성
- [x] `P10-02` Metabase → DuckDB Driver 설치/Version/Lock 검증
- [x] `P10-03` Read-only 권한과 Published Mart만 노출되는지 검증
- [x] `P10-04` 재기동 후 Connection/Dashboard 지속성 확인
- [x] `P10-05` 연결 방식과 제한을 ADR-012에 기록

기본 경로:

```text
Metabase
    ↓
읽기 전용 Serving DuckDB (`data/serving/mart.duckdb`)
    ↑
Published Mart → Serving Export
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

구현 증적은 `src/serving/export.py`의 Mart 전용 원자 Export, `compose.yaml`의 `bi` Profile과 `metabase/README.md`의 Driver 설치 절차다. 2026-09-23에 Run `5d74a326-954b-4850-a3d6-56d1556fec8d`의 `PUBLISHED` Mart 9개를 검증하고 Serving Export를 실행해 `data/serving/mart.duckdb`를 생성했다. Metabase는 health check를 통과했고 DuckDB 드라이버도 로드됐으며, 컨테이너 재기동 뒤에도 health check와 드라이버 등록을 재확인했다. `METABASE_API_KEY`로 등록한 `Commerce Mart Serving` Connection은 Mart 12개와 `dimensions`·`facts`·`metrics` Schema만 조회했고 `access_mode=read_only`·읽기 전용 `/serving` Mount를 확인했다. 재기동 뒤에도 Connection과 `metrics.rpt_membership_tier_performance` 조회가 유지돼 `P10-01`~`P10-05`를 완료했다.

## 10-2. 적용: Phase 6 완료 후 진행

조회 대상 Model과 Metric 정의가 확정돼야 진행할 수 있는 Task다.

### 10-2-1. Semantic 정의

- [x] `P10-06` Order/Customer/Product/Date Model 관계 설정
- [x] `P10-07` GMV, Orders, AOV 정의 등록
- [x] `P10-08` Category/Product/Customer Metric 정의 등록
- [x] `P10-09` UTC Date와 Filter 기본값 검증

핵심 Metric의 계산식은 [Mart Grain 계약](../reference/mart-grain.md)의 Measure 계약에서
가져온다. BI는 Measure를 재정의하지 않고 Mart가 제공하는 값을 그대로 집계한다.

### 10-2-2. Sales Dashboard

- [ ] `P10-10` Daily GMV
- [ ] `P10-11` Daily Orders
- [ ] `P10-12` AOV
- [ ] Date/Order Status Filter와 합계 검증

### 10-2-3. Product Dashboard

- [ ] `P10-13` Category GMV
- [ ] `P10-14` Top Products
- [ ] `P10-15` Sales Volume
- [ ] Item Grain과 Order Grain 혼합으로 인한 Fan-out이 없는지 검증

### 10-2-4. Customer Dashboard

- [ ] `P10-16` New Customers
- [ ] `P10-17` Repeat Customers
- [ ] `P10-18` 구독 상태·거래 실적 등급 분포/추이
- [ ] `P10-19` Region 분석
- [ ] 현재 속성과 주문 시점 속성의 사용 목적을 명시

### 10-2-5. 재현성과 검증

- [ ] `P10-20` Dashboard Export 또는 재생성 가능한 설정 보존
- [ ] `P10-21` Dashboard별 Source Model/Query/Filter 문서화
- [ ] `P10-22` dbt 기준 Query와 Dashboard Total 대조
- [ ] `P10-23` Screenshot과 Dataset/Run/Commit 식별자 기록

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
| Customer  | New/Repeat, Membership, Region     | 고객 정의와 기준 시점 명시  |

## 요구사항 추적

| 구분 | 연결 항목                         | 증거                      |
| ---- | --------------------------------- | ------------------------- |
| PRD  | Section 19 BI                     | Connection과 Dashboard    |
| PRD  | ADR-012 Metabase Serving Strategy | 선택 근거와 검증 결과     |
| FR   | FR-18 Metabase                    | 3개 Dashboard와 재현 문서 |

Phase 10에는 별도 AC 번호가 없으므로 ROADMAP의 Connection/Dashboard Gate를 Release Gate로 사용한다.

## 산출물

- Metabase Service와 Connection 설정
- Connection Strategy ADR
- Sales/Product/Customer Dashboard
- Metric Dictionary와 Model 관계 문서
- Dashboard 재현/Export 자료
- dbt 기준 Query와 Dashboard Total 비교 결과

## 파일·폴더별 변경 요약

| 경로 | 변경 내용 |
| ---- | --------- |
| `dbt/dbt_project.yml`, `tests/test_published_mart_queryable.py` | `marts.metrics`를 table로 실체화하고, 외부 접근 없이 Published Mart 전 객체가 조회되는 회귀 Test를 추가했다. |
| `src/serving/`, `tests/serving/` | Mart Schema만 복사하고 Publish Hash·행 수를 Manifest에 보존하는 원자 Serving Export와 단위 Test를 추가했다. |
| `airflow/dags/warehouse_pipeline_dag.py`, `tests/test_airflow_dags.py` | Publish 성공 뒤 Serving Export를 실행하고 Export 실패를 Publish와 분리해 Summary에 남기도록 연결했다. |
| `compose.yaml`, `.env.example`, `sql/bootstrap/01-create-databases-and-roles.sh`, `metabase/`, `data/serving/` | `bi` Profile Metabase, 별도 애플리케이션 DB·역할, 읽기 전용 Serving Mount와 Driver 설치 문서를 추가했다. Connection Gate 준비 과정에서 플러그인 디렉터리를 Metabase UID/GID `2000`이 쓰도록 고쳐 DuckDB 드라이버가 로드됨을 확인했고, release `1.5.5.0` jar의 플러그인 표기 버전은 `1.4.1.0`임을 문서화했다. DuckDB JDBC native 라이브러리가 Alpine musl에서 동작하지 않아, `metabase/Dockerfile`은 공식 Metabase 앱을 glibc 기반 Temurin Java 이미지에서 실행하고 health check용 `wget`을 포함한다. WSL 멈춤 완화를 위해 Metabase `JAVA_OPTS=-Xmx1g`, `mem_limit: 1536m`, `restart: "no"`를 적용했다. `.env.example`에는 Connection Gate 자동 검증용 `METABASE_API_KEY`의 로컬 전용 안내를 추가했다. 2026-09-23 Run `5d74a326-954b-4850-a3d6-56d1556fec8d`에서 생성한 Serving Export는 Mart 12개를 `data/serving/mart.duckdb`에 담았고, `Commerce Mart Serving` Connection이 세 Mart Schema를 read-only로 조회한 뒤 컨테이너 재기동 후에도 유지됨을 확인했다. |
| `dbt/profiles.yml`, `src/warehouse/publish.py`, `dbt/models/intermediate/int_order_items_enriched.sql`, `dbt/models/marts/facts/`, `dbt/tests/`, `tests/test_warehouse_*` | Mart Build 세션을 UTC로 고정해 날짜 키·대체 키의 실행 환경 의존성을 없앴고, Publish CLI에 전체 새로고침 인자를 추가했다. 주문 항목 Fact에는 주문 원본에서 유도한 `purchase_date_key`와 날짜 차원 FK·주문 Fact 일치 Test를 추가했다. Run `a10d06c3-c32e-4407-afdc-a1167b820c44`는 UTC 기준 Run `5d74a326-954b-4850-a3d6-56d1556fec8d`와 비교해 `facts.fct_order_item`만 Hash가 변경됨을 검증했다. |
| `docs/reference/metric-dictionary.md`, `metabase/queries/semantic_*.sql` | GMV·Orders·AOV와 카테고리·상품·고객 지표의 원본 Grain·가산성·계약 근거를 고정하고, Metabase 등록 지표와 Warehouse 대조 SQL을 추가했다. |
| `docs/adr/012-metabase-serving-strategy.md` | Serving DuckDB 기본 경로와 Connection Gate 대기 상태를 ADR 초안에 기록했다. |

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

Phase 10 완료 후 README의 Architecture, 실행 절차, Acceptance Test, Benchmark, Runbook, Dashboard Evidence 링크를 최종 점검한다. V2 후보인 CDC, Delete/Tombstone, Cloud PoC는 V1 완료 조건에 포함하지 않는다.
