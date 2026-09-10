# Phase 2. Deterministic Generator

> 상태: Done
>
> Milestone: 1 — Source Foundation
>
> 선행 Phase: [Phase 1. Source Environment](phase-01-source-environment.md)
>
> 기준 문서: [ROADMAP](ROADMAP.md), [PRD v1.8](../../PRD_v1.8.md)

## 목표

정적 Olist Source를 신규 주문, 상태 변경, 고객 속성 변경, Late Arrival이 반복해서 발생하는 서비스형 OLTP Source로 만든다. 동일 Snapshot과 동일 입력은 동일한 변경 결과를 생성해야 한다.

## 핵심 계약

- Business ID는 UUIDv5 또는 결정적 Hash로 생성한다.
- `customer_unique_id`는 동일 인물의 Business Key, `customer_id`는 주문 시점 Customer Record다.
- 기존 가변 고객 구독·등급 행의 새 `updated_at`은 직전 값보다 반드시 크다. 계정 행은 생성 후 불변이다.
- Late Arrival은 과거 Business Event Time과 현재 원천 변경 시각으로 표현한다.
- Order/Item/Payment 묶음은 하나의 Transaction으로 생성한다.
- Warehouse가 원천 데이터 동시성 잠금을 보유하는 동안 Generator는 Source를 변경하지 않는다.
- 한 수집 구간에서 동일 SCD2 추적 속성을 여러 번 바꿔 중간 Version을 소실시키지 않는다.

## 선행 조건

- AC-14와 AC-18을 통과한 기준 Source Snapshot이 있다.
- `pipeline_metadata` Database와 `generator_runs`를 기록할 권한이 있다.
- Source PK/FK/CHECK가 활성화되어 있다.

## 구현 순서

### 1. 결정성 기반

- [x] `P2-01` Generator Config Schema 정의
- [x] `P2-02` `random_seed` 입력 및 기록
- [x] `P2-03` UTC `logical_date` 입력 및 검증
- [x] `P2-04` `generator_version` 입력 및 호환성 검사
- [x] `P2-05` UUIDv5/Hash 기반 Deterministic ID Utility
- [x] `P2-06` `generator_runs` DDL과 상태, 입력, Count, Logical Hash 기록

결정성 입력은 다음을 포함한다.

```text
source_snapshot_id
random_seed
logical_date
order_count
anomaly_profile
generator_version
```

### 2. Customer 시나리오

- [x] `P2-07` 신규 `customer_unique_id` 생성
- [x] `P2-08` 주문 단위 신규 `customer_id` 생성
- [x] `P2-09` 기존 고객 재구매 시나리오
- [x] `P2-10` 주소 변경과 새 Customer Record 생성
- [x] `P2-11` 결정적 Membership 계산 및 변경

### 3. Order/Item/Payment 생성

- [x] `P2-12` 신규 Order 생성
- [x] `P2-13` Order Item과 Composite Key 생성
- [x] `P2-14` Payment와 `payment_sequential` 생성
- [x] 세 Entity 생성 실패 시 전체 Transaction Rollback

### 4. 상태 변화

허용 전이만 생성한다.

```text
Order:   created → approved → shipped → delivered
         created/approved → canceled

Payment: pending → completed → refunded
         pending → failed
```

- [x] 상태 전이 전 현재 상태와 기대 Version 확인
- [x] 기존 Row 변경 시 `updated_at` 단조 증가 보장
- [x] 동일 `updated_at`에서 서로 다른 값으로 변경 금지
- [x] Source에는 Raw-compatible 상태를 저장하고 표준화는 수행하지 않음

### 5. Service-level 시나리오

- [x] `P2-15` Late Order 생성
- [x] `P2-16` Delayed Payment 생성
- [x] `P2-17` 과거 Event Timestamp를 가진 Late Update 생성
- [x] `P2-18` Membership Change 생성

파이프라인 검증용 Duplicate/NULL Key/Broken FK 같은 오류는 정상 OLTP Source를 오염시키지 않고 Phase 3/6의 주입 경로에서 처리한다.

### 6. 원천 변경 동시성

- [x] `P2-19` `source_mutation_leases` DDL과 원천 데이터 동시성 잠금 조회/획득 로직
- [x] `P2-20` Lease 획득 실패 시 Source 변경 전 안전하게 종료
- [x] `P2-21` Generator Transaction 종료 후 Lease 해제
- [x] `P2-22` 만료/소유권 상실 시 변경을 중단하는 Fencing 검증

### 실행 통합

- [x] 현재 성공 Seed Snapshot을 자동 식별하는 Generator 실행 서비스
- [x] Lease 보호 아래 결정적 Order Bundle 생성과 `generator_runs` 결과 기록
- [x] 동일 성공 입력의 결과 재사용과 Warehouse의 원천 데이터 동시성 잠금 중 Source 변경 0 검증

CLI가 직접 실행하는 Profile은 `default`, `late-arrival`, `membership-change`,
`subscription-trial`, `subscription-active`, `subscription-payment-failed`,
`subscription-cancel-requested`, `subscription-churned`, `subscription-rejoined`다.
`membership-change`는 `BRONZE` 또는 `SILVER` 사람 한 명의 `customer_loyalty_tiers` 행만
갱신한다. 구독 Profile은 상태별로 허용된 현재 상태의 사람 한 명을 결정적으로 골라
`customer_subscriptions` 행을 갱신한다. `subscription-active`의 정기 결제와 `subscription-trial`
종료 시점의 결제는 `subscription_payments`에 행을 추가한다. `PAYMENT_FAILED`는 7일 유예
종료 시각을 만들고, `subscription-churned`는 해당 시각 이후에만 실행할 수 있다.
`CHURNED → ACTIVE`는 재가입 전이로 보존한다. 시각 기반 만료 스캔은 매 실행마다 돌며
`benefit_ends_at` 경과 행을 `CHURNED`로 전이한다. `delayed-payment`는 Phase 3 검증에서도
조합할 수 있는 재사용 가능한 Source Scenario Fixture로 제공한다.

## 범위 밖

- Bronze/Quarantine Object 생성
- 파이프라인 오류 주입 결과를 Source DB에 저장
- Airflow DAG Scheduling
- dbt 상태 표준화와 SCD2 모델 구현

## 테스트

### 결정성

기준 Snapshot을 복제해 동일 입력으로 각각 실행하고 다음을 비교한다.

```text
생성/변경 Key Set
Entity별 Row Count
상태와 비즈니스 값
Logical Hash
```

### 계약

- 허용되지 않은 Order/Payment 상태 전이가 0인지 검증
- 모든 기존 Row 변경에서 `new.updated_at > old.updated_at`인지 검증
- Late Arrival에서 Business Event Time은 과거이고 원천 변경 시각은 현재인지 검증
- 실패한 복합 생성에서 부분 Order/Item/Payment가 남지 않는지 검증
- Warehouse의 원천 데이터 동시성 잠금 보유 중 생성 Row/변경 Row가 0인지 검증

## 요구사항 추적

| 구분 | 연결 항목                                    | 주요 증거                |
| ---- | -------------------------------------------- | ------------------------ |
| PRD  | Section 4.1 `updated_at` 안전성 계약         | 원천 변경 시각 Test      |
| PRD  | Section 7 Generator와 Anomaly                | 결정성 및 상태 전이 Test |
| PRD  | Section 7.3 원천 데이터 동시성 잠금          | Lease 충돌 Test          |
| ADR  | ADR-009 관측 기반 고객 SCD2                  | 변경 횟수/관측 계약      |
| ADR  | ADR-013 원천 변경/수집 동시성                | Lease Test               |
| FR   | FR-02 Deterministic Generator                | 동일 Snapshot 비교       |
| FR   | FR-05 원천 데이터 동시성 잠금                | 동시성 실행 기록         |
| AC   | AC-15 Generator 재현                         | Key Set/Hash 비교        |
| AC   | AC-20 원천 변경 시각 Cursor Safety의 생성 측 | Late Update Fixture      |
| AC   | AC-21 Generator vs Warehouse의 생성 측       | Source 변경 0 증거       |

AC-20과 AC-21의 전체 E2E 판정은 Phase 3의 Ingestion과 결합해 완료한다.

## 산출물

- Generator Config와 CLI
- Deterministic ID/Hash Utility
- Customer/Order/Item/Payment 생성 모듈
- 상태 변경과 Late Arrival 시나리오
- `generator_runs` Metadata
- 원천 데이터 동시성 잠금 Client
- 결정성, Transaction, 원천 변경 시각 자동 테스트

## 파일·폴더별 변경 요약

| 경로                                                          | 변경      | 요약                                                                                                                  |
| ------------------------------------------------------------- | --------- | --------------------------------------------------------------------------------------------------------------------- |
| `src/generator/config.py`                                     | 생성·수정 | 결정성 실행 Config, UTC `logical_date`, 지원 Version·Profile 검증과 CLI 실행 Profile 범위를 추가했다.                 |
| `src/generator/ids.py`                                        | 생성      | UUIDv5 Business ID와 안정적인 Logical Hash 유틸리티를 추가했다.                                                       |
| `src/generator/metadata.py`                                   | 생성      | `generator_runs` Schema 준비와 RUNNING/완료 실행 이력 기록 기능을 추가했다.                                           |
| `src/generator/customers.py`                                  | 수정      | 불변 Customer 계정, 사람 단위 구독 Record와 등급 Record를 각각 `customer_subscriptions`·`customer_loyalty_tiers`로 분리하고, 구독 상태 전이·등급의 단조 변경 저장과 시각 기반 만료 스캔을 추가했다. |
| `src/generator/subscription_payments.py`                      | 생성      | 구독 자동결제 1건을 `subscription_payments`에 결정적으로 기록하고 `next_billing_at`을 1개월 뒤로 민다.                 |
| `src/generator/orders.py`                                     | 수정      | Order·Item·Payment Bundle 저장 시 새 사람의 `customer_subscriptions` `NON_MEMBER` 행과 `customer_loyalty_tiers` `BRONZE` 행을 함께 보장하도록 변경했다.                               |
| `src/generator/transitions.py`                                | 생성      | Order·Payment 허용 상태 전이, 기대 Version, 원천 변경 시각 검증을 추가했다.                                           |
| `src/generator/scenarios.py`                                  | 생성      | Late Order·Delayed Payment·Late Update·Membership Change Scenario를 추가했다.                                         |
| `src/generator/lease.py`                                      | 생성      | Generator·Warehouse 원천 데이터 동시성 잠금의 획득·갱신·Fencing·해제를 추가했다.                                      |
| `src/generator/service.py`                                    | 수정      | Seed Snapshot 검증, Lease 보호 Source 생성, 실행 결과 재사용과 거래 실적 등급·구독 상태 변경 Profile 실행을 추가했다.          |
| `src/generator/__main__.py`                                   | 생성·수정 | Generator CLI 기반을 만들고, 기본 실행 적재·`--validate-only`·실행 가능 Profile 선택을 지원하도록 변경했다.           |
| `sql/metadata/002_create_generator_metadata.sql`              | 생성·수정 | Generator 실행 Metadata Schema를 만들고, 성공 실행 입력만 Unique하게 보관해 실패 실행의 재시도를 허용하도록 변경했다. |
| `sql/metadata/003_create_source_mutation_leases.sql`          | 생성      | `commerce_source` 원천 데이터 동시성 잠금 Table을 추가했다.                                                           |
| `src/generator/__init__.py`                                   | 수정      | Generator Config와 현재 구현 Version을 Package API로 노출했다.                                                        |
| `tests/generator/`                                            | 생성·수정 | Config, 결정적 ID/Hash, Metadata 입력과 동일 Snapshot 입력의 Bundle 재현 단위 테스트를 추가했다.                      |
| `tests/integration/test_generator_metadata_integration.py`    | 생성      | 실제 PostgreSQL에 Generator 실행 이력이 저장되는지 검증하는 통합 테스트를 추가했다.                                   |
| `tests/integration/test_generator_customer_integration.py`    | 생성      | Customer Record 저장 멱등성과 Membership 변경 시각을 검증하는 통합 테스트를 추가했다.                                 |
| `tests/integration/test_generator_order_integration.py`       | 생성      | Order Bundle의 Insert/Skip, FK 오류 Rollback 통합 테스트를 추가했다.                                                  |
| `tests/integration/test_generator_transition_integration.py`  | 생성      | 상태 전이 재실행, Business Timestamp, 오래된 Version 거부를 검증하는 통합 테스트를 추가했다.                          |
| `tests/integration/test_generator_scenario_integration.py`    | 생성      | Service-level Scenario의 Business Event와 원천 변경 시각 분리를 검증하는 통합 테스트를 추가했다.                      |
| `tests/integration/test_source_mutation_lease_integration.py` | 생성      | Generator·Warehouse 원천 데이터 동시성 잠금의 배타성, 해제, 만료 인수 Fencing을 검증하는 통합 테스트를 추가했다.      |
| `tests/integration/test_generator_service_integration.py`     | 생성      | 실제 Generator 적재, 성공 결과 재사용, Warehouse의 원천 데이터 동시성 잠금 차단을 검증하는 통합 테스트를 추가했다.    |
| `tests/generator/test_customers.py`, `tests/generator/test_scenarios.py` | 수정 | 거래 실적 등급 경계, 구독 상태 전이, 상태별 시각, 해지 후 재가입을 검증했다. |
| `docs/phases/phase-02-deterministic-generator.md`             | 수정      | P2-01~22와 구독 상태·거래 실적 등급 Generator 전환, 파일별 변경 요약을 기록했다.                                 |

### 구독·등급 Generator 재작업

구독 상태 전이와 등급 갱신 로직은 통합 테이블(A안) 기준으로 먼저 작성했다. 이후
[비교](../architecture/membership-table-split-comparison.md)를 거쳐 B안(Source만 분리)으로
확정했으므로, 저장 대상을 `customer_subscriptions`와 `customer_loyalty_tiers` 두 테이블로
나누고 CHECK 제약 위치를 옮긴다. 상태 전이 규칙, 만료 스캔 순서, Seed 기준선 로직은 그대로
쓴다. `subscription_payments` 자동결제 기록은 이 재작업에서 새로 만든다. 세부 순서는
[전환 계획](../architecture/subscription-membership-transition-plan.md) 5절에 있다.

## Definition of Done

- [x] 모든 `P2-*` Task가 완료됐다.
- [x] 동일 Snapshot/입력의 Key Set, Count, 상태, Hash가 같다.
- [x] 허용되지 않은 상태 전이가 0이다.
- [x] Mutable Row의 `updated_at` 역행 또는 동일 위치 변경이 0이다.
- [x] 복합 Entity 생성이 원자적으로 동작한다.
- [x] Warehouse의 원천 데이터 동시성 잠금 중 Generator 변경이 0이다.
- [x] AC-15가 통과하고 AC-20/21용 Fixture가 준비됐다.

## 검증 증적

2026-09-07에 아래 검증을 실행했다.

```bash
uv run ruff check src/generator tests/generator
uv run pytest -q tests/generator
RUN_POSTGRES_INTEGRATION=1 uv run pytest -q tests/integration/test_generator_metadata_integration.py tests/integration/test_generator_customer_integration.py tests/integration/test_generator_order_integration.py tests/integration/test_generator_transition_integration.py tests/integration/test_generator_scenario_integration.py tests/integration/test_generator_service_integration.py tests/integration/test_source_mutation_lease_integration.py
```

- Ruff 오류 없음
- Generator 단위 테스트 `39 passed`
- PostgreSQL 통합 테스트 `10 passed`

## Portfolio Evidence

- 동일 입력을 두 번 실행한 Key Set/Hash 비교
- Business Event Time과 원천 변경 시각이 다른 Late Arrival 예시
- Customer Identity와 Membership 변경 Timeline
- Warehouse/Generator Lease 충돌 실행 기록

## 권장 Commit

```text
feat: add deterministic source generator
```

## 다음 Phase 인계

Phase 3은 Seed와 Generator가 만든 `updated_at`/PK Cursor를 사용한다. Late Arrival Fixture와 Warehouse의 원천 데이터 동시성 잠금 충돌 Fixture를 AC-20/21의 통합 검증에 재사용한다.
