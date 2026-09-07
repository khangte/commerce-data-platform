# Phase 2. Deterministic Generator

> 상태: In Progress
>
> Milestone: 1 — Source Foundation
>
> 선행 Phase: [Phase 1. Source Environment](phase-01-source-environment.md)
>
> 기준 문서: [ROADMAP](ROADMAP.md), [PRD v1.4](../../PRD_v1.4.md)

## 목표

정적 Olist Source를 신규 주문, 상태 변경, 고객 속성 변경, Late Arrival이 반복해서 발생하는 서비스형 OLTP Source로 만든다. 동일 Snapshot과 동일 입력은 동일한 변경 결과를 생성해야 한다.

## 핵심 계약

- Business ID는 UUIDv5 또는 결정적 Hash로 생성한다.
- `customer_unique_id`는 동일 인물의 Business Key, `customer_id`는 주문 시점 Customer Record다.
- 기존 Mutable Row의 새 `updated_at`은 직전 값보다 반드시 크다.
- Late Arrival은 과거 Business Event Time과 현재 Source Mutation Time으로 표현한다.
- Order/Item/Payment 묶음은 하나의 Transaction으로 생성한다.
- Warehouse가 Source Freeze Lease를 보유하는 동안 Generator는 Source를 변경하지 않는다.
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

- [ ] 상태 전이 전 현재 상태와 기대 Version 확인
- [ ] 기존 Row 변경 시 `updated_at` 단조 증가 보장
- [ ] 동일 `updated_at`에서 서로 다른 값으로 변경 금지
- [ ] Source에는 Raw-compatible 상태를 저장하고 Canonicalization은 수행하지 않음

### 5. Service-level 시나리오

- [ ] `P2-15` Late Order 생성
- [ ] `P2-16` Delayed Payment 생성
- [ ] `P2-17` 과거 Event Timestamp를 가진 Late Update 생성
- [ ] `P2-18` Membership Change 생성

Pipeline 검증용 Duplicate/NULL Key/Broken FK 같은 Corruption은 정상 OLTP Source를 오염시키지 않고 Phase 3/6의 주입 경로에서 처리한다.

### 6. Source Mutation 동시성

- [ ] `P2-19` `source_mutation_leases` DDL과 Global Lease 조회/획득 로직
- [ ] `P2-20` Lease 획득 실패 시 Source 변경 전 안전하게 종료
- [ ] `P2-21` Generator Transaction 종료 후 Lease 해제
- [ ] `P2-22` 만료/소유권 상실 시 변경을 중단하는 Fencing 검증

## 범위 밖

- Bronze/Quarantine Object 생성
- Pipeline-level Corruption을 Source DB에 저장
- Airflow DAG Scheduling
- dbt 상태 Canonicalization과 SCD2 모델 구현

## 테스트

### 결정성

기준 Snapshot을 복제해 동일 입력으로 각각 실행하고 다음을 비교한다.

```text
생성/변경 Key Set
Entity별 Row Count
상태와 비즈니스 값
Logical Content Hash
```

### 계약

- 허용되지 않은 Order/Payment 상태 전이가 0인지 검증
- 모든 기존 Row 변경에서 `new.updated_at > old.updated_at`인지 검증
- Late Arrival에서 Business Event Time은 과거이고 Mutation Time은 현재인지 검증
- 실패한 복합 생성에서 부분 Order/Item/Payment가 남지 않는지 검증
- Warehouse Lease 보유 중 생성 Row/변경 Row가 0인지 검증

## 요구사항 추적

| 구분 | 연결 항목                                   | 주요 증거                |
| ---- | ------------------------------------------- | ------------------------ |
| PRD  | Section 4.1 `updated_at` 안전성 계약        | Mutation Time Test       |
| PRD  | Section 7 Generator와 Anomaly               | 결정성 및 상태 전이 Test |
| PRD  | Section 7.3 Global Source Mutation Lease    | Lease 충돌 Test          |
| ADR  | ADR-009 Observed Customer SCD2              | 변경 횟수/관측 계약      |
| ADR  | ADR-013 Source Mutation/Extract 동시성      | Lease Test               |
| FR   | FR-02 Deterministic Generator               | 동일 Snapshot 비교       |
| FR   | FR-05 Global Source Mutation Lease          | 동시성 실행 기록         |
| AC   | AC-15 Generator 재현                        | Key Set/Hash 비교        |
| AC   | AC-20 Mutation Time Cursor Safety의 생성 측 | Late Update Fixture      |
| AC   | AC-21 Generator vs Warehouse의 생성 측      | Source 변경 0 증거       |

AC-20과 AC-21의 전체 E2E 판정은 Phase 3의 Ingestion과 결합해 완료한다.

## 산출물

- Generator Config와 CLI
- Deterministic ID/Hash Utility
- Customer/Order/Item/Payment 생성 모듈
- 상태 변경과 Late Arrival 시나리오
- `generator_runs` Metadata
- Source Mutation Lease Client
- 결정성, Transaction, Mutation Time 자동 테스트

## 파일·폴더별 변경 요약

| 경로                                                       | 변경 | 요약                                                                                    |
| ---------------------------------------------------------- | ---- | --------------------------------------------------------------------------------------- |
| `src/generator/config.py`                                  | 생성 | 결정성 실행 Config, UTC `logical_date`, 지원 Version과 Anomaly Profile 검증을 추가했다. |
| `src/generator/ids.py`                                     | 생성 | UUIDv5 Business ID와 안정적인 Logical Content Hash 유틸리티를 추가했다.                 |
| `src/generator/metadata.py`                                | 생성 | `generator_runs` Schema 준비와 RUNNING/완료 실행 이력 기록 기능을 추가했다.             |
| `src/generator/customers.py`                               | 생성 | 신규·재구매·주소 변경 Customer Record와 결정적 Membership 변경 계획을 추가했다.         |
| `src/generator/orders.py`                                  | 생성 | Order·Item·Payment Bundle 생성, Seed Catalog 선택, 원자적 멱등 저장을 추가했다.          |
| `src/generator/__main__.py`                                | 생성 | Source를 변경하지 않고 Generator 입력과 Metadata 초기화를 검증하는 CLI를 추가했다.      |
| `src/generator/__init__.py`                                | 수정 | Generator Config와 현재 구현 Version을 Package API로 노출했다.                          |
| `sql/metadata/002_create_generator_metadata.sql`           | 생성 | 결정성 입력, 결과 Count/Hash, 실행 상태를 보관하는 `generator_runs` 테이블을 추가했다.  |
| `tests/generator/`                                         | 생성 | Config, 결정적 ID/Hash, Metadata 입력 기록 단위 테스트를 추가했다.                      |
| `tests/integration/test_generator_metadata_integration.py` | 생성 | 실제 PostgreSQL에 Generator 실행 이력이 저장되는지 검증하는 통합 테스트를 추가했다.     |
| `tests/integration/test_generator_customer_integration.py` | 생성 | Customer Record 저장 멱등성과 Membership 변경 시각을 검증하는 통합 테스트를 추가했다.   |
| `tests/integration/test_generator_order_integration.py`    | 생성 | Order Bundle의 Insert/Skip, FK 오류 Rollback 통합 테스트를 추가했다.                    |
| `docs/phases/phase-02-deterministic-generator.md`          | 수정 | Phase 진행 상태와 P2-01~11 완료, 파일별 변경 요약을 기록했다.                           |

## Definition of Done

- [ ] 모든 `P2-*` Task가 완료됐다.
- [ ] 동일 Snapshot/입력의 Key Set, Count, 상태, Hash가 같다.
- [ ] 허용되지 않은 상태 전이가 0이다.
- [ ] Mutable Row의 `updated_at` 역행 또는 동일 위치 변경이 0이다.
- [ ] 복합 Entity 생성이 원자적으로 동작한다.
- [ ] Warehouse Lease 중 Generator 변경이 0이다.
- [ ] AC-15가 통과하고 AC-20/21용 Fixture가 준비됐다.

## Portfolio Evidence

- 동일 입력을 두 번 실행한 Key Set/Hash 비교
- Business Event Time과 Mutation Time이 다른 Late Arrival 예시
- Customer Identity와 Membership 변경 Timeline
- Warehouse/Generator Lease 충돌 실행 기록

## 권장 Commit

```text
feat: add deterministic source generator
```

## 다음 Phase 인계

Phase 3은 Seed와 Generator가 만든 `updated_at`/PK Cursor를 사용한다. Late Arrival Fixture와 Warehouse Lease 충돌 Fixture를 AC-20/21의 통합 검증에 재사용한다.
