# Phase 6. Data Quality & Publish

> 상태: Planned  
> Milestone: 2 — Data Platform Core  
> 선행 Phase: [Phase 5. dbt + DuckDB Modeling](phase-05-dbt-duckdb-modeling.md)  
> 기준 문서: [ROADMAP](ROADMAP.md), [PRD v1.5](../../PRD_v1.5.md)

## 목표

Ingestion과 Warehouse의 품질 책임을 명확히 분리하고, 품질 검증을 통과한 Mart만 소비 대상으로 Publish한다. 실패한 Build는 이미 Commit된 Bronze나 마지막 성공 Mart를 훼손하지 않아야 한다.

## 품질 책임 경계

| 계층      | 책임                                                                                                  | 실패 처리                      |
| --------- | ----------------------------------------------------------------------------------------------------- | ------------------------------ |
| Ingestion | Schema, Type, NULL Key, Batch Duplicate, Source Domain, Numeric Range, Broken Reference, Cursor Range | Row Quarantine 또는 Batch 실패 |
| Warehouse | Business Key, Relationship, Canonical 값, 시간 순서, SCD2, Fact Grain/Measure                         | Build/Test 실패                |
| Publish   | 검증된 Build만 소비 경로로 승격                                                                       | 이전 성공 Mart 유지            |

Phase 3과 5에서 각 계층의 기본 테스트를 구현하고, 이 Phase에서는 이를 실행 가능한 통합 Gate와 Publish 경계로 완성한다.

## 선행 조건

- Phase 3의 Quarantine과 Batch Failure 정책이 자동 테스트된다.
- Phase 5의 dbt Model과 Model-level Test가 통과한다.
- Warehouse Build를 격리할 Schema/File 경계가 결정됐다.

## 구현 순서

### 1. Ingestion Quality 통합

- [ ] `P6-01` Ingestion Validation Rule Registry 정리
- [ ] `P6-02` Row Error와 Batch Error 분류 검증
- [ ] `P6-03` Freeze된 Parent Key 기준 Broken Reference 검증
- [ ] `P6-04` Reject Rate 0/이하/초과 경계 테스트
- [ ] `P6-05` Duplicate/NULL Key/Broken FK/Invalid Status/Negative Value Fixture

검증 순서와 Error Code가 Phase 3 문서 및 PRD Section 11과 일치해야 한다.

### 2. Warehouse Quality

- [ ] `P6-06` dbt Generic Test 구성: `unique`, `not_null`, `relationships`, `accepted_values`
- [ ] `P6-07` 금액 Non-negative Custom Test
- [ ] `P6-08` 주문 Timestamp 순서 Custom Test
- [ ] `P6-09` SCD2 Overlap/Current Version Custom Test
- [ ] `P6-10` Fact FK Missing/Business Key Duplicate Test
- [ ] `P6-11` 정상 E2E Unknown Key 0 Test
- [ ] `P6-12` Fact Measure/Fan-out 회귀 Test

필수 Custom Contract:

```text
payment_value >= 0
price >= 0
freight_value >= 0
delivered_at >= purchase_at
approved_at >= purchase_at
SCD2 overlap = 0
Current Customer Version = 1
Fact FK Missing = 0
Fact Business Key Duplicate = 0
Normal E2E Unknown Key = 0
```

### 3. Publish Safety

- [ ] `P6-13` Build 대상과 Published Mart의 물리적 경계 정의
- [ ] `P6-14` Build → Test → Publish 전환 구현
- [ ] `P6-15` Publish 전환을 단일 Transaction 또는 복구 가능한 단위로 처리
- [ ] `P6-16` dbt Build/Test 실패 시 이전 Published Mart 유지
- [ ] `P6-17` Publish Run ID, Invocation ID, Hash, 상태 기록

기본 의미:

```text
dbt Build/Test 성공
→ 검증된 Build를 Published Mart로 승격

dbt Build/Test 실패
→ Bronze/Watermark 유지
→ 실패 Build 격리
→ 마지막 성공 Mart 유지
```

DuckDB 제약을 관측한 뒤 Build Schema → Test → Swap 또는 별도 Warehouse File → 검증 → 교체 중 하나를 선택하고 ADR에 근거를 기록한다.

### 4. E2E 품질 Gate

- [ ] `P6-18` Source→Bronze Catalog→Fact Count/Key 추적
- [ ] `P6-19` 성공/빈/실패/재실행 Metadata 조회 SQL
- [ ] `P6-20` 새 Clone에서 Seed→Generator→Ingestion→dbt Test 재현
- [ ] `P6-21` Phase 0~6 통합 검증 명령을 README에 반영

## 범위 밖

- 모든 운영 장애에 대한 Runbook 작성
- 성능 최적화와 Benchmark 수치
- Dashboard 구현

## 테스트와 Gate

### Corruption Matrix

| Corruption     | 기대 계층 | 기대 결과                             |
| -------------- | --------- | ------------------------------------- |
| Duplicate      | Ingestion | Quarantine/정확한 Error Count         |
| NULL Key       | Ingestion | Quarantine                            |
| Broken FK      | Ingestion | Freeze된 Parent Key 기준 격리         |
| Invalid Status | Ingestion | Source Domain Error                   |
| Negative Value | Ingestion | Numeric Range Error                   |
| SCD2 Overlap   | Warehouse | dbt Test 실패, Publish 차단           |
| Fact Fan-out   | Warehouse | Grain/Measure Test 실패, Publish 차단 |

### Acceptance 연결

| AC    | 시나리오              | 합격 증거                                 |
| ----- | --------------------- | ----------------------------------------- |
| AC-01 | E2E                   | 고정 주문의 Source→Bronze→Fact Count 추적 |
| AC-08 | 5종 Corruption        | 기대 Reject 일치, 정상 Source 무오염      |
| AC-12 | Referential Integrity | FK/Unique 통과, 정상 Unknown 0            |
| AC-13 | Observability         | 단일 SQL로 네 실행 상태 조회              |
| AC-16 | 새 Clone              | Version/Health/Seed/E2E/dbt Test 성공     |

### Publish 실패 시나리오

1. 성공 Mart를 Publish하고 Logical Hash를 기록한다.
2. 의도적으로 dbt Test가 실패하는 입력을 Build한다.
3. Publish가 거부되는지 확인한다.
4. Published Mart의 Row Count/Hash가 이전 성공 상태와 같은지 확인한다.
5. 수정 후 다시 Build/Test/Publish하고 새 Run을 기록한다.

## 요구사항 추적

| 구분 | 연결 항목                         |
| ---- | --------------------------------- |
| PRD  | Section 11 Ingestion Validation   |
| PRD  | Section 17 Data Quality와 Publish |
| PRD  | Section 18 Observability와 오류   |
| FR   | FR-12 Ingestion/Warehouse Quality |
| FR   | FR-16 Quarantine                  |

## 산출물

- 계층별 Data Quality Rule Registry
- Corruption Fixture와 통합 테스트
- dbt Generic/Custom Test Suite
- 안전한 Mart Publish Workflow
- Publish Metadata와 이전 성공 Mart 보존 테스트
- E2E 검증 명령과 새 Clone 재현 기록

## Definition of Done

- [ ] 모든 `P6-*` Task가 완료됐다.
- [ ] 5종 Corruption을 기대 계층에서 정확히 탐지한다.
- [ ] 정상 데이터 False Positive가 0이다.
- [ ] Warehouse 실패가 Bronze/Watermark를 변경하지 않는다.
- [ ] Build/Test 실패 뒤 마지막 성공 Mart의 Count/Hash가 유지된다.
- [ ] AC-01, 08, 12, 13, 16이 통과한다.
- [ ] Publish 전략과 관측 근거가 ADR에 기록됐다.

## Portfolio Evidence

- 계층별 품질 책임 Matrix
- Corruption별 Quarantine/Error Count
- 실패한 dbt Build 전후 Published Mart Hash
- Source→Bronze→Fact 단일 Record 추적
- 새 Clone 재현 로그

## 권장 Commit

```text
feat: enforce data quality and safe mart publishing
```

## 다음 Phase 인계

Phase 7은 새 기능 추가보다 Phase 0~6에서 구현한 실패·충돌·재처리 계약을 의도적으로 깨뜨리고, 탐지와 복구 과정을 Runbook으로 증명한다.
