# Phase 1. Source Environment

> 상태: Planned  
> Milestone: 1 — Source Foundation  
> 선행 Phase: [Phase 0. Bootstrap](phase-00-bootstrap.md)  
> 기준 문서: [ROADMAP](ROADMAP.md), [PRD v1.4](../../PRD_v1.4.md)

## 목표

Olist Raw 데이터를 원본 Naming과 값을 최대한 유지하는 PostgreSQL OLTP Source로 구성하고, 동일 입력을 반복 적재해도 결과가 변하지 않는 Seed 절차를 완성한다.

## 핵심 계약

- PostgreSQL과 이후 Bronze는 선택한 Olist Column Name을 유지한다.
- 분석용 Rename과 상태 Canonicalization은 Source가 아니라 dbt Staging에서 수행한다.
- Source 확장은 `created_at`, `updated_at`과 Synthetic 시나리오 필드로 제한한다.
- Timestamp는 UTC `TIMESTAMPTZ`, 금액은 PRD에 정의된 고정 Precision을 사용한다.
- Seed는 임시 Staging을 거친 Transactional UPSERT로 처리한다.
- 같은 Raw 입력과 같은 `seeded_at`은 같은 Row Count와 Content Hash를 만든다.

## 선행 조건

- Phase 0의 Python, uv, Compose 검증이 통과했다.
- `.env`에 PostgreSQL Runtime Credential이 있고 Git에서 제외됐다.
- `data/raw/olist/`가 다운로드 대상 경로로 준비됐다.

## 구현 순서

### 1. PostgreSQL 인프라

- [ ] `P1-01` PostgreSQL 18.6 Compose Service 구성
- [ ] `P1-02` Readiness를 확인하는 Health Check 구성
- [ ] `P1-03` Source/Metadata/Airflow 역할별 Credential 분리
- [ ] `P1-04` `commerce_source` Database 생성
- [ ] `P1-05` `airflow_metadata` Database 생성
- [ ] `P1-06` `pipeline_metadata` Database 생성

Credential 원본은 `.env`에만 두며 SQL, Manifest, dbt Model에 기록하지 않는다.

### 2. Olist Raw Dataset

- [ ] `P1-07` KaggleHub를 이용한 결정적 다운로드 경로 구현
- [ ] `P1-08` 필수 CSV 6개 존재 여부 검증
- [ ] `P1-09` CSV Header Allowlist/Contract 검증
- [ ] `P1-10` 파일별 Raw Checksum 계산 및 기록

필수 Dataset:

```text
customers
orders
order_items
order_payments
products
sellers
```

V1 미사용 컬럼은 Raw CSV에만 보존하며 Source Schema에는 포함하지 않는다.

### 3. Source DDL

FK 의존 순서에 맞춰 구현한다.

```text
customers   products   sellers
    │          │          │
    └────── orders ───────┘
                │
        order_items  order_payments
```

- [ ] `P1-11` `customers` DDL
- [ ] `P1-12` `products` DDL
- [ ] `P1-13` `sellers` DDL
- [ ] `P1-14` `orders` DDL
- [ ] `P1-15` `order_items` DDL
- [ ] `P1-16` `order_payments` DDL
- [ ] `P1-17` PK/FK/CHECK/Numeric Precision 제약조건
- [ ] `P1-18` `(updated_at, PK)` 또는 Table별 Cursor에 맞는 증분 Index

### 4. Seed Loader

먼저 `customers` 하나로 전체 흐름을 검증한 뒤 6개 Table로 확장한다.

- [ ] `P1-19` 임시 PostgreSQL Staging Schema/Table 구성
- [ ] `P1-20` CSV 문자열을 Source Type으로 명시적 변환
- [ ] `P1-21` `created_at`, `updated_at` 등 Extension Field 계산
- [ ] `P1-22` Load 직전 Header Contract 재검증
- [ ] `P1-23` PK/FK와 필수 Domain 검증
- [ ] `P1-24` 단일 실행 단위의 Transactional UPSERT
- [ ] `P1-25` `seed_runs`에 입력, Version, Count, Hash, 상태 기록
- [ ] `P1-26` 입력/Version이 다른 비정상 재실행을 차단하는 Seed Guard

## 범위 밖

- Synthetic 신규/변경 Row 생성
- Pipeline Corruption 주입
- Bronze 적재와 Watermark 관리
- 분석용 Column Rename 또는 상태 Canonicalization
- SCD2와 Mart 구축

## 테스트

### 자동 검증

- Source DDL 생성 테스트
- CSV Header Allowlist 테스트
- Type 변환과 Timestamp UTC 테스트
- PK/FK/CHECK 위반 테스트
- Seed Transaction Rollback 테스트
- Seed Guard 입력 충돌 테스트

### Acceptance 시나리오

```text
동일 Raw + 동일 seeded_at
→ Seed 1회 결과 Count/Hash 기록
→ Seed 2회 실행
→ Row Count 동일
→ Content Hash 동일
→ PK/FK 위반 0
```

```text
Source Schema Allowlist
→ V1 제외 컬럼이 Source에 없음
→ 선택 컬럼 Naming과 값이 Raw와 일치
→ 프로젝트 Extension Field만 추가됨
```

## 요구사항 추적

| 구분 | 연결 항목                                   | 주요 증거                  |
| ---- | ------------------------------------------- | -------------------------- |
| PRD  | Section 4 공통 데이터 계약                  | Type/Timestamp Schema Test |
| PRD  | Section 5 Seed 계약                         | Raw Checksum, Seed Run     |
| PRD  | Section 6 PostgreSQL Source 계약            | DDL과 제약조건 Test        |
| ADR  | ADR-006 Single PostgreSQL Container         | Database/Role 구성         |
| ADR  | ADR-008 Source Schema 보존과 Staging 표준화 | Allowlist/Naming Test      |
| FR   | FR-01 Raw-compatible Olist Seed             | Seed Loader 실행 기록      |
| AC   | AC-14 Seed 2회                              | Count/Content Hash 비교    |
| AC   | AC-18 Source Schema Allowlist               | Schema/Value 비교 결과     |

## 산출물

- PostgreSQL Compose Service와 초기화 SQL
- Source 6개 Table DDL 및 증분 Index
- Raw Dataset 검증기와 Checksum 기록
- Transactional Seed Loader/CLI
- `seed_runs` Metadata DDL과 기록 로직
- Seed 및 Source Contract 자동 테스트

## Definition of Done

- [ ] 모든 `P1-*` Task가 완료됐다.
- [ ] PostgreSQL Health Check가 통과한다.
- [ ] 필수 CSV 6개의 Header와 Checksum이 기록된다.
- [ ] Source PK/FK/CHECK 위반이 0이다.
- [ ] AC-14와 AC-18이 자동 또는 재현 가능한 명령으로 통과한다.
- [ ] Source Naming에 분석용 Rename이 섞이지 않았다.
- [ ] Credential이 Git과 실행 증적에 노출되지 않았다.

## Portfolio Evidence

- Raw Header와 Source Schema Allowlist 비교
- Seed 1회/2회 Row Count 및 Content Hash
- FK 의존 순서와 Source ERD
- Incremental Cursor를 지원하는 Index 설명

## 권장 Commit

```text
feat: build raw-compatible postgres source
```

## 다음 Phase 인계

Phase 2는 Seed가 끝난 Source Snapshot을 기준점으로 사용한다. Generator가 변경하는 모든 Mutable Row는 이 Phase에서 정의한 `updated_at`과 제약조건을 준수해야 한다.
