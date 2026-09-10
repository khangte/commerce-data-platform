# Phase 1. Source Environment

> 상태: Done  
> Milestone: 1 — Source Foundation  
> 선행 Phase: [Phase 0. Bootstrap](phase-00-bootstrap.md)  
> 기준 문서: [ROADMAP](ROADMAP.md), [PRD v1.7](../../PRD_v1.7.md)

## 목표

Olist Raw 데이터를 원본 Naming과 값을 최대한 유지하는 PostgreSQL OLTP Source로 구성하고, 동일 입력을 반복 적재해도 결과가 변하지 않는 Seed 절차를 완성한다.

## 핵심 계약

- PostgreSQL과 이후 Bronze는 선택한 Olist Column Name을 유지한다.
- 분석용 Rename과 상태 표준화는 Source가 아니라 dbt Staging에서 수행한다.
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

- [x] `P1-01` PostgreSQL 18.6 Compose Service 구성
- [x] `P1-02` Readiness를 확인하는 Health Check 구성
- [x] `P1-03` Source/Metadata/Airflow 역할별 Credential 분리
- [x] `P1-04` `commerce_source` Database 생성
- [x] `P1-05` `airflow_metadata` Database 생성
- [x] `P1-06` `pipeline_metadata` Database 생성

Credential 원본은 `.env`에만 두며 SQL, Manifest, dbt Model에 기록하지 않는다.

### 2. Olist Raw Dataset

- [x] `P1-07` KaggleHub를 이용한 결정적 다운로드 경로 구현
- [x] `P1-08` 필수 CSV 6개 존재 여부 검증
- [x] `P1-09` CSV Header Allowlist/Contract 검증
- [x] `P1-10` 파일별 Raw Checksum 계산 및 기록

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

KaggleHub는 Dataset 전체를 내려받으므로 `geolocation`, `reviews`, `product_category_name_translation` CSV도 `data/raw/olist/`에 존재한다. 세 파일은 V1 제외이며 Seed 대상이 아니다. `P1-08`은 필수 6개만 검증하고 나머지 파일의 존재를 실패로 처리하지 않는다. 제외 근거는 PRD Section 5.1에 있다.

### 3. Source DDL

FK 의존 순서에 맞춰 구현한다.

```text
customers   products   sellers
    │          │          │
    └────── orders ───────┘
                │
        order_items  order_payments
```

- [x] `P1-11` `customers` DDL
- [x] `P1-12` `products` DDL
- [x] `P1-13` `sellers` DDL
- [x] `P1-14` `orders` DDL
- [x] `P1-15` `order_items` DDL
- [x] `P1-16` `order_payments` DDL
- [x] `P1-17` PK/FK/CHECK/Numeric Precision 제약조건
- [x] `P1-18` `(updated_at, PK)` 또는 Table별 Cursor에 맞는 증분 Index

### 4. Seed Loader

먼저 `customers` 하나로 전체 흐름을 검증한 뒤 7개 Table로 확장한다.

- [x] `P1-19` 임시 PostgreSQL Staging Schema/Table 구성
- [x] `P1-20` CSV 문자열을 Source Type으로 명시적 변환
- [x] `P1-21` `created_at`, `updated_at` 등 Extension Field 계산
- [x] `P1-22` Load 직전 Header Contract 재검증
- [x] `P1-23` PK/FK와 필수 Domain 검증
- [x] `P1-24` 단일 실행 단위의 Transactional UPSERT
- [x] `P1-25` `seed_runs`에 입력, Version, Count, Hash, 상태 기록
- [x] `P1-26` 입력/Version이 다른 비정상 재실행을 차단하는 Seed Guard

## 범위 밖

- Synthetic 신규/변경 Row 생성
- 파이프라인 오류 주입
- Bronze 적재와 Watermark 관리
- 분석용 Column Rename 또는 상태 표준화
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
- Source 7개 Table DDL 및 증분 Index
- Raw Dataset 검증기와 Checksum 기록
- Transactional Seed Loader/CLI
- `seed_runs` Metadata DDL과 기록 로직
- Seed 및 Source Contract 자동 테스트

## 파일·폴더별 변경 요약

| 경로                                             | 변경      | 요약                                                                       |
| ------------------------------------------------ | --------- | -------------------------------------------------------------------------- |
| `.env.example`                                   | 수정      | PostgreSQL 포트, Database, 역할별 계정 환경 변수 계약을 추가했다.          |
| `compose.yaml`                                   | 수정      | PostgreSQL 18.6 서비스, 영속 Volume, 초기화 SQL, Health Check를 추가했다.  |
| `sql/bootstrap/01-create-databases-and-roles.sh` | 생성      | Source·Metadata·Airflow Database와 역할을 멱등적으로 생성하도록 추가했다.  |
| `sql/source/001_create_source_tables.sql`        | 수정      | 불변 계정 `customers`와 사람 단위 `customer_memberships` 7개 테이블, 기존 계정 멤버십의 호환 마이그레이션, Cursor Index를 반영했다. |
| `sql/metadata/001_create_seed_metadata.sql`      | 생성      | Seed 실행 이력과 Count/Hash/상태를 기록하는 `seed_runs` 테이블을 추가했다. |
| `src/common/database.py`                         | 생성      | `.env` 기반 PostgreSQL 연결과 SQL 적용 공통 기능을 추가했다.               |
| `src/seed/contracts.py`                          | 생성      | CSV 파일·헤더·기본 키 계약 검증과 Raw Checksum 계산을 추가했다.            |
| `src/seed/loader.py`                             | 수정      | CSV 변환, 검증, 임시 Staging, Transactional UPSERT에 사람 단위 Membership Seed를 추가했다. |
| `src/seed/__main__.py`                           | 생성      | `python -m src.seed` CLI와 `seeded_at` 입력 처리를 추가했다.               |
| `src/__init__.py`, `src/seed/__init__.py`        | 생성·수정 | Seed 모듈을 Python Package로 구성했다.                                     |
| `tests/seed/test_contracts.py`                   | 생성      | CSV 계약과 Timestamp/Checksum 단위 테스트를 추가했다.                      |
| `tests/integration/test_seed_integration.py`     | 생성      | 멱등 적재, Source Allowlist, Seed Guard 통합 테스트를 추가했다.            |
| `pyproject.toml`                                 | 수정      | pytest 경로와 PostgreSQL 통합 테스트 Marker를 추가했다.                    |
| `README.md`                                      | 수정      | PostgreSQL 기동, Seed 실행, 통합 테스트 명령을 추가했다.                   |
| `docs/phases/phase-01-source-environment.md`     | 수정      | 완료 상태, 체크리스트, 검증 증적, 내부 용어의 한국어 표기를 반영했다.      |

Membership Grain 분리 후 `customers`는 계정 불변값과 `created_at`만 보관하고,
`customer_memberships`가 사람 단위 등급과 `created_at`·`updated_at`을 보관한다. Seed는 완료
주문 수를 `customer_unique_id`별로 집계해 정확히 한 Membership 행을 만든다.

## Definition of Done

- [x] 모든 `P1-*` Task가 완료됐다.
- [x] PostgreSQL Health Check가 통과한다.
- [x] 필수 CSV 6개의 Header와 Checksum이 기록된다.
- [x] Source PK/FK/CHECK 위반이 0이다.
- [x] AC-14와 AC-18이 자동 또는 재현 가능한 명령으로 통과한다.
- [x] Source Naming에 분석용 Rename이 섞이지 않았다.
- [x] Credential이 Git과 실행 증적에 노출되지 않았다.

## 검증 증적

2026-09-07에 PostgreSQL `18.6`을 호스트 포트 `5433`에서 기동하고 아래 명령을 실행했다.

```bash
uv run ruff check .
uv run pytest
RUN_POSTGRES_INTEGRATION=1 uv run pytest -q \
  tests/integration/test_seed_integration.py::test_same_raw_input_and_seeded_at_are_idempotent
RUN_POSTGRES_INTEGRATION=1 uv run pytest -q \
  tests/integration/test_seed_integration.py::test_source_schema_keeps_only_the_selected_raw_columns_and_extensions \
  tests/integration/test_seed_integration.py::test_seed_guard_rejects_a_changed_baseline_after_success
```

- Unit Test: `10 passed, 3 skipped`
- AC-14: 동일 Raw와 `seeded_at` 재실행 결과 `1 passed`
- AC-18 및 Seed Guard: `2 passed`
- PostgreSQL Health Check: `healthy`

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
