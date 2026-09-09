# Membership Grain 분리 기획안

> 상태: Proposed
> 작성일: 2026-09-09
> 관련 문서: [PRD v1.5](../../PRD_v1.5.md), [데이터 변환 흐름](data-transformation-flow.md), [Phase 5](../phases/phase-05-dbt-duckdb-modeling.md)

## 배경

Phase 5 SCD2 구현 중 `int_customer_history` Contract 검사가 122건 실패했다. PRD 규칙 "동일 고객·동일 `updated_at`의 서로 다른 Hash는 Contract Error"에 위반된 건수다.

### 원인 분석

Olist 원본에서 `customer_id`와 `customer_unique_id`의 의미가 다르다.

| 원천 컬럼            | 의미                        | 카디널리티      |
| -------------------- | --------------------------- | --------------- |
| `customer_id`        | 주문 1건당 발급되는 계정 ID | 99441 (주문 수와 1:1) |
| `customer_unique_id` | 실제 사람 식별자            | 96096           |

재구매 고객 2997명 중 **122명이 재구매 시 다른 배송지를 사용**한다. 이 122명이 Contract 위반 건수와 정확히 일치한다.

문제의 구조는 두 층위다.

1. **표면 원인**: `seed/loader.py`가 `customers.updated_at`을 전 레코드 동일 상수(`seeded_at`)로 채운다. 서로 다른 계정이 동일 시각을 갖게 되어 "동일 시점 다중 관측"으로 보인다.
2. **근본 원인**: 한 사람이 **동시에 여러 배송지를 병행 사용**하는 데이터를, "값이 시간순으로 하나씩 바뀐다"는 SCD2 모델에 넣으려 했다. `city`/`state`는 계정 생성 시 확정되는 불변값이므로 `[valid_from, valid_to)` 구간으로 표현할 수 없다.

`updated_at`을 정확한 값으로 고쳐도 근본 원인은 남는다. 시각이 달라지면 Contract Error는 사라지지만, "6월부터 이전 주소는 무효"라는 **사실과 다른 이력**이 조용히 만들어진다.

### 추가 발견

`membership_level`은 Olist 원본에 없는 파생 컬럼이다. `seed/loader.py`가 배송완료 건수로 계산해 채운다. 계산은 `customer_unique_id`(사람) 단위로 하면서 저장은 `customer_id`(계정) 행마다 중복 복사한다. 같은 사람의 계정이 3개면 동일 등급이 3번 저장된다. 정규화 위반이며, 이 중복이 SCD2에서 다중 관측으로 나타난다.

## 설계 원칙

Grain에 따라 테이블을 분리한다.

| 테이블                 | Grain                        | 가변성 | SCD2 대상 |
| ---------------------- | ---------------------------- | ------ | --------- |
| `customers`            | 계정 (주문당 1개)            | 불변   | 아니오    |
| `customer_memberships` | 사람 (`customer_unique_id`)  | 가변   | **예**    |

`city`/`state`는 계정 불변값이므로 SCD2에서 제외하고 주문 시점 배송지 스냅샷으로 취급한다. `membership_level`만 사람 단위 가변값이므로 SCD2로 추적한다.

이 구조는 실무의 표준 패턴을 따른다. 원천은 현재 상태만 보관하고, Warehouse가 Bronze 스냅샷 누적으로 이력을 복원한다. PRD 7.5 "Source는 현재 상태만 보관하므로"가 전제하는 구조와 일치한다.

## 변경 내역

### 1. Source 스키마 (Phase 1)

```sql
-- 변경: 계정 단위, 불변
customers (
    customer_id VARCHAR(64) COLLATE "C" PRIMARY KEY,
    customer_unique_id VARCHAR(64) COLLATE "C" NOT NULL,
    customer_city VARCHAR(128),
    customer_state CHAR(2),
    created_at TIMESTAMPTZ NOT NULL
)
-- 제거: membership_level, updated_at
-- 제거: customers_membership_level_check, customers_updated_at_check

-- 신규: 사람 단위, 가변
customer_memberships (
    customer_unique_id VARCHAR(64) COLLATE "C" PRIMARY KEY,
    membership_level VARCHAR(16) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT customer_memberships_level_check
        CHECK (membership_level IN ('bronze', 'silver', 'gold')),
    CONSTRAINT customer_memberships_updated_at_check
        CHECK (updated_at >= created_at)
)
```

PK가 `customer_unique_id`이므로 "한 사람 = 한 행"이 DB 제약으로 보장된다. 동일 시점 다중 관측이 물리적으로 불가능하므로 Contract Error가 원천 소멸한다.

### 2. 증분 수집 계약 (Phase 3)

| 테이블                 | Cursor 컬럼  | 근거                                       |
| ---------------------- | ------------ | ------------------------------------------ |
| `customers`            | `created_at` | 불변 테이블. `order_items`와 동일 패턴     |
| `customer_memberships` | `updated_at` | 가변 테이블. 등급 변경 시 갱신             |

Source Table이 6개에서 **7개**로 늘어난다. `bronze_source` Macro의 `supported_tables`, Warehouse DAG 수집 대상, Bronze Object 경로 규칙에 반영한다.

### 3. Seed 정책 (Phase 2)

- `customers`: `membership_level`·`updated_at` 제거. `created_at`은 계정별 첫 주문 시각 유지
- `customer_memberships`: 사람 단위로 배송완료 건수를 집계해 등급을 산정한다. 1인 1행
- `updated_at`은 `seeded_at`을 유지한다. 사람 단위 1행이므로 충돌이 발생하지 않는다

### 4. Generator 정책 (Phase 2)

- `CustomerRecord`에서 `membership_level`·`updated_at`을 제거해 순수 계정 레코드로 만든다
- `MembershipRecord`를 신설한다 (사람 단위)
- `membership_change_scenario`를 Orchestrator에 연결한다. 현재는 정의만 되어 있고 호출되지 않아 실제 UPDATE가 발생하지 않는다
- PRD 7.5 "고객당 수집 구간 내 최대 1회 변경" 제약이 PK 구조로 자연 보장된다

### 5. Warehouse 모델 (Phase 5)

| 모델                        | 변경                                                     |
| --------------------------- | -------------------------------------------------------- |
| `stg_customers_current`     | `membership_level` 제거. 주소 대표 선택 로직 축소        |
| `stg_customer_observations` | `stg_membership_observations`로 대체. 소스가 새 테이블   |
| `int_customer_history`      | 추적 속성 `membership_level` 1개. `attribute_hash` 단순화 |
| `dim_customer`              | Grain은 사람 Version. `city`/`state` 제외                |
| `int_orders_enriched`       | 배송지를 주문별 스냅샷으로 직접 보유                     |

SCD2 Business Key는 `customer_unique_id`를 유지한다. `valid_from` 규칙(최초 Version은 `created_at`, 이후는 `updated_at`)도 변경하지 않는다.

### 6. 주소 처리

`city`/`state`는 SCD2에서 제외하고 주문 시점 배송지 스냅샷으로 취급한다. `int_orders_enriched`가 `stg_customers_current`에서 `source_customer_id`로 직접 가져온다. 계정 불변값이므로 주문 시점 값이 곧 정확한 값이다. Kimball의 Degenerate Dimension 패턴에 해당한다.

## 인수 조건 영향

| AC    | 변경                                          |
| ----- | --------------------------------------------- |
| AC-09 | 유지. 오히려 정상 검증이 가능해진다           |
| AC-10 | 유지                                          |
| AC-12 | 유지                                          |
| AC-19 | Mapping 표에 `customer_memberships` 추가      |
| AC-22 | 유지                                          |

FR-15(SCD2/Temporal Join, P0)는 그대로 유지된다.

## 문서 수정 대상

| 문서                                       | 수정 내용                                                              |
| ------------------------------------------ | ---------------------------------------------------------------------- |
| `PRD_v1.5.md` → `v1.6`                     | Section 7.5, 14.1, 15, Source Table 목록(6→7), AC-19                    |
| `docs/phases/phase-01-*`                   | DDL 변경, 테이블 7개                                                   |
| `docs/phases/phase-02-*`                   | Seed·Generator 정책                                                    |
| `docs/phases/phase-03-*`                   | 수집 계약, Cursor 컬럼                                                 |
| `docs/phases/phase-05-*`                   | 모델 정의, SCD2 규칙, 추적 속성                                        |
| `docs/architecture/data-transformation-flow.md` | 계층별 표, ERD, 주문 여정 예시                                    |

## 실행 순서

1. Source DDL 수정
2. `src/ingestion/tables.py` 수집 계약
3. `src/seed/loader.py` Seed 정책
4. `src/generator/customers.py` Generator 정책
5. 재시드 + Bronze 재생성
6. dbt 모델 재작성
7. 테스트 수정
8. 문서 갱신

5번이 되돌리기 어려운 지점이다. 기존 Bronze Object가 구 스키마이므로 진행 전에 `schema_version` 상향과 전체 재생성 중 하나를 결정해야 한다.

## 리스크

| 항목            | 내용                                                                             |
| --------------- | -------------------------------------------------------------------------------- |
| 재시드 차단     | `_assert_seed_guard`가 `raw_checksum` 불일치와 기존 Generator Run으로 막는다     |
| Bronze 호환성   | 기존 Object는 구 스키마다. `schema_version` 상향 또는 전체 재수집이 필요하다     |
| Phase 5 재작업  | 이미 구현한 Dimension·Intermediate Model 상당수를 재작성한다                     |
| 작업량          | 코드 10개, 테스트 5개, 문서 6개                                                  |

## 기대 효과

- Contract Error 122건이 구조적으로 소멸한다. 우회가 아닌 근본 해결이다
- 원천 정규화로 등급 중복 저장이 사라진다. 계정 3개면 3번 저장하던 것을 1번으로 줄인다
- Olist 원본에 없던 컬럼이 별도 테이블로 분리되어 원본 호환성이 향상된다
- SCD2가 진짜 시간순 단일값만 추적하므로 의미가 명확해진다
- 유료 구독 멤버십 모델과 개념이 일치한다

## 검토한 대안

| 대안                                | 기각 사유                                                                       |
| ----------------------------------- | ------------------------------------------------------------------------------- |
| `updated_at`만 정확한 값으로 수정   | Contract Error는 사라지지만 "병행 주소"를 "이사"로 잘못 모델링한다              |
| Contract 검사 키 완화               | 문제를 덮는다. `dim_customer`에 겹치는 구간이 남아 AC-09가 대신 실패한다        |
| SCD2 Grain을 계정 단위로 하향       | 사람 단위 등급 이력 추적이 불가능해진다                                         |
| `membership_level` 제거             | FR-15(P0), AC-09, AC-10이 소멸한다. SCD2 기능 전체가 사라진다                   |
| 원천에 `valid_from`/`valid_to` 저장 | PRD 7.5와 충돌한다. Warehouse의 SCD2 복원이 단순 복사로 축소된다                |
