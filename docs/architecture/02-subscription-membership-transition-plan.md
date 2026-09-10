# 구독 상태와 멤버십 등급 전환 계획

> 상태: Decided — B안 (Source만 분리)
> 작성일: 2026-09-10
> 관련 문서:
> [구독 생명주기 요구사항](subscription-lifecycle-requirements.md),
> [구독·등급 테이블 분리 비교](membership-table-split-comparison.md),
> [Membership Grain 분리 기획안](membership-grain-separation.md),
> [PRD v1.8](../../PRD_v1.8.md),
> [데이터 변환 흐름](data-transformation-flow.md),
> [Phase 1](../phases/phase-01-source-environment.md),
> [Phase 2](../phases/phase-02-deterministic-generator.md),
> [Phase 3](../phases/phase-03-incremental-ingestion.md),
> [Phase 5](../phases/phase-05-dbt-duckdb-modeling.md),
> [Phase 9](../phases/phase-09-bi.md)

## 1. 결정

사람(`customer_unique_id`) 단위 Grain은 유지하되, 기존의 주문 횟수 기반 단일
`membership_level`을 다음 두 독립 속성으로 분리한다.

| 속성                  | 의미                            | 변경 원인                                  |
| --------------------- | ------------------------------- | ------------------------------------------ |
| `subscription_status` | 구독의 가입·결제·해지 생명주기  | 가입, 무료체험 종료, 결제, 결제 실패, 해지 |
| `membership_tier`     | 완료 주문 실적에 따른 고객 등급 | 완료 주문 수가 등급 경계에 도달            |

이 결정으로 구독 중인 고객의 혜택 상태와 고객의 거래 실적을 혼동하지 않는다. 예를 들어
`subscription_status = 'ACTIVE'`이면서 `membership_tier = 'GOLD'`일 수 있다.

[테이블 분리 비교](membership-table-split-comparison.md)에서 **B안(Source만 분리)**으로
확정했다. 두 속성은 별도 Source Table로 나누고 Warehouse에서 하나의 `dim_customer`로
합친다. 기존 `customer_memberships`는 `customer_subscriptions`와 `customer_loyalty_tiers`
둘로 대체된다.

분리 근거는 CHECK 제약 오염이다. 한 테이블에 두 축을 두면 등급 변경이 `updated_at`을
밀어 구독 시각 제약을 깨뜨린다. A안은 축별 `updated_at` 컬럼 2개와 CHECK 3개로 이를
막지만, 테이블을 나누면 각 테이블의 `updated_at`이 곧 축별 변경 시각이라 구조적으로
해결된다.

Dimension은 나누지 않는다. 나눠도 SCD2 총 행 수가 줄지 않는 반면 Temporal Join이 Fact마다
2회로 늘고 Late Arrival 부분 결측 규칙을 Fact 두 곳에서 정해야 한다.

## 2. 상태와 등급 계약

### 2.1 구독 상태

`subscription_status`의 허용값은 아래와 같다.

| 값                 | 의미                                   |
| ------------------ | -------------------------------------- |
| `NON_MEMBER`       | 서비스 회원이지만 구독에 가입하지 않음 |
| `TRIAL`            | 무료체험 중                            |
| `ACTIVE`           | 유료 구독 정상 이용 중                 |
| `PAYMENT_FAILED`   | 갱신 결제가 실패해 조치가 필요한 상태  |
| `CANCEL_REQUESTED` | 해지를 신청했으나 혜택 종료일 전       |
| `CHURNED`          | 구독과 혜택이 종료됨                   |

`REJOINED`는 장기 현재 상태가 아니라 `CHURNED` 뒤에 다시 `TRIAL` 또는 `ACTIVE`가 된 전이
이벤트다. Warehouse는 Bronze 관측 이력에서 이 전이를 판별해 `rejoined_at`과 재가입 횟수
측정값을 만들며, Source의 현재 상태 도메인에는 넣지 않는다.

허용 전이는 다음을 기준으로 한다.

```text
NON_MEMBER → TRIAL → ACTIVE
NON_MEMBER → ACTIVE
TRIAL/ACTIVE → PAYMENT_FAILED → ACTIVE            # 재결제 성공으로 회수
PAYMENT_FAILED → CHURNED                          # benefit_ends_at 경과, 유예 소진 해지
PAYMENT_FAILED → CANCEL_REQUESTED                 # 유예 중 사용자가 해지 신청
TRIAL/ACTIVE → CANCEL_REQUESTED → CHURNED
CHURNED → TRIAL 또는 ACTIVE                       # 재가입 이벤트
```

결제 실패는 즉시 해지가 아니다. `PAYMENT_FAILED`는 `benefit_ends_at`까지 혜택을 유지하는 유예
상태이며, 그 시각을 지나면 `CHURNED`로 전이한다. 이 전이가 없으면 결제 실패 고객이 영구히
`PAYMENT_FAILED`에 머물러 결제 실패 이탈율이 항상 0이 된다.

`CHURNED`는 종착 상태가 아니라 재가입 대기 상태다. 어떤 상태도 `NON_MEMBER`로 되돌아가지
않는다. `NON_MEMBER`는 사람마다 최초 한 번만 존재하는 시작 상태다.

### 2.2 멤버십 등급

초기 전환에서는 기존 계산 규칙을 보존하되 컬럼명을 `membership_tier`로 바꾼다.

| 값       | 완료(`delivered`) 주문 수 |
| -------- | ------------------------- |
| `BRONZE` | 0~4건                     |
| `SILVER` | 5~14건                    |
| `GOLD`   | 15건 이상                 |

등급은 구독 여부와 독립적으로 모든 서비스 회원에게 계산한다. 따라서 `NON_MEMBER` 또는
`CHURNED` 고객도 마지막 또는 현재의 `membership_tier`를 가진다. 이 등급은 구독 상품의
혜택 등급이 아니라 **거래 실적 등급**이며, 향후 혜택 정책을 추가하더라도 별도 정책표로
연결한다.

## 3. 목표 Source 모델

```text
customer_subscriptions  -- 사람(customer_unique_id)당 현재 구독 상태 1행
├── customer_unique_id       PK
├── subscription_status      NOT NULL DEFAULT 'NON_MEMBER'
├── trial_ends_at            NULL
├── benefit_ends_at          NULL
├── next_billing_at          NULL
├── payment_failed_at        NULL
├── cancel_requested_at      NULL
├── created_at               NOT NULL
└── updated_at               NOT NULL    -- 증분 Cursor, 구독 축 변경 시각

customer_loyalty_tiers  -- 사람(customer_unique_id)당 현재 등급 1행
├── customer_unique_id       PK
├── membership_tier          NOT NULL DEFAULT 'BRONZE'
├── created_at               NOT NULL
└── updated_at               NOT NULL    -- 증분 Cursor, 등급 축 변경 시각

subscription_payments   -- 사람당 N행, 결제 1건이 1행
└── 상세는 구독 생명주기 요구사항 3.3절 참조
```

각 테이블의 `updated_at`이 곧 해당 축의 변경 시각이므로 6절 제약을 그대로 CHECK로 쓴다.
등급 변경이 구독 제약을 오염시키지 않는다.

모든 시각은 UTC `TIMESTAMPTZ`를 사용한다. 구독 주기는 1개월 고정이다. `ACTIVE` 상태에서
`next_billing_at`은 직전 결제일의 1개월 뒤이며, 연 단위나 다중 요금제는 이번 범위에 없다.
Source는 현재 상태 한 행만 저장하고, 변경 전 상태는 보관하지 않는다. Bronze의 불변 스냅샷 누적과 `updated_at` Cursor가 이를 Warehouse
SCD2 이력으로 복원한다.

초기 Seed는 원본 Olist에 구독 정보가 없으므로 모든 사람을 `NON_MEMBER`로 만들고, 기존
주문 이력으로만 `membership_tier`를 계산한다. 합성 Generator가 이후의 체험·구독·결제 실패·
해지·재가입 시나리오를 만든다.

## 4. Warehouse 모델과 분석 계약

Staging은 Source마다 하나씩 둔다. `stg_customer_subscriptions`와
`stg_customer_loyalty_tiers`가 각 축을 표준화한다. 두 축은 `int_customer_history`에서
하나의 시간축으로 병합되며, 어느 한 축만 새 관측이 있으면 다른 축은 직전 값을 이어받는다.
이 단계가 두 Source의 독립적인 Watermark로 생기는 Late Arrival 부분 결측을 흡수한다.

병합된 `int_customer_history`와 `dim_customer`는 적어도 아래 속성의 변경을 SCD2 Version으로
만든다.

```text
subscription_status
membership_tier
trial_ends_at
benefit_ends_at
payment_failed_at
cancel_requested_at
```

`next_billing_at`은 SCD2 속성 Hash에서 제외한다. 구독 주기는 1개월 고정이므로 이 값은 상태
변화 없이 매달 갱신된다. Hash에 포함하면 `subscription_status = 'ACTIVE'`가 유지되는 12개월
구독 고객 한 명이 분석 가치 없는 SCD2 Version 12개를 만든다. 현재 결제 예정일은
`is_current = true` Version에서 읽는다.

주문 Fact의 고객 결합은 기존처럼 주문 시각과 `[valid_from, valid_to)` 구간으로 결정한다.
따라서 주문 당시의 구독 상태와 거래 실적 등급을 모두 분석할 수 있다. 현재 고객 분포는
`is_current = true` Version만 사용한다.

재가입은 `int_customer_history`에서 계산한다. 이 모델이 이미 사람별 전체 관측 이력을 정렬해
보므로, `lag(subscription_status)` 윈도우로 직전 Version이 `CHURNED`이고 현재 Version이 `TRIAL`
또는 `ACTIVE`인 지점을 재가입 전이로 표시한다. 같은 윈도우의 누적 합이 `rejoin_count`가 되고,
해당 Version의 `valid_from`이 `rejoined_at`이 된다.

`dim_customer`에서 계산하지 않는 이유는 SCD2 소비 패턴 때문이다. 현재 고객 분포 질의는
`is_current = true` 한 행만 읽으므로 과거 Version을 되짚을 수 없다. 이력을 이미 펼쳐 둔
`int_customer_history`에서 파생해 `dim_customer`의 각 Version에 값으로 실어 보낸다.

별도 Source 이벤트 테이블은 이번 전환 범위에 추가하지 않는다.

## 5. 구현 범위와 순서

| 순서 | 대상             | 변경 내용                                                                                                                                                    | 상태   |
| ---- | ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------ |
| 1    | PRD·Phase 문서   | 용어, 상태 전이, Source Schema, 인수 조건과 BI 요구사항을 동기화한다. PRD v1.8에 반영했다.                                                                   | 완료   |
| 2    | Source DDL       | `customer_memberships`를 `customer_subscriptions`와 `customer_loyalty_tiers`로 나누고 `subscription_payments`를 신설한다. v1.6 등급 이관 DO 블록을 삭제한다. | 재작업 |
| 3    | Seed             | 기존 등급 계산을 대문자 `membership_tier`에 적용하고 모든 Seed 고객을 `NON_MEMBER`로 초기화한다. 두 테이블에 나눠 적재한다.                                  | 재작업 |
| 4    | Generator        | 구독 상태 전이와 실적 등급 갱신을 분리하고, 시각 기반 만료 스캔과 자동결제를 만든다. 전이·시각 단조 증가·재가입 판별 테스트를 추가한다.                      | 재작업 |
| 5    | Ingestion·Bronze | Arrow Schema, 상태 도메인 검증, Schema Version, Catalog 검증을 새 컬럼에 맞춘다.                                                                             | 미진행 |
| 6    | dbt              | Staging을 축별로 나누고 `int_customer_history` 병합 단계를 만든다. SCD2 Hash, Temporal Join, 재가입 파생 측정값과 `fact_subscription_payments`를 갱신한다.   | 미진행 |
| 7    | 품질·BI          | 상태 전이·구간 비중복·등급 규칙을 검증하고 구독 퍼널, 결제 실패, 해지, 재가입, 등급별 지표를 만든다.                                                         | 미진행 |
| 8    | 재기준화         | Source와 Bronze의 호환 불가 스키마를 교체하고, 전체 재수집·Catalog 동기화·dbt build로 기준선을 다시 만든다.                                                  | 미진행 |

2~4단계는 통합 테이블(A안) 기준으로 먼저 구현했다. 이후 비교를 거쳐 B안으로 확정했으므로
해당 코드를 두 테이블 구조로 다시 만든다. 이미 작성한 상태 전이 규칙과 Seed 기준선 로직은
그대로 쓸 수 있고, 테이블 경계와 CHECK 제약 위치만 바뀐다.

## 6. 데이터 제약과 테스트

- `subscription_status`는 정의된 여섯 값만 허용한다.
- `membership_tier`는 `BRONZE`, `SILVER`, `GOLD`만 허용한다.
- 두 Source 모두 `updated_at >= created_at`을 DB CHECK 제약으로 강제한다.
- 기존 행 변경 시 새 `updated_at`이 이전 값보다 커야 한다. 단일 행 CHECK로 표현할 수 없으므로
  Generator 계약으로 강제한다. `persist_membership_records`가 이 검증을 수행한다.
- `TRIAL`에는 `trial_ends_at`, `PAYMENT_FAILED`에는 `payment_failed_at`,
  `CANCEL_REQUESTED`에는 `cancel_requested_at`이 있어야 한다.
- `CANCEL_REQUESTED`에는 `benefit_ends_at > updated_at`이 있어야 하며, `CHURNED`에는
  `benefit_ends_at <= updated_at`이 있어야 한다. `PAYMENT_FAILED`에도 유예 종료를 나타내는
  `benefit_ends_at`이 있어야 한다. 재시도 횟수와 유예 길이는 이후 제품 정책으로 정한다.
- 위 제약의 `updated_at`은 `customer_subscriptions`의 것이다. 등급 테이블이 분리되어
  있으므로 등급 변경은 이 값을 밀지 않는다.
- 위 제약은 각 행의 `updated_at` 시점에만 성립한다. Bronze 관측 시각 기준으로는 검증하지
  않는다. `CANCEL_REQUESTED` 행은 `benefit_ends_at`이 지나도 다음 전이 배치 전까지 Source에
  그대로 남아 매일 같은 값으로 관측되며, 이는 정상이다. 혜택 종료일 경과 후 `CHURNED` 전이는
  Generator의 후속 실행이 만든다.
- 동일 사람·동일 관측 시각에 서로 다른 속성 Hash가 생기지 않아야 하며, SCD2 유효 구간은 겹치지 않는다.
- `CHURNED → TRIAL/ACTIVE` 전이를 재가입으로 정확히 한 번 계산한다.

세부적인 결제 재시도 횟수, 유예 기간, 등급별 실제 혜택과 가격은 제품 정책이 정해진 뒤
별도 결정한다. 이 전환은 그러한 정책을 수용할 데이터 경계와 검증 가능한 상태 전이를
먼저 만든다.

## 7. 호환성과 전환 위험

이 변경은 Source·Bronze·dbt Schema와 SCD2 속성 Hash를 모두 바꾸는 호환 불가 변경이다.
Source Table은 7개에서 9개가 된다. `customer_memberships` 하나가 둘로 나뉘고
`subscription_payments`가 새로 생긴다.
기존 Bronze Object를 새 스키마와 함께 읽으면 안 된다. 기존 Membership Grain 분리와 같은
재기준화 절차로 Source, Watermark, Bronze Object, DuckDB Catalog를 정리한 뒤 전체 수집을
실행한다.

기존 `membership_level` 값을 `membership_tier`로 이관하는 별도 DDL 블록은 만들지 않는다.
재기준화가 Source를 비우고 Seed가 동일한 주문 수 규칙으로 등급을 다시 계산하므로 결과가 같고,
이관 블록은 실행되지 않는 코드로 남는다. 같은 이유로 v1.6의 계정 단위 등급 이관 DO 블록도
`sql/source/001_create_source_tables.sql`에서 삭제한다. 과거 구독 상태는 Olist 원본으로 복원할 수 없으므로, 전환 기준 시점의 모든 기존 고객은
`NON_MEMBER` 기준선에서 시작한다. 이는 실제 과거 구독 이력이라는 주장을 하지 않는 합성
환경의 명시적 기준선이다.
