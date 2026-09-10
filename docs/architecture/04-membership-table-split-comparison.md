`# 구독·등급 테이블 분리 비교

> 상태: Decided — B안 (Source만 분리)
> 작성일: 2026-09-10
> 관련 문서:
> [구독 생명주기 요구사항](subscription-lifecycle-requirements.md),
> [구독 상태와 멤버십 등급 전환 계획](subscription-membership-transition-plan.md),
> [Membership Grain 분리 기획안](membership-grain-separation.md),
> [데이터 변환 흐름](data-transformation-flow.md)

## 0. 이 문서의 목적

구독 생명주기(`subscription_status`)와 거래 실적 등급(`membership_tier`)을 하나의 Source
Table과 하나의 Dimension으로 관리할지, Source만 나눌지, Dimension까지 나눌지를 결정한다.

## 1. 두 종류의 분리를 구분한다

이 문서가 다루는 분리와 이미 확정된 분리는 성격이 다르다. 혼동하면 결정이 어긋난다.

### 1.1 Grain 분리 (확정, 선택지 아님)

[구독 생명주기 요구사항](subscription-lifecycle-requirements.md)에서 확정한
`subscription_payments` 분리는 **Grain이 달라서 생긴 분리**다.

```text
customer_memberships    사람당 1행    현재 상태
subscription_payments   사람당 N행    결제 이력
```

행 수가 다르므로 한 테이블에 넣을 수 없다. 선택의 여지가 없다.

### 1.2 속성 분리 (이 문서의 주제)

구독과 등급은 **둘 다 사람당 1행**이다. 같은 Grain이므로 한 테이블에 넣을 수 있고, 나눌
수도 있다. 이것이 결정 대상이다.

```text
customer_subscriptions   사람당 1행
customer_loyalty_tiers   사람당 1행
```

결제 테이블을 만들었다고 해서 등급 분리가 따라오지 않는다. 결제를 나눠도 등급은 여전히
어딘가에 있어야 한다.

## 2. 결제 테이블이 바꾼 것

`subscription_payments` 확정이 이 비교의 전제를 세 가지 바꿨다.

**`customer_memberships`의 구성이 기울었다.** 컬럼 11개 중 구독이 8개, 등급이 2개다.
테이블 이름은 등급을 뜻하는데 내용은 사실상 구독 테이블이다.

| 컬럼                                                  | 축   | 변경 원인              |
| ----------------------------------------------------- | ---- | ---------------------- |
| `subscription_status`, `subscription_updated_at`      | 구독 | 결제 이벤트, 만료 스캔 |
| `trial_ends_at`, `benefit_ends_at`, `next_billing_at` | 구독 | 결제 이벤트, 만료 스캔 |
| `payment_failed_at`, `cancel_requested_at`            | 구독 | 결제 이벤트, 만료 스캔 |
| `membership_tier`, `tier_updated_at`                  | 등급 | 주문 완료 수           |

**Source 표면 기준선이 이미 올랐다.** `subscription_payments` 확정으로 7개에서 8개가 됐다.
"7개 유지"는 더 이상 통합안의 장점이 아니다. 8개와 9개의 차이는 7개와 8개의 차이와 같은 폭이다.

**Generator 코드가 이미 두 갈래다.** 만료 스캔은 구독 축만 건드리고, 등급 갱신은 주문
완료 수만 본다. 한 테이블이면 두 로직이 같은 행을 UPDATE한다.

## 3. 후보 세 안

### 3.1 A안: 통합

Source 하나에 두 축을 두고 축별 `updated_at`으로 구분한다.

```text
Source     customer_memberships          -- 구독 + 등급, 축별 updated_at
           subscription_payments
           (합계 8개)

Warehouse  dim_customer                  -- 두 속성이 한 Dimension
Fact       fact_orders.customer_key                 -- Temporal Join 1회
           fact_subscription_payments.customer_key  -- Temporal Join 1회
```

### 3.2 B안: Source만 분리

Source는 축별로 나누고 dbt에서 다시 합쳐 Dimension은 하나로 만든다.

```text
Source     customer_subscriptions        -- 구독 상태 + 시각 5개
           customer_loyalty_tiers        -- membership_tier
           subscription_payments
           (합계 9개)

Warehouse  stg_customer_subscriptions
           stg_customer_loyalty_tiers
           int_customer_history          -- 두 Staging을 여기서 합침
           dim_customer                  -- 여전히 하나
Fact       fact_orders.customer_key                 -- Temporal Join 1회
           fact_subscription_payments.customer_key  -- Temporal Join 1회
```

### 3.3 C안: 완전 분리

Source와 Dimension을 모두 나눈다.

```text
Source     customer_subscriptions
           customer_loyalty_tiers
           subscription_payments
           (합계 9개)

Warehouse  dim_customer_subscription
           dim_customer_tier
Fact       fact_orders.subscription_key + tier_key                -- Temporal Join 2회
           fact_subscription_payments.subscription_key + tier_key -- Temporal Join 2회
```

D안(Source 1개 + Dimension 2개)은 한 Source를 두 Dimension으로 쪼갤 근거가 없어 제외한다.

## 3.5 안별 상세 스키마

세 안이 공통으로 두는 테이블은 아래에 반복하지 않는다.

- `subscription_payments` (Source): 세 안 동일. 사람당 N행, 결제 1건이 1행.
- `fact_subscription_payments` (Fact): 세 안 동일한 Grain. FK 구성만 안별로 다르다.
- `customers`, `orders`, `order_items`, `order_payments`, `products`, `sellers`: 이번 변경과
  무관하므로 생략한다.

`+`는 추가 컬럼, `~`는 기존 대비 바뀐 컬럼을 뜻한다.

### 3.5.1 A안: 통합

**Source: `customer_memberships`** (사람당 1행)

| 컬럼                      | 타입        | 비고                                         |
| ------------------------- | ----------- | -------------------------------------------- |
| `customer_unique_id`      | VARCHAR(64) | PK                                           |
| `subscription_status`     | VARCHAR(20) | `+` NOT NULL DEFAULT `NON_MEMBER`            |
| `subscription_updated_at` | TIMESTAMPTZ | `+` 구독 축 변경 시각                        |
| `membership_tier`         | VARCHAR(10) | `~` 기존 `membership_level` 개명             |
| `tier_updated_at`         | TIMESTAMPTZ | `+` 등급 축 변경 시각                        |
| `trial_ends_at`           | TIMESTAMPTZ | `+` NULL                                     |
| `benefit_ends_at`         | TIMESTAMPTZ | `+` NULL                                     |
| `next_billing_at`         | TIMESTAMPTZ | `+` NULL                                     |
| `payment_failed_at`       | TIMESTAMPTZ | `+` NULL                                     |
| `cancel_requested_at`     | TIMESTAMPTZ | `+` NULL                                     |
| `created_at`              | TIMESTAMPTZ | NOT NULL                                     |
| `updated_at`              | TIMESTAMPTZ | `~` `greatest(축별 updated_at)`, Cursor 전용 |

CHECK 3개 추가. 4.2절 참조.

**Staging: `stg_customer_observations`** (1개)

Bronze `customer_memberships` 관측 하나에서 두 축을 모두 표준화한다. `attribute_hash`는
`subscription_status`, `membership_tier`, `trial_ends_at`, `benefit_ends_at`,
`payment_failed_at`, `cancel_requested_at`을 이어붙여 계산한다. `next_billing_at`은 제외.

**Intermediate: `int_customer_history`** (1개)

`stg_customer_observations` 하나만 입력. `lag(attribute_hash)`로 Version 경계를 만든다.
현행 구조와 동일.

**Dimension: `dim_customer`** (1개)

| 컬럼                   | 비고                            |
| ---------------------- | ------------------------------- |
| `customer_key`         | Surrogate PK                    |
| `customer_id`          | 사람 Business Key               |
| `subscription_status`  | `+`                             |
| `membership_tier`      | `~`                             |
| `trial_ends_at` 외 3개 | `+` 상태 시각                   |
| `rejoin_count`         | `+` `int_customer_history` 파생 |
| `rejoined_at`          | `+`                             |
| `attribute_hash`       |                                 |
| `valid_from`           |                                 |
| `valid_to`             |                                 |
| `is_current`           |                                 |

**Fact FK 구성**

| Fact                         | FK                 | Temporal Join |
| ---------------------------- | ------------------ | ------------- |
| `fact_orders`                | `customer_key` 1개 | 1회           |
| `fact_subscription_payments` | `customer_key` 1개 | 1회           |

### 3.5.2 B안: Source만 분리

**Source: `customer_subscriptions`** (사람당 1행)

| 컬럼                  | 타입        | 비고                          |
| --------------------- | ----------- | ----------------------------- |
| `customer_unique_id`  | VARCHAR(64) | PK                            |
| `subscription_status` | VARCHAR(20) | NOT NULL DEFAULT `NON_MEMBER` |
| `trial_ends_at`       | TIMESTAMPTZ | NULL                          |
| `benefit_ends_at`     | TIMESTAMPTZ | NULL                          |
| `next_billing_at`     | TIMESTAMPTZ | NULL                          |
| `payment_failed_at`   | TIMESTAMPTZ | NULL                          |
| `cancel_requested_at` | TIMESTAMPTZ | NULL                          |
| `created_at`          | TIMESTAMPTZ | NOT NULL                      |
| `updated_at`          | TIMESTAMPTZ | NOT NULL, 이 테이블 Cursor    |

CHECK는 이 테이블의 `updated_at` 기준. 축별 컬럼 불필요.

**Source: `customer_loyalty_tiers`** (사람당 1행)

| 컬럼                 | 타입        | 비고                       |
| -------------------- | ----------- | -------------------------- |
| `customer_unique_id` | VARCHAR(64) | PK                         |
| `membership_tier`    | VARCHAR(10) | NOT NULL DEFAULT `BRONZE`  |
| `created_at`         | TIMESTAMPTZ | NOT NULL                   |
| `updated_at`         | TIMESTAMPTZ | NOT NULL, 이 테이블 Cursor |

**Staging: 2개**

- `stg_customer_subscriptions`: 구독 축 표준화, `subscription_attribute_hash` 계산
- `stg_customer_loyalty_tiers`: 등급 축 표준화, `tier_attribute_hash` 계산

**Intermediate: `int_customer_history`** (1개, 병합 지점)

두 Staging을 하나의 시간축으로 병합한다. 어느 한 축만 새 관측이 있으면 다른 축은 직전
값을 이어받는다. 이 단계가 Late Arrival 부분 결측을 흡수한다. 병합된 시점마다 두 축을
이어붙여 `attribute_hash`를 계산하고 Version 경계를 만든다.

병합 구현은 두 방식이 있다. 실제 SQL을 작성하며 정한다.

```text
1. union 시간축 + 축별 as-of join
   두 축의 updated_at을 union으로 모은 뒤, 각 시점에서 축별 유효 값을 찾는다.
   정확하다. 윈도우 함수를 축마다 한 번씩 쓴다.

2. full outer join + last_value 채우기
   customer_unique_id와 updated_at으로 full outer join한 뒤,
   null을 last_value(ignore nulls)로 채운다.
   간단하다. 정렬 전제가 명확해야 한다.
```

두 축의 관측 시각이 서로 다르므로 단순 `full outer join`만으로는 부족하다. 각 시점에서
다른 축의 그 시점 유효 값을 찾아야 하며 이는 사실상 축별 as-of join이다.

**Dimension: `dim_customer`** (1개)

컬럼 구성은 A안과 동일. 입력이 병합된 `int_customer_history`라는 점만 다르다.

**Fact FK 구성**

| Fact                         | FK                 | Temporal Join |
| ---------------------------- | ------------------ | ------------- |
| `fact_orders`                | `customer_key` 1개 | 1회           |
| `fact_subscription_payments` | `customer_key` 1개 | 1회           |

### 3.5.3 C안: 완전 분리

**Source: `customer_subscriptions`, `customer_loyalty_tiers`**

B안과 동일.

**Staging: 2개**

B안과 동일.

**Intermediate: 2개**

- `int_subscription_history`: 구독 축만 SCD2. `lag(subscription_attribute_hash)`로 경계.
- `int_tier_history`: 등급 축만 SCD2. `lag(tier_attribute_hash)`로 경계.

두 모델은 서로 조인하지 않는다. 병합 지점이 없다.

**Dimension: 2개**

`dim_customer_subscription`

| 컬럼                                     | 비고                            |
| ---------------------------------------- | ------------------------------- |
| `subscription_key`                       | Surrogate PK                    |
| `customer_id`                            | 사람 Business Key               |
| `subscription_status`                    |                                 |
| `trial_ends_at` 외 3개                   | 상태 시각                       |
| `rejoin_count`                           | `int_subscription_history` 파생 |
| `rejoined_at`                            |                                 |
| `subscription_attribute_hash`            |                                 |
| `valid_from` / `valid_to` / `is_current` |                                 |

`dim_customer_tier`

| 컬럼                                     | 비고              |
| ---------------------------------------- | ----------------- |
| `tier_key`                               | Surrogate PK      |
| `customer_id`                            | 사람 Business Key |
| `membership_tier`                        |                   |
| `tier_attribute_hash`                    |                   |
| `valid_from` / `valid_to` / `is_current` |                   |

**Fact FK 구성**

| Fact                         | FK                                  | Temporal Join |
| ---------------------------- | ----------------------------------- | ------------- |
| `fact_orders`                | `subscription_key` + `tier_key` 2개 | 2회           |
| `fact_subscription_payments` | `subscription_key` + `tier_key` 2개 | 2회           |

두 Dimension은 서로 조인하지 않는다. 교차 분석은 Fact에서 두 FK를 각각 조인해 얻는다.

### 3.5.4 세 안 공통: `fact_subscription_payments`

Grain은 구독 결제 1건. `payment_value`, `payment_status`, `billing_period_start`,
`billing_period_end`, `billing_sequence`를 가진다. FK만 3.5.1~3.5.3의 표대로 다르다.

## 4. 결정 항목 1: CHECK 제약 오염

### 4.1 문제

단일 테이블에서 단일 `updated_at`을 쓰면 등급 변경이 구독 제약을 깨뜨린다.

```text
2026-01-05  구독 NON_MEMBER→TRIAL   updated_at = 01-05
2026-02-01  등급 BRONZE→SILVER      updated_at = 02-01   -- 구독은 안 바뀜
```

02-01 행만 보면 `subscription_status = 'TRIAL'`이고 `updated_at = 02-01`이라 체험 시작이
02-01처럼 보인다. 실제는 01-05다.

시각 계약도 깨진다.

```text
CANCEL_REQUESTED:  benefit_ends_at > updated_at
CHURNED:           benefit_ends_at <= updated_at
```

`CANCEL_REQUESTED` 상태에서 등급만 바뀌면 `updated_at`이 앞으로 밀리는데
`benefit_ends_at`은 그대로다. 혜택 종료일을 지나면 상태가 정상인데도 CHECK 위반으로
INSERT가 거부된다.

### 4.2 안별 해결 방식

**A안** — 컬럼 2개와 CHECK 3개를 추가해 해결한다.

```sql
subscription_updated_at  TIMESTAMPTZ NOT NULL
tier_updated_at          TIMESTAMPTZ NOT NULL

CHECK (subscription_status <> 'CANCEL_REQUESTED' OR benefit_ends_at > subscription_updated_at)
CHECK (subscription_status <> 'CHURNED'          OR benefit_ends_at <= subscription_updated_at)
CHECK (updated_at = greatest(subscription_updated_at, tier_updated_at))
```

증분 Cursor는 `updated_at` 하나를 그대로 쓴다. `greatest()`이므로 어느 축이 바뀌든
전진한다.

**B안·C안** — 테이블이 나뉘므로 각 테이블의 `updated_at`이 곧 축별 변경 시각이다. 추가
컬럼도 CHECK도 없다. **구조적으로 해결된다.**

```sql
-- customer_subscriptions
CHECK (subscription_status <> 'CANCEL_REQUESTED' OR benefit_ends_at > updated_at)
CHECK (subscription_status <> 'CHURNED'          OR benefit_ends_at <= updated_at)
```

이 항목은 B안과 C안이 이긴다.

## 5. 결정 항목 2: SCD2 Version과 Temporal Join

### 5.1 Version 수는 A안과 B안이 같다

Dimension이 하나면 Version 수는 두 축 변경 시점의 합집합이다. Source를 나눠도 dbt에서
합치면 결과가 같다.

```text
2026-01-05  구독 NON_MEMBER→TRIAL
2026-02-01  등급 BRONZE→SILVER
2026-02-10  구독 TRIAL→ACTIVE

A안  dim_customer 3 Version
B안  dim_customer 3 Version  (두 Staging을 합친 뒤 동일)
C안  dim_customer_subscription 2 Version + dim_customer_tier 1 Version = 3행
```

세 안 모두 총 행 수가 같다. C안의 차이는 행당 컬럼 수가 좁다는 것뿐이며, 이 프로젝트
규모에서 실익은 무시할 만하다.

**"분리하면 불필요한 SCD2 Version이 준다"는 주장은 성립하지 않는다.**

### 5.2 변경 빈도 가정

> **아래 수치는 임의로 가정한 값이다.** Generator 시나리오 분포를 아직 정하지 않았고,
> 구독 이력은 Olist 원본에 없어 실측할 수 없다. 시나리오 목록도 전이 그래프가 허용하는
> 경로 중 일부를 골라 적은 것이며, 각 시나리오에 몇 퍼센트가 배정되는지는 정하지 않았다.
> 확정된 수치가 아니라 판단을 위한 가정이다.

12개월 기준으로 다음 시나리오를 가정한다. 횟수는 각 경로의 전이 개수를 센 값이다.

| 시나리오       | 전이                                                     | 횟수 |
| -------------- | -------------------------------------------------------- | ---- |
| 미가입 유지    | 없음                                                     | 0    |
| 체험 후 이탈   | `NON_MEMBER → TRIAL → CANCEL_REQUESTED → CHURNED`        | 3    |
| 체험 후 정착   | `NON_MEMBER → TRIAL → ACTIVE`                            | 2    |
| 결제 실패 회수 | `NON_MEMBER → TRIAL → ACTIVE → PAYMENT_FAILED → ACTIVE`  | 4    |
| 결제 실패 이탈 | `NON_MEMBER → TRIAL → ACTIVE → PAYMENT_FAILED → CHURNED` | 4    |
| 해지 후 재가입 | `… → CHURNED → TRIAL → ACTIVE`                           | 5~6  |

가장 긴 경로도 12개월에 6회 안쪽이라고 가정한다. `ACTIVE → ACTIVE` 자동결제는 상태가 안
바뀌므로 Version을 만들지 않는다. `next_billing_at`을 Hash에서 제외했기 때문이다.

등급은 경계가 5건과 15건 두 개뿐이므로 평생 최대 2회 바뀐다. 이 값은 등급 규칙에서
직접 나오므로 가정이 아니다.

이 문서는 **두 축의 전이 횟수가 작아 어느 안을 골라도 SCD2 Version 폭증이 없다고
가정하고** 나머지 항목으로 판단한다. 평균값이나 축 간 비율은 결론에 쓰지 않는다.
분포를 확정한 뒤 전이 횟수가 이 가정보다 훨씬 크게 나오면 이 절만 다시 본다.

### 5.3 Temporal Join 비용

현재 Temporal Join은 `int_orders_enriched.sql`에 있다.

```sql
left join {{ ref('dim_customer') }}
    on stg_orders.customer_id = dim_customer.customer_id
    and stg_orders.purchase_at >= dim_customer.valid_from
    and stg_orders.purchase_at < coalesce(dim_customer.valid_to, timestamptz 'infinity')
```

**A안·B안** — 위 형태 그대로 Fact마다 1회다.

**C안** — 같은 형태를 Fact마다 2회 쓴다.

```sql
left join {{ ref('dim_customer_subscription') }} as sub
    on stg_orders.customer_id = sub.customer_id
    and stg_orders.purchase_at >= sub.valid_from
    and stg_orders.purchase_at < coalesce(sub.valid_to, timestamptz 'infinity')
left join {{ ref('dim_customer_tier') }} as tier
    on stg_orders.customer_id = tier.customer_id
    and stg_orders.purchase_at >= tier.valid_from
    and stg_orders.purchase_at < coalesce(tier.valid_to, timestamptz 'infinity')
```

Temporal Join은 범위 조건이라 동등 조인보다 비싸다. Fact가 `fact_orders`와
`fact_subscription_payments` 둘이므로 C안은 총 4회를 수행한다.

### 5.4 대표 분석 질문

**Q. 주문 시점 구독 상태 × 등급별 평균 주문 금액**

A안·B안은 Dimension이 하나라 동일하다.

```sql
select
    dim_customer.subscription_status,
    dim_customer.membership_tier,
    avg(fact_orders.payment_value) as aov
from fact_orders
join dim_customer using (customer_key)
group by 1, 2;
```

C안은 조인이 2회다.

```sql
select
    s.subscription_status,
    t.membership_tier,
    avg(fact_orders.payment_value) as aov
from fact_orders
join dim_customer_subscription as s on fact_orders.subscription_key = s.subscription_key
join dim_customer_tier         as t on fact_orders.tier_key         = t.tier_key
group by 1, 2;
```

단일 축을 묻는 질문(구독 상태별 매출, 등급별 매출)은 세 안이 비슷하며, C안이 좁은
테이블을 읽어 소폭 유리하다.

## 6. 결정 항목 3: Late Arrival 부분 결측

Phase 5G에서 Late Arrival 경계를 이미 구현했다. 이 항목이 C안의 가장 큰 비용이다.

### 6.1 부분 결측이 생기는 이유

Source를 나누면 두 테이블의 증분 수집 Watermark가 독립적이다. 한 배치에서
`customer_subscriptions`는 새 데이터를 가져왔는데 `customer_loyalty_tiers`는 아직 안
가져온 상태가 정상적으로 존재한다.

### 6.2 안별 처리 지점

**A안** — Source가 하나이므로 부분 결측 자체가 없다. 현행 코드가 그대로 동작한다.

**B안** — 부분 결측이 생기지만 **`int_customer_history`에서 한 번에 흡수한다**. Fact보다
앞단이므로 뒤로 번지지 않는다.

```sql
-- int_customer_history에서 두 Staging을 합칠 때
full outer join ... using (customer_unique_id)
-- 없는 쪽은 직전 Version 값을 이어받는다
```

처리 지점이 한 곳이고, 그 지점이 이미 SCD2 Version을 만드는 모델이라 자연스럽다.

**C안** — Fact마다 조합이 넷으로 늘어난다.

| 구독 Version | 등급 Version | 처리                   |
| ------------ | ------------ | ---------------------- |
| 있음         | 있음         | 정상                   |
| 있음         | 없음         | **새 규칙 필요**       |
| 없음         | 있음         | **새 규칙 필요**       |
| 없음         | 없음         | 기존 Late Arrival 규칙 |

부분 결측 시 선택지는 셋이며, 무엇을 고르든 문서화와 테스트가 필요하다.

```text
1. 주문 자체를 Fact에서 보류        -- 매출 집계가 늦어짐
2. 있는 쪽만 채우고 없는 쪽은 null  -- Fact에 불완전 행이 생김
3. 없는 쪽은 기본값으로 채움        -- 잘못된 등급으로 집계될 위험
```

이 결정을 `fact_orders`와 `fact_subscription_payments` **두 곳에서** 각각 해야 한다.

## 7. 요약

| 항목                     | A안 (통합)               | B안 (Source만 분리)        | C안 (완전 분리)             |
| ------------------------ | ------------------------ | -------------------------- | --------------------------- |
| Source Table             | 8개                      | 9개                        | 9개                         |
| Watermark                | 8개                      | 9개                        | 9개                         |
| Bronze 경로              | 8개                      | 9개                        | 9개                         |
| dbt Staging 모델         | 1개                      | 2개                        | 2개                         |
| dbt Dimension 모델       | 1개                      | 1개                        | 2개                         |
| SCD2 총 행 수            | n+m                      | n+m (동일)                 | n+m (동일)                  |
| CHECK 제약 오염          | 컬럼 2개 + CHECK 3개     | **구조적 해결**            | **구조적 해결**             |
| Temporal Join (Fact 2개) | 2회                      | 2회                        | 4회                         |
| 교차 분석 SQL            | 조인 1회                 | 조인 1회                   | 조인 2회                    |
| Late Arrival 부분 결측   | 발생 안 함               | `int_customer_history` 1곳 | **Fact 2곳, 규칙 3종 선택** |
| Generator 코드           | 한 함수가 두 축 UPDATE   | 함수 2개로 분리            | 함수 2개로 분리             |
| 테이블 의미              | 구독 8 : 등급 2로 기울음 | 선명                       | 선명                        |

## 8. 판단 근거

**C안의 주 논거가 성립하지 않는다.** Dimension을 나눠도 SCD2 총 행 수는 같다. Version이
줄지 않는데 Temporal Join 4회와 Late Arrival 부분 결측 규칙을 Fact 두 곳에서 대신 낸다.
얻는 것은 행당 컬럼 수가 좁아지는 것뿐이며 이 규모에서 무의미하다.

**B안의 탈락 근거가 사라졌다.** 이전 비교에서 B안을 "Version이 줄지 않는데 Bronze 표면만
늘어난다"는 이유로 제외했다. 그때는 CHECK 제약 오염 문제를 몰랐다. 지금은 B안이 그 문제를
공짜로 해결한다는 사실이 추가됐다. A안은 같은 결과를 컬럼 2개와 CHECK 3개로 산다.

**결제 테이블이 B안의 비용을 낮췄다.** 통합안 기준선이 이미 8개로 올랐고, FK 정합성도
개선된다. `subscription_payments.customer_unique_id`가 등급 테이블을 참조하는 것보다
`customer_subscriptions`를 참조하는 편이 의미에 맞다.

**B안의 Late Arrival 부담은 C안보다 훨씬 작다.** 부분 결측이 생기는 것은 같지만 처리
지점이 `int_customer_history` 한 곳이고, Fact까지 번지지 않는다. C안은 Fact 두 곳에서
각각 규칙을 정해야 한다.

**A안의 유일한 우위는 Source 표면 1개다.** 대신 컬럼 2개, CHECK 3개, 그리고 테이블 의미가
기울어지는 비용을 낸다.

## 9. 결정란

- [ ] A안 통합 — `customer_memberships` 한 테이블에 축별 `updated_at`을 둔다
- [x] **B안 Source만 분리** — `customer_subscriptions` + `customer_loyalty_tiers`, `dim_customer` 하나
- [ ] C안 완전 분리 — Source와 Dimension을 모두 나눈다

### 9.1 B안 선택 근거

**CHECK 제약 오염이 구조적으로 해결된다.** A안은 컬럼 2개와 CHECK 3개로 같은 결과를 산다.
특히 `updated_at = greatest(subscription_updated_at, tier_updated_at)` 제약은 Generator가
두 축을 갱신할 때마다 정합성을 맞춰야 하는 지속 부담이다. B안은 각 로직이 자기 테이블만
쓴다.

**Temporal Join이 1회로 유지된다.** C안은 Fact 두 개에서 각각 2회씩 총 4회다. 교차 분석
SQL도 B안은 조인 1회다.

**Late Arrival 처리 지점이 한 곳이다.** `int_customer_history`의 병합 단계에서 흡수하고
Fact까지 번지지 않는다. C안은 `fact_orders`와 `fact_subscription_payments` 두 곳에서 각각
부분 결측 규칙을 정하고 테스트해야 한다.

**Source 표면 증가 비용이 낮아졌다.** `subscription_payments` 확정으로 통합안도 8개가 됐다.
B안 9개와의 차이는 1개뿐이다.

### 9.2 B안이 지는 비용

**병합 로직이 이 안의 유일한 새 복잡도다.** 3.5.2절의 "직전 값을 이어받는다"는 단순
`full outer join`보다 무겁다. 두 축의 관측 시각이 서로 다르므로 각 시점에서 다른 축의
그 시점 유효 값을 찾아야 한다. 사실상 축별 as-of join이다.

**Staging 모델과 Source 표면이 1개씩 는다.** Watermark, Bronze 경로, Arrow Schema, Catalog
검증 대상이 각각 9개가 된다.

결정 후 [전환 계획](subscription-membership-transition-plan.md) 3절 Source 모델, 4절 SCD2
속성, 6절 제약과 [구독 생명주기 요구사항](subscription-lifecycle-requirements.md) 3절
Source 모델을 선택한 안에 맞춰 갱신한다.
