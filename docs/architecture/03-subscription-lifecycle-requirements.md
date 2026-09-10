# 구독 생명주기 요구사항

> 상태: Confirmed
> 작성일: 2026-09-10
> 관련 문서:
> [구독 상태와 멤버십 등급 전환 계획](subscription-membership-transition-plan.md),
> [구독·등급 테이블 통합과 분리 비교](membership-table-split-comparison.md),
> [Membership Grain 분리 기획안](membership-grain-separation.md)

## 1. 확정 요구사항

| 항목             | 결정                                                       |
| ---------------- | ---------------------------------------------------------- |
| 구독 주기        | 1개월 고정                                                 |
| 자동 갱신        | `ACTIVE` 유지 시 `next_billing_at` 도달마다 자동결제       |
| 해지             | 신청 시 `benefit_ends_at`까지 혜택 유지, 경과 후 자동 종료 |
| 체험 종료        | `trial_ends_at` 도달 시 자동 결제 시도, 성공하면 `ACTIVE`  |
| 결제 실패 유예   | 7일. `benefit_ends_at = payment_failed_at + 7일`           |
| 결제 이벤트 저장 | `subscription_payments` Source Table 신설                  |

## 2. 결제는 Dimension이 아니라 Fact다

자동결제를 요구사항에 넣으면 결제가 실제 이벤트가 된다. 이것을
`customer_memberships`의 `next_billing_at` 변화로만 표현하면 두 가지 중 하나가 깨진다.

```text
next_billing_at을 SCD2 Hash에 포함   →  ACTIVE 12개월 고객이 Version 12개.
                                        상태는 12번 다 ACTIVE. 무의미한 폭증.
next_billing_at을 SCD2 Hash에서 제외 →  12번의 결제가 Warehouse에서 사라짐.
                                        구독 매출 집계 불가.
```

결제는 상태가 아니라 사건이므로 Dimension이 아니라 Fact에 속한다. 별도 Source Table로
분리하면 둘 다 해결된다. `next_billing_at`은 Hash에서 제외한 채로 두고, 결제 건수와
금액은 `fact_subscription_payments`에서 집계한다.

기존 `order_payments`는 주문 결제이므로 구독 결제와 성격이 다르다. 같은 테이블에 섞지
않는다.

## 3. Source 모델

[테이블 분리 비교](membership-table-split-comparison.md)에서 **B안(Source만 분리)**을
선택했다. 구독과 등급을 별도 Source Table로 두고 Warehouse에서 하나의 `dim_customer`로
합친다. 기존 `customer_memberships`는 두 테이블로 대체된다.

### 3.1 customer_subscriptions

구독 상태의 **현재 값**만 보관한다. 결제 이력은 여기 없다.

```text
customer_subscriptions                   -- 사람(customer_unique_id)당 1행
├── customer_unique_id       PK
├── subscription_status      NOT NULL DEFAULT 'NON_MEMBER'
├── trial_ends_at            NULL
├── benefit_ends_at          NULL
├── next_billing_at          NULL
├── payment_failed_at        NULL
├── cancel_requested_at      NULL
├── created_at               NOT NULL
└── updated_at               NOT NULL    -- 증분 Cursor
```

이 테이블의 `updated_at`이 곧 구독 축 변경 시각이므로 4.2절 시각 계약을 그대로 CHECK
제약으로 쓴다. 등급 변경이 이 값을 밀지 않는다.

### 3.2 customer_loyalty_tiers

거래 실적 등급의 현재 값만 보관한다.

```text
customer_loyalty_tiers                   -- 사람(customer_unique_id)당 1행
├── customer_unique_id       PK
├── membership_tier          NOT NULL DEFAULT 'BRONZE'
├── created_at               NOT NULL
└── updated_at               NOT NULL    -- 증분 Cursor
```

등급 계산 규칙은 기존 `membership_level`과 같다. 완료(`delivered`) 주문 수가 5건이면
`SILVER`, 15건이면 `GOLD`다.

### 3.3 subscription_payments

구독 결제 1건이 1행이다. 사람당 여러 행이 쌓이는 Append 성격이다.

```text
subscription_payments
├── customer_unique_id       NOT NULL, FK → customer_subscriptions
├── billing_sequence         NOT NULL    -- 사람별 결제 순번, 1부터
├── payment_status           NOT NULL    -- completed | failed
├── payment_value            NOT NULL    -- 월 구독료
├── billing_period_start     NOT NULL
├── billing_period_end       NOT NULL    -- start + 1개월
├── created_at               NOT NULL
├── updated_at               NOT NULL
└── PK (customer_unique_id, billing_sequence)
```

`order_payments`의 `(order_id, payment_sequential)` 복합 PK 패턴을 따른다. 증분 Cursor는
`updated_at`이며 Index는 `(updated_at, customer_unique_id, billing_sequence)`다.

`payment_status` 도메인을 `completed`와 `failed` 둘로 좁힌다. 구독 결제는 즉시 확정이므로
`pending`이 없고, 환불은 이번 범위 밖이다.

Source Table은 7개에서 9개가 된다. `customer_memberships` 하나가
`customer_subscriptions`와 `customer_loyalty_tiers` 둘로 나뉘고 `subscription_payments`가
새로 생긴다.

## 4. 상태 전이 규칙

### 4.1 전이 그래프

```text
NON_MEMBER → TRIAL                              가입, 체험 시작
NON_MEMBER → ACTIVE                             체험 없이 즉시 유료 가입

TRIAL → ACTIVE                                  trial_ends_at 도달, 결제 성공
TRIAL → PAYMENT_FAILED                          trial_ends_at 도달, 결제 실패
TRIAL → CANCEL_REQUESTED                        체험 중 해지 신청

ACTIVE → ACTIVE                                 next_billing_at 도달, 결제 성공 (상태 불변)
ACTIVE → PAYMENT_FAILED                         next_billing_at 도달, 결제 실패
ACTIVE → CANCEL_REQUESTED                       해지 신청

PAYMENT_FAILED → ACTIVE                         유예 중 재결제 성공
PAYMENT_FAILED → CANCEL_REQUESTED               유예 중 해지 신청
PAYMENT_FAILED → CHURNED                        benefit_ends_at 경과, 유예 소진

CANCEL_REQUESTED → CHURNED                      benefit_ends_at 경과, 자동 종료
CANCEL_REQUESTED → ACTIVE                       혜택 종료 전 해지 철회

CHURNED → TRIAL                                 재가입, 체험 재제공
CHURNED → ACTIVE                                재가입, 즉시 유료
```

`ACTIVE → ACTIVE`는 상태가 안 바뀌므로 SCD2 Version을 만들지 않는다.
`subscription_payments`에 행 하나가 추가되고 `next_billing_at`만 1개월 뒤로 밀린다.
`next_billing_at`을 Hash에서 제외했으므로 `dim_customer`는 그대로다.

`CHURNED`는 종착 상태가 아니라 재가입 대기 상태다. 어떤 상태도 `NON_MEMBER`로 돌아가지
않는다. `NON_MEMBER`는 사람마다 최초 한 번만 존재한다.

### 4.2 시각 필드 계약

| 상태               | 필수 시각                                | 값 규칙                                     |
| ------------------ | ---------------------------------------- | ------------------------------------------- |
| `NON_MEMBER`       | 없음                                     | 시각 필드 전부 NULL                         |
| `TRIAL`            | `trial_ends_at`                          | 가입 시각 + 체험 기간                       |
| `ACTIVE`           | `next_billing_at`                        | 직전 결제일 + 1개월                         |
| `PAYMENT_FAILED`   | `payment_failed_at`, `benefit_ends_at`   | `benefit_ends_at = payment_failed_at + 7일` |
| `CANCEL_REQUESTED` | `cancel_requested_at`, `benefit_ends_at` | `benefit_ends_at` = 남은 구독 기간의 끝     |
| `CHURNED`          | `benefit_ends_at`                        | 종료 시각                                   |

## 5. Generator 만료 스캔

Source는 현재 상태 1행만 저장하므로 아무도 UPDATE하지 않으면 `benefit_ends_at`이 지나도
영원히 `CANCEL_REQUESTED`로 남는다. 자동 갱신과 자동 종료를 실현하려면 Generator가 매
실행마다 시각 기반 스캔을 돌려야 한다.

무작위 시나리오 선택이 아니라 **시각 기반 결정적 스캔**이다. 같은 `logical_date`로
재실행하면 같은 결과가 나오므로 재현성 계약을 지킨다.

### 5.1 스캔 순서

매 `logical_date` 실행 시 아래 순서로 처리한다. 순서가 바뀌면 결과가 달라지므로 고정한다.

```text
1. 만료 종료   benefit_ends_at <= logical_date
               AND subscription_status IN ('CANCEL_REQUESTED', 'PAYMENT_FAILED')
               → CHURNED

2. 체험 종료   trial_ends_at <= logical_date AND subscription_status = 'TRIAL'
               → 결제 시도. 성공하면 ACTIVE, 실패하면 PAYMENT_FAILED

3. 정기 결제   next_billing_at <= logical_date AND subscription_status = 'ACTIVE'
               → 결제 시도. 성공하면 ACTIVE 유지, 실패하면 PAYMENT_FAILED

4. 재결제      subscription_status = 'PAYMENT_FAILED' AND benefit_ends_at > logical_date
               → 결정적 재시도. 성공하면 ACTIVE
```

1번이 2·3번보다 먼저다. 이미 만료된 사람에게 결제를 시도하지 않기 위해서다.

### 5.2 결제 성공·실패 판정

무작위가 아니라 기존 `logical_hash` 패턴을 재사용한다.

```text
logical_hash({
    generator_inputs: config.deterministic_inputs(),
    entity: "subscription-billing",
    customer_unique_id: ...,
    billing_sequence: ...,
})
```

해시 값의 하위 구간으로 실패율을 결정한다. 실패율은 12개월 시뮬레이션에서 회수와 이탈이
모두 관측되도록 정한다.

### 5.3 결제 1건이 만드는 변경

결제가 성공하면 두 테이블이 함께 바뀐다.

```text
subscription_payments    행 1개 INSERT (billing_sequence + 1)
customer_subscriptions   next_billing_at = billing_period_end
                         updated_at 갱신
                         subscription_status는 ACTIVE 유지 (변경 없음)
```

`customer_loyalty_tiers`는 건드리지 않는다. 결제와 등급은 변경 원인이 다르다.

`subscription_status`가 안 바뀌므로 SCD2 속성 Hash도 안 바뀐다. `dim_customer`에 Version이
생기지 않는다. 이것이 결제를 Fact로 분리한 이유다.

## 6. Warehouse 모델

### 6.0 두 Source의 병합

B안이므로 Source는 둘이지만 Dimension은 하나다. 병합은 `int_customer_history`에서 한다.

```text
stg_customer_subscriptions ─┐
                            ├─→ int_customer_history ─→ dim_customer
stg_customer_loyalty_tiers ─┘
```

두 축의 관측 시각이 서로 다르므로 각 시점에서 다른 축의 그 시점 유효 값을 이어받는다.
두 Source의 증분 Watermark가 독립적이라 한쪽만 새 데이터가 도착하는 경우가 정상적으로
발생하며, 이 단계가 그것을 흡수한다. Fact까지 번지지 않는다.

구현 방식은 비교 문서 3.5.2절에 두 가지를 적었고 실제 SQL 작성 시 정한다.

### 6.1 SCD2 속성 Hash

```text
subscription_status
membership_tier
trial_ends_at
benefit_ends_at
payment_failed_at
cancel_requested_at
```

`next_billing_at`은 제외한다. 매월 갱신되지만 상태 변화가 아니다. 현재 결제 예정일은
`is_current = true` Version에서 읽는다.

### 6.2 fact_subscription_payments

```text
fact_subscription_payments
├── customer_key            FK → dim_customer, 결제 시점 Temporal Join
├── customer_unique_id
├── billing_sequence
├── payment_status
├── payment_value
├── billing_period_start
└── billing_period_end
```

Grain은 결제 1건이다. 결제 시각으로 `dim_customer`와 Temporal Join하므로 결제 당시의 구독
상태와 등급을 함께 분석할 수 있다.

`fact_payments`는 주문 결제이므로 이 Fact와 별개다. 이름이 비슷하니 dbt Model 설명에
구분을 명시한다.

### 6.3 가능해지는 분석

| 지표             | 계산                                                    |
| ---------------- | ------------------------------------------------------- |
| 구독 MRR         | `fact_subscription_payments`의 월별 `completed` 합계    |
| 결제 실패율      | `failed` / 전체 결제 건수                               |
| 결제 실패 회수율 | `PAYMENT_FAILED` 진입 후 7일 내 `completed`가 있는 비율 |
| 체험 전환율      | `TRIAL` 진입자 중 `billing_sequence = 1`이 성공한 비율  |
| 재가입율         | `int_customer_history`의 `CHURNED → TRIAL/ACTIVE` 전이  |
| 구독 상태 × 등급 | `dim_customer`의 `is_current` 교차 분포                 |

## 7. 앞선 결정에 미치는 영향

### 7.1 테이블 분리 결정

[비교 문서](membership-table-split-comparison.md)에서 **B안(Source만 분리)**으로 확정했다.
결제 테이블 추가가 이 결정에 두 가지로 작용했다.

**Source 표면 증가 비용을 낮췄다.** 결제 테이블로 통합안(A안)도 8개가 됐으므로, 분리안의
9개와 차이가 1개뿐이다. 결제 테이블이 없을 때의 7개 대 8개 대비와 같은 폭이다.

**C안의 비용을 키웠다.** `fact_subscription_payments`도 결제 시점으로 Temporal Join을
한다. C안이면 이 Fact도 FK 2개를 갖고 Temporal Join을 2회 하므로 총 4회가 되고, Late
Arrival 부분 결측 규칙을 Fact 두 곳에서 각각 정해야 한다.

B안은 Temporal Join 1회를 유지하면서 CHECK 제약 오염을 구조적으로 해결한다.

### 7.2 전환 계획에 반영할 항목

[전환 계획](subscription-membership-transition-plan.md)에 아래를 추가한다.

| 절  | 추가·수정 내용                                                                  |
| --- | ------------------------------------------------------------------------------- |
| 2.1 | 전이 그래프에 `ACTIVE → ACTIVE`, `CANCEL_REQUESTED → ACTIVE` 추가               |
| 3   | `customer_memberships`를 `customer_subscriptions` + `customer_loyalty_tiers`로 분리 |
| 3   | `subscription_payments` Source Table 신설                                       |
| 4   | `int_customer_history` 병합 단계, `fact_subscription_payments` 추가             |
| 5   | 구현 순서에 테이블 분리, 결제 테이블, Generator 만료 스캔 반영                  |
| 6   | 유예 7일, 시각 필드 계약, 결제 도메인 제약 추가                                 |

## 8. 미결정 사항

아래는 구현 착수 전에 정한다.

- 체험 기간 길이. 실무 관행은 7일 또는 30일이다.
- 월 구독료 금액. 단일 가격이면 상수, 향후 요금제 확장은 범위 밖이다.
- 결제 실패율. 12개월 시뮬레이션에서 회수와 이탈이 모두 관측되도록 역산한다.
- 신규 가입률과 해지 신청률. Generator 시나리오 분포를 정할 때 함께 결정한다.
