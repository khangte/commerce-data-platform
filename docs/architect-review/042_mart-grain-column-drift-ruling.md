# 042. Mart Grain 계약 컬럼 불일치 판정과 Phase 6·7 되돌림

- 일자: 2026-09-22
- 대상: `docs/reference/mart-grain.md` vs `dbt/models/marts/`, `docs/phases/phase-06-dimensional-modeling.md`, `docs/phases/phase-07-data-quality-publish.md`
- 발견 경위: Phase 10 BI 실행계획 수립 중 `P10-07`·`P10-08` Metric 등록 대상 컬럼 확인
- 판정: **계약 문서를 구현에 맞춘다. 정합 작업은 Phase 6, 상태 정리는 Phase 7에서 처리한다 (lead 결정 2026-09-22)**
- 선행 판정: [002. Mart Grain 계약과 구현의 명명 불일치 (보류)](002_mart-grain-contract-drift.md) — 이 문서로 종결한다.

## 1. 결정

[002](002_mart-grain-contract-drift.md) §4의 두 갈래 중 **1번(계약을 구현에 맞춘다)** 을 택한다.

근거는 세 가지다.

- 구현은 Model·Test·통합 Test·Benchmark Hash 정렬 Key까지 현재 컬럼 이름에 의존한다. 구현을 문서에 맞추면 `src/warehouse/mart_hash.py`의 정렬 Key와 Phase 9가 고정한 Mart Result Hash가 함께 바뀐다. Phase 9는 `Done`이고 그 증거는 Hash 동일성 위에 서 있다.
- `dim_product`·`dim_seller`는 Type 1 Dimension이다. Business Key를 PK로 두는 것이 타당하다는 논거가 이미 [002](002_mart-grain-contract-drift.md) §4에 있고 [009](009_type1-dimension-pk-and-tests.md)가 PK Test를 그 방향으로 채웠다.
- Surrogate Key 명명 일관성은 얻는 것이 문서 미학뿐이고 잃는 것이 Fact FK·Join 경로·Hash다.

따라서 Surrogate Key 명명 정책은 다시 열지 않는다. 계약 문서가 구현을 따라간다.

## 2. 불일치 목록 (2026-09-22 실측)

`describe select * from <schema>.<table>`과 `dbt/models/marts/**/*.sql`로 확인했다.

| 항목 | 계약 문서 | 구현 | 조치 |
| ---- | --------- | ---- | ---- |
| `fct_order` 금액 Measure | 없음 | `gross_order_value`, `payment_total` | 문서에 추가. PRD §14.3의 의미 분리를 함께 적는다 |
| `fct_order` 배송 Measure | `carrier_handoff_at`·`estimated_delivery_at`·`delivered_at`·`purchased_at` Timestamp | `carrier_handoff_days`, `delivery_days`, `delivery_delay_days`, `is_late` | 문서를 구현으로 교체. 4개 모두 Non-additive로 표기 |
| `fct_order` 고객 추적 | `source_customer_id` | 없음 (`dim_customer.customer_id` Join) | 문서에서 제거하고 Entity 단위 집계 경로를 명시 |
| `fct_order_item` 판매가 | `price` | `item_price` | 문서를 `item_price`로 |
| `fct_order_payment` Grain Key | `payment_sequential` | `payment_sequence` | 문서를 `payment_sequence`로. `src/warehouse/mart_hash.py:44`도 `payment_sequence`를 쓴다 |
| §4 Report | 템플릿 표가 비어 있다 | `rpt_subscription_funnel_daily`, `rpt_subscription_payment_outcomes_daily`, `rpt_membership_tier_performance` 존재 | 3개 Model의 Grain 문장·Unique Key·컬럼표·알려진 제약을 채운다 |

## 3. 왜 Phase 6으로 되돌리는가

Phase 6 DoD 2번 항목이 `[x]`다.

```
- [x] [Mart Grain 계약](../reference/mart-grain.md)이 확정되고 구현이 그 계약을 따른다.
```

§2의 6개 항목이 남아 있는 동안 이 항목은 참이 아니다. Phase 6의 산출물은 "Grain 계약 확정"이고, 계약 문서는 Phase 6이 만든 정본이다. Phase 10에서 고치면 Phase 6 DoD는 거짓인 채로 닫힌 기록이 된다. 마감 문서가 미달 항목을 숨기지 않는 원칙([041](041_phase9-task17-closeout-review.md) §1)을 Phase 6에도 같게 적용한다.

`docs/phases/phase-07-data-quality-publish.md:3`의 `상태: In Progress`는 Phase 7 자신의 마감 누락이다. 미체크 항목이 0건이고 DoD 6항목 전부 증거를 갖는다. Phase 7에서 상태 줄만 닫는다.

## 4. 되돌림 범위

Phase 6에 Task 2개를 추가하고 Phase 6 상태를 `In Progress`로 되돌린다. 기존 `P6-01`~`P6-25`의 체크와 내용은 건드리지 않는다.

| Task | 내용 |
| ---- | ---- |
| `P6-26` | §2 상단 5개 항목 정합 — 계약 §2·§3 컬럼표를 구현과 일치시키고 컬럼 종류·Additive 구분을 유지한다 |
| `P6-27` | 계약 §4 Report 계약 작성 — 기존 `rpt_*` 3개의 Grain 문장·Unique Key·컬럼표·§4.3 제약 |

Phase 7은 상태 줄 1행과 `metrics` View 결함의 소재 기록만 다룬다. 결함 자체의 수정(`marts.metrics`의 `table` 실체화)은 lead 승인에 따라 Phase 10 Task 1에서 한다.

## 5. Phase 10과의 경계

- Phase 10은 새로 추가하는 `rpt_customer_order_activity_daily`의 계약 §4 등록만 한다. 기존 3개 Model의 §4 작성은 `P6-27`이다.
- Phase 10 Batch 3(Semantic·Dashboard)은 `P6-26`·`P6-27` 완료를 선행 조건으로 갖는다. Metric 등록이 컬럼 이름을 직접 쓰기 때문이다.
- Phase 10 Batch 1·2(Serving Export, Connection Gate)는 컬럼 이름에 의존하지 않으므로 병행 가능하다.
- `docs/phases/phase-10-bi.md`의 DoD `P9-*` 오타와 `.env.example`의 `# BI (Phase 9)` 주석은 Phase 10 자기 문서이므로 Phase 10 마감 Task에서 고친다.

## 6. 범위 밖

- Surrogate Key 명명 정책 재설계 (§1에서 종결)
- Fact·Dimension의 Grain·Unique Key 변경
- 구현 컬럼 이름 변경
- ADR-001~015 중 Phase 10과 무관한 누락 ADR 작성
