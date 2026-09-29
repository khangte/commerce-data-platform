# 052. Generator 신규 주문이 CREATED에 머무는 현상

- 일자: 2026-09-29
- 대상: `src/generator/service.py` `run_generator`, `src/generator/transitions.py`, `airflow/dags/source_simulation_dag.py`
- 요청: lead 분석 요청(구현 지시 없음, 판정만)
- 판정: **설계·구현 누락(Design Gap).** 의도된 동작이 아니다. 권고안은 B(정기 실행에 기존 주문 전이 단계 추가)다. 구현 여부는 사용자가 정한다.

## 측정(읽기 전용, 2026-09-29 09:30 UTC 기준)

Generator 실행 기록(`pipeline_metadata.generator_runs`)은 모두 `SUCCESS`다. 2026-09-04부터 47회 실행했다. `default` 41회는 신규 주문 50건을 만들었다. 구독 Profile 6회는 60건을 만들었다. 2026-09-29 00:00부터는 `SOURCE_DAG_SCHEDULE='*/10 * * * *'`로 10분마다 1건씩 만든다.

| 위치 | 범위 | 상태 분포 | 전이된 행 |
| --- | --- | --- | --- |
| Source `orders` | `created_at >= 2026-09-01`(Generator 생성분) | `created` 110 | `updated_at > created_at` 0건 |
| Source `order_payments` | 위 주문의 결제 | `pending` 110 | 0건 |
| Mart `facts.fct_order` | `purchase_date_key >= 20260901` | `CREATED` 110 | 해당 없음 |

Source와 Mart가 일치한다. 수집·변환 파이프라인은 들어온 값을 그대로 반영했다. 원인은 Generator가 기존 주문을 한 번도 바꾸지 않는 데 있다.

Seed(Olist) 주문 99,441건은 `delivered` 96,478, `shipped` 1,107, `canceled` 625, `unavailable` 609, `invoiced` 314, `processing` 301, `created` 5, `approved` 2다. Seed의 `created` 5건은 원천 데이터의 값이다. 이번 현상과 관계없다.

## 원인

1. PRD §7.1과 Phase 2 §4는 Order `created → approved → shipped → delivered`(`created`·`approved → canceled`)와 Payment `pending → completed → refunded`·`pending → failed`를 "허용 전이만 생성한다"로 정의한다. PRD §13.3의 Task 이름도 `generate_transactional_changes`다.
2. `transitions.py`는 전이 계획과 적용(`plan_order_transition`, `apply_order_transition`, `persist_order_transition`, Payment 대응 함수)을 구현했다. 단위 테스트와 통합 테스트도 있다.
3. 그러나 `run_generator`는 신규 Order Bundle 생성, 구독 만료 스캔, 구독·멤버십 Profile만 실행한다. `src/`, `airflow/`, `scripts/`에서 주문 전이 함수를 호출하는 곳은 `scenarios.py`의 Fixture(`late_order_update_transition`, `delayed_payment_transition`)뿐이다. `delayed-payment`는 `EXECUTABLE_ANOMALY_PROFILES`에서도 빠져 있다.
4. Phase 2 체크리스트의 "상태 변화" 항목은 모두 `[x]`다. 그러나 이 항목들은 전이 함수의 규칙(허용 전이, 기대 Version, `updated_at` 단조 증가)을 검증한 것이다. 정기 실행이 전이를 만든다는 항목은 없다. 구독 계약은 만료 스캔을 매 실행에 연결했지만, 주문에는 같은 연결이 없다. 이 차이를 의도했다는 기록(PRD, Phase 문서, ADR)은 찾지 못했다.

따라서 규칙은 명세되고 부품은 구현됐으나 실행 경로에 연결되지 않았다. 버그보다 설계 누락에 가깝다. 어느 문서도 "누가, 언제, 어떤 주문을 전이하는가"를 정하지 않았기 때문이다.

## 영향

- `metrics.rpt_customer_order_activity_daily`와 `metrics.rpt_membership_tier_performance`는 `DELIVERED`만 매출과 배송 완료 건수로 센다. 따라서 Generator 주문은 이 지표에 영원히 0으로 들어간다. Dashboard에서 "주문은 늘어나는데 매출은 그대로"로 보인다.
- Mutable Row의 증분 경로(기존 Row의 `updated_at` 전진, Staging 최신 상태 선택, `int_affected_order_keys` 재계산, `fct_order` `delete+insert`)는 정기 실행에서 주문 축으로 한 번도 실행되지 않는다. 이 경로는 테스트로만 검증돼 있다.
- PRD §7.4 "과거 Event의 늦은 Update"도 정기 실행에서 발생하지 않는다.

## 대안

| 대안 | 판정 | 근거 |
| --- | --- | --- |
| A. 의도된 동작으로 보고 Dashboard 설명만 보완 | 기각 | §7.1이 전이를 Generator 책임으로 정했다. 설명을 달아도 `DELIVERED` 기준 지표에 Generator 주문이 영구히 빠진다. |
| B. `run_generator`에 기존 주문 전이 단계 추가 | **권고** | 명세된 전이를 정기 실행에 연결한다. 기존 부품과 Guard를 그대로 쓴다. 다음 절 참조. |
| C. 전이 전용 Profile(예: `order-progress`)과 별도 Schedule | 기각 | 전이는 이상 상황이 아니라 정상 OLTP 동작이다. 한 실행은 한 Profile만 받으므로 두 번째 Schedule과 Lease 경합이 생긴다. 얻는 것이 없다. |
| D. 신규 주문을 처음부터 최종 상태로 삽입 | 기각 | 기존 Row 변경이 없어 증분 Mutable 경로를 전혀 검증하지 못한다. 상태 시각도 `updated_at`과 분리되지 않는다. |

## 권고안 B의 설계 제약

구현을 결정하면 아래 규칙을 지킨다.

1. **대상**: Seed 기준선 이후 Generator가 만든 주문 중 최종 상태(`delivered`, `canceled`)가 아닌 주문만 대상이다. Seed 주문(`shipped` 1,107건 등)은 과거 원천 데이터이므로 건드리지 않는다. `invoiced`, `processing`, `unavailable`은 전이표에 없으므로 대상 규칙과 관계없이 제외된다.
2. **결정성(§7.2)**: 주문마다 다음 단계의 예정 비즈니스 시각과 취소 여부를 `random_seed`, `order_id`, 단계 이름의 Hash로 정한다. `logical_date`가 예정 시각 이상이면 그 전이를 적용한다. 대상은 `order_id` 순으로 처리한다. 같은 입력의 재실행은 기존처럼 `generator_runs` 결과를 재사용한다. 출력이 바뀌므로 `GENERATOR_VERSION`을 올린다. 취소 비율은 Seed 분포 수준으로 낮게 둔다.
3. **실행당 한 단계**: 한 실행에서 주문 하나는 최대 한 단계만 전이한다. `transitions.py`는 `updated_at`의 엄격한 증가와 "같은 `updated_at`에서 다른 값으로 변경 금지"를 강제한다. 여러 단계를 한 `updated_at`에 몰면 이 규칙과 충돌한다. 실행이 멈췄다가 재개되면 밀린 주문은 실행마다 한 단계씩 따라잡는다. 지금 `created`인 110건도 별도 Backfill 없이 이 방식으로 따라잡는다.
4. **Cursor 단조성(048)**: 전이의 `updated_at`은 현재 `logical_date`다. `_assert_source_cursor_forward`가 이미 쓰기 전에 `logical_date > max(cursor)`를 `orders`와 `order_payments`에 확인한다. 대상 Row의 기존 `updated_at`은 이전 실행의 `logical_date`이므로 항상 더 작다. `plan_order_transition`의 단조 증가 검사도 그대로 통과한다. 새 Guard는 필요 없다.
5. **비즈니스 시각 분리(§4.1)**: `order_approved_at`, `order_delivered_carrier_date`, `order_delivered_customer_date`에는 예정 비즈니스 시각을 쓴다. 예정 시각은 `logical_date` 이하다. 예정 시각이 과거면 §7.4 "과거 Event의 늦은 Update"가 자연스럽게 생긴다.
6. **Payment 연동**: `approved` 전이와 같은 실행에서 결제를 `pending → completed`로 바꾼다. `created → canceled`면 `pending → failed`, `approved → canceled`면 `completed → refunded`다. 두 테이블 변경은 같은 Transaction과 같은 `updated_at`을 쓴다. 한 Source 실행 안의 여러 테이블 변경이므로 기존 Lease와 Rollback 범위 안에 있다.
7. **증분 Watermark**: 전이된 Row는 `updated_at`이 전진하므로 다음 수집 구간에 잡힌다. `stg_orders`는 최신 상태를 고르고, `int_affected_order_keys`와 `fct_order`의 `delete+insert`가 해당 주문을 다시 계산한다. `order_payments`도 `affected_from_payments`로 같은 주문을 다시 계산한다. Warehouse 쪽 변경은 필요 없다. 수용 검증에 "전이된 주문이 다음 Publish의 `fct_order`에서 새 상태로 보인다"를 넣는다.
8. **SCD2(§7.5)**: 주문 상태는 SCD2 추적 속성이 아니다. PRD §0.5(v1.6 변경 요약)는 멤버십만 SCD2로 관리한다. 따라서 §7.5의 "수집 구간 내 1회" 제약은 직접 적용되지 않는다. 주문 전이 단계는 고객 등급과 구독 계약을 바꾸지 않아야 한다. 그래야 같은 실행의 `membership-change`·구독 Profile이 지키는 1회 제약에 영향을 주지 않는다. 3번의 "실행당 한 단계"는 주문 축에도 같은 관측 원칙을 적용한다.

## 결과로 생기는 동작 변화

- 일별 지표는 구매일 기준이다. 따라서 주문이 나중에 `DELIVERED`가 되면 이미 지난 날짜의 매출이 Publish마다 늘어난다. 이것은 Late Update를 반영한 정상 재계산이다. Dashboard Card 설명에 "최근 일자는 배송 완료에 따라 증가할 수 있음"을 적는다.
- 예정 시각의 간격이 `*/10` 실행 주기보다 훨씬 길면(예: 배송 수일) 짧은 관측 기간에는 `DELIVERED`가 늦게 보인다. 간격 값은 구현 계획에서 Seed의 단계별 지연 분포를 참고해 정한다.

## 권고안을 채택하지 않을 때(대안 A만 할 때)

- 주문 축 Card에 "Generator 신규 주문은 `CREATED`에 머물며 `DELIVERED` 기준 매출에 포함되지 않음"을 적는다.
- 상태별 주문 수 Card를 추가해 `CREATED` 누적을 드러낸다.
- PRD §7.1과 Phase 2 §4에 "V1 정기 실행은 주문 전이를 만들지 않는다"를 명시한다. 명세와 실행의 불일치를 문서로 닫기 위해서다.

## 문서 영향(B 채택 시)

- PRD §7.1 또는 §13.3에 정기 실행의 주문 전이 규칙(대상, 실행당 한 단계, 결정적 예정 시각)을 적는다. PRD 수정 절차(이전 버전을 `PRD.bak/`로 이동하고 버전 증가)를 따른다.
- Phase 2 "실행 통합"에 정기 실행 전이 항목과 검증 결과를 추가한다.
- 새 ADR은 필요 없다. 기존 결정의 누락된 연결을 채우는 변경이다.
