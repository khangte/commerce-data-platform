# Dashboard 증적

| Dashboard | Metabase ID | Dataset / Run / Export | Commit | Snapshot | Screenshot |
| --- | ---: | --- | --- | --- | --- |
| Sales | 2 | `Commerce Mart Serving` / `c1bfae07-e7e9-4851-bf6b-9e687282a8f3` / `ae027aa3-c83c-4f61-8b2d-7516d4cedd87` | 미커밋 | `metabase/export/dashboard-2.json` | `docs/bi/screenshots/dashboard-2-2026-09-29.png` |
| Product | 3 | `Commerce Mart Serving` / `c1bfae07-e7e9-4851-bf6b-9e687282a8f3` / `ae027aa3-c83c-4f61-8b2d-7516d4cedd87` | 미커밋 | `metabase/export/dashboard-3.json` | `docs/bi/screenshots/dashboard-3-2026-09-29.png` |
| Customer | 4 | `Commerce Mart Serving` / `c1bfae07-e7e9-4851-bf6b-9e687282a8f3` / `ae027aa3-c83c-4f61-8b2d-7516d4cedd87` | 미커밋 | `metabase/export/dashboard-4.json` | `docs/bi/screenshots/dashboard-4-2026-09-29.png` |

2026-09-29 04:30 UTC Warehouse 실행 `manual__2026-09-29T04:30:00+00:00`은 `success`로 종료됐다. 캡처 직전 Serving Manifest는
Export `ae027aa3-c83c-4f61-8b2d-7516d4cedd87`와 Publish Run
`c1bfae07-e7e9-4851-bf6b-9e687282a8f3`의 Mart Hash를 보존했다. 이 Run은
`facts.fct_order` 99,541행, `facts.fct_order_item` 112,841행,
`dimensions.dim_subscription` 4행, `facts.fct_subscription_payment` 2행,
`metrics.rpt_subscription_funnel_daily` 4행을 Serving에 포함한다. Dashboard API JSON은 카드와
Filter 매핑을 보존하는 이전 설정 Snapshot이며, 위의 새 PNG는 현재 Serving 파일을 읽어 다시 캡처했다.
기존 `dashboard-{2,3,4}.png`는 이전 시점의 화면 이력으로 그대로 남겨뒀다.

논리 시각 2026-09-04~09-10의 Generator 실행으로 만든 결제 2건은 실패 Profile의 최초 실패와
2026-09-09 재시도다. 정규 자동 청구 예정일은 2026-10-04 이후여서 이 증적 구간에는 정규 자동 청구 결제가 없다.
Metabase가 교체 전 DuckDB 파일 연결을 유지해 이전 값을 반환했으므로 Serving 교체 뒤 Metabase만 재기동했다.
이후 card 54는 4개 이벤트 날짜, card 59는 `ACTIVE` 3건·`CANCEL_REQUESTED` 1건을 반환했고
세 Dashboard PNG를 다시 캡처했다. 카드 54·59 모두 빈 차트가 아니다.
card 54의 `payment_failed_count`는 0이다. Warehouse를 7회 생성 뒤 한 번만 실행해 중간
`PAYMENT_FAILED` 상태를 구독 차원 이력으로 관측하지 않았기 때문이다. 실패 결제 1건과 같은 회차의
성공 재시도 1건은 `facts.fct_subscription_payment`에 각각 보존돼 있다.

card 56 `Region Analysis (주문 시점)`은 `customer_state`별 주문 수 내림차순 `bar`로 저장했다.
Brazil 주 코드의 기본 Region Map이 없는 Metabase에서 `map`은 지역 선택 경고만 표시하므로 사용하지
않는다. `metabase/export/card-56.json`과 `dashboard-4.json`, `docs/bi/screenshots/dashboard-4.png`는
2026-09-28에 이 설정으로 다시 생성했다.

card 58 `Membership Tier Trend (주문 시점)`은 주문 Fact·주문 시점 고객 SCD2 결합을 날짜·거래 실적
등급별 고객 수로 펼친 line 카드다. `metabase/export/card-58.json`과 `dashboard-4.json`에는 card 58의
`calendar_date`·`membership_tier`·`customer_count` 시각화 설정과 Dashboard 4의 `(row 12, col 0, 12×4)`
배치가 보존돼 있으며, `docs/bi/screenshots/dashboard-4-2026-09-29.png`를 같은 설정으로 재캡처했다.

card 59 `Current Subscription Status Distribution`은 `is_current = true`인
`dimensions.dim_subscription` 계약 Version만 상태별로 중복 없이 세는 bar다. `metabase/export/card-59.json`과
`dashboard-4.json`에는 `subscription_status`·`contract_count` 시각화 설정과 Dashboard 4의
`(row 16, col 0, 6×4)` 배치가 보존돼 있으며, 실제 상태 막대가 렌더링된 Customer PNG를 재생성했다.

이번 화면 증적의 대조 수치와 구독 카드의 이벤트 합계는 [Dashboard 합계 대조](totals-reconciliation.md)에
기록했다.

`P10-23`은 2026-09-28에 완료했다. `scripts/capture_metabase_dashboards.py`가 `METABASE_API_KEY`를
`X-API-Key` 요청 헤더로만 전달하는 headless Chromium BrowserContext에서 세 Dashboard를 열어 표의
PNG를 저장했다. 기본 화면 뒤 8초의 카드 렌더링 대기를 두어 하단의 card 58·59도 골격 화면이 아닌 실제
차트로 저장한다. 논리 시각 2026-09-10까지 재증적에서 card 54의 상태 전이 시계열과 card 59의 현재 상태 분포가
빈 화면이 아닌 것을 확인하기 위해 Snapshot과 Customer PNG를 다시 생성했다. API JSON Snapshot은 Dashboard·카드·Filter의
재생성 기준으로 계속 보존하고, PNG는 실제 화면 증적으로 사용한다.
