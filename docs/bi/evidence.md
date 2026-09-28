# Dashboard 증적

| Dashboard | Metabase ID | Dataset / Run / Export | Commit | Snapshot | Screenshot |
| --- | ---: | --- | --- | --- | --- |
| Sales | 2 | `Commerce Mart Serving` / `8890b135-5391-4290-8b05-bf99f24668a6` / `0616bd30-7c38-4a3c-b03b-aaecb0cc6cec` | `5e149ef534aca5d39e2461ba211497e3081e0a2f` | `metabase/export/dashboard-2.json` | `docs/bi/screenshots/dashboard-2.png` |
| Product | 3 | `Commerce Mart Serving` / `8890b135-5391-4290-8b05-bf99f24668a6` / `0616bd30-7c38-4a3c-b03b-aaecb0cc6cec` | `5e149ef534aca5d39e2461ba211497e3081e0a2f` | `metabase/export/dashboard-3.json` | `docs/bi/screenshots/dashboard-3.png` |
| Customer | 4 | `Commerce Mart Serving` / `8890b135-5391-4290-8b05-bf99f24668a6` / `0616bd30-7c38-4a3c-b03b-aaecb0cc6cec` | `5e149ef534aca5d39e2461ba211497e3081e0a2f` | `metabase/export/dashboard-4.json` | `docs/bi/screenshots/dashboard-4.png` |

Serving Manifest는 Export `0616bd30-7c38-4a3c-b03b-aaecb0cc6cec`와 Publish Run
`8890b135-5391-4290-8b05-bf99f24668a6`의 Mart Hash를 보존한다. 이 Run은
`dimensions.dim_subscription` 4행, `facts.fct_subscription_payment` 1행,
`metrics.rpt_subscription_funnel_daily` 4행을 Serving에 포함한다. Dashboard API JSON은 카드와
Filter 매핑까지 포함하므로 화면 PNG의 재생성 기준으로도 사용한다.

card 56 `Region Analysis (주문 시점)`은 `customer_state`별 주문 수 내림차순 `bar`로 저장했다.
Brazil 주 코드의 기본 Region Map이 없는 Metabase에서 `map`은 지역 선택 경고만 표시하므로 사용하지
않는다. `metabase/export/card-56.json`과 `dashboard-4.json`, `docs/bi/screenshots/dashboard-4.png`는
2026-09-28에 이 설정으로 다시 생성했다.

card 58 `Membership Tier Trend (주문 시점)`은 주문 Fact·주문 시점 고객 SCD2 결합을 날짜·거래 실적
등급별 고객 수로 펼친 line 카드다. `metabase/export/card-58.json`과 `dashboard-4.json`에는 card 58의
`calendar_date`·`membership_tier`·`customer_count` 시각화 설정과 Dashboard 4의 `(row 12, col 0, 12×4)`
배치가 보존돼 있으며, `docs/bi/screenshots/dashboard-4.png`를 같은 설정으로 재캡처했다.

Commit `5e149ef`은 구독 계약·결제 재증적 전후의 세 Dashboard Query와 현재 Dataset을 함께 확인한
기준이다. 대조 수치와 구독 카드의 이벤트 합계는 [Dashboard 합계 대조](totals-reconciliation.md)에
기록했다.

`P10-23`은 2026-09-28에 완료했다. `scripts/capture_metabase_dashboards.py`가 `METABASE_API_KEY`를
`X-API-Key` 요청 헤더로만 전달하는 headless Chromium BrowserContext에서 세 Dashboard를 열어 표의
PNG를 저장했다. 논리 시각 2026-10-28 재증적에서 card 54의 상태 전이 시계열이 빈 화면이 아닌 것을
확인하기 위해 Snapshot과 Customer PNG를 다시 생성했다. API JSON Snapshot은 Dashboard·카드·Filter의 재생성
기준으로 계속 보존하고, PNG는 실제 화면 증적으로 사용한다.
