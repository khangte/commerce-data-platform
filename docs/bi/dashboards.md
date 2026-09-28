# Phase 10 Dashboard 재현 안내

Metabase Collection `Commerce Mart 대시보드`의 Dashboard ID는 Sales `2`, Product `3`, Customer `4`다. 모두 `Commerce Mart Serving`(database ID `2`)만 읽는다.

| Dashboard | 카드 | Source Model | Filter / 기준 시점 |
| --- | --- | --- | --- |
| Sales | Daily GMV, Daily Orders, Daily AOV | `fct_order`, `dim_date` | UTC 주문일. 주문 상태는 Orders에만 적용하며 GMV·AOV는 항상 `DELIVERED`다. |
| Product | Category GMV, Top Products, Sales Volume | `fct_order_item`, `dim_product`, `dim_date` | UTC 주문일. 주문 Fact를 결합하지 않는다. |
| Customer | New/Repeat, Subscription Status Trend·Current Distribution, Membership Tier 집계·추이, Region, Current Tier | `rpt_customer_order_activity_daily`, `rpt_subscription_funnel_daily`, `rpt_membership_tier_performance`, `dim_subscription`, `fct_order`, `dim_customer`, `dim_date` | New/Repeat·구독 추이·등급·지역은 사건 발생 시점, Current Tier·Current Subscription Status는 현재 시점이다. Region은 주별 주문 수 내림차순 막대 차트다. |

Query 원본은 `metabase/queries/dashboard_*.sql`과 API Snapshot의 `dataset_query`에 보존한다. 빈 Metabase 앱 DB에서 Connection을 만든 뒤 `METABASE_API_KEY`를 설정하고 `scripts/metabase_snapshot.sh`의 JSON을 카드·Dashboard API payload의 기준으로 사용해 재생성한다. OSS에는 별도 직렬화 Export가 없으므로 PostgreSQL 앱 DB와 이 JSON Snapshot을 함께 보관한다.

화면 증적은 headless Chromium으로 재캡처한다. `docker compose --profile bi up -d metabase` 뒤
`uv run playwright install chromium`(최초 1회)과 `uv run python scripts/capture_metabase_dashboards.py`를
실행하면 Dashboard 2·3·4가 각각 `docs/bi/screenshots/dashboard-{2,3,4}.png`로 저장된다. 스크립트는
`METABASE_API_KEY`를 BrowserContext의 `X-API-Key` 요청 헤더로만 전달하며 URL·출력 파일에는 넣지 않는다.
각 Dashboard는 하단 카드까지 실제 차트로 렌더링할 수 있도록 기본 화면이 보인 뒤 8초를 더 기다린다.

Customer Dashboard의 card 56 `Region Analysis (주문 시점)`은 `fct_order.customer_state`별
`order_count`를 내림차순으로 표시하는 `bar`다. Brazil 주 코드에 대응하는 기본 Region Map이 없는
Metabase 환경에서도 같은 사건 시점 지역 분포를 화면에 안정적으로 표시하도록 `map`을 사용하지 않는다.

card 54 `Subscription Status Trend`는 `metrics.rpt_subscription_funnel_daily`의 이벤트 날짜와
계약 시작·활성·결제 실패·해지 신청·이탈·재가입 측정값을 line 차트로 표시한다. 논리 시각
2026-10-28 재증적은 구독 계약 4건과 자동 청구 결제 1건을 포함해 빈 차트가 아닌 상태 전이 시계열을
확인했다.

card 59 `Current Subscription Status Distribution`은
`metabase/queries/dashboard_subscription_status_distribution.sql`에서 `is_current = true`인
계약 Version만 상태별로 세는 bar다. 현재 시점 계약 상태 분포이므로, card 54의 상태 전이 이벤트
퍼널 시계열과 혼용하지 않는다.

card 58 `Membership Tier Trend (주문 시점)`은
`metabase/queries/dashboard_membership_tier_trend.sql`의 날짜별 거래 실적 등급 고객 수를 line 차트로
표시한다. `rpt_membership_tier_performance`와 동일하게 주문 Fact와 주문 시점 고객 SCD2 버전을 결합하되,
날짜 축으로 펼친다. 따라서 card 55의 등급별 주문·GMV 집계 및 card 57의 현재 등급 분포와 혼용하지 않는다.
