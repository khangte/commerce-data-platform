# Phase 10 Dashboard 재현 안내

Metabase Collection `Commerce Mart 대시보드`의 Dashboard ID는 Sales `2`, Product `3`, Customer `4`다. 모두 `Commerce Mart Serving`(database ID `2`)만 읽는다.

| Dashboard | 카드 | Source Model | Filter / 기준 시점 |
| --- | --- | --- | --- |
| Sales | Daily GMV, Daily Orders, Daily AOV | `fct_order`, `dim_date` | UTC 주문일. 주문 상태는 Orders에만 적용하며 GMV·AOV는 항상 `DELIVERED`다. |
| Product | Category GMV, Top Products, Sales Volume | `fct_order_item`, `dim_product`, `dim_date` | UTC 주문일. 주문 Fact를 결합하지 않는다. |
| Customer | New/Repeat, Subscription Status Trend, Membership Tier, Region, Current Tier | `rpt_customer_order_activity_daily`, `rpt_subscription_funnel_daily`, `rpt_membership_tier_performance`, `fct_order`, `dim_customer` | New/Repeat·구독·등급·지역은 사건 발생 시점, Current Tier는 현재 시점이다. |

Query 원본은 `metabase/queries/dashboard_*.sql`과 API Snapshot의 `dataset_query`에 보존한다. 빈 Metabase 앱 DB에서 Connection을 만든 뒤 `METABASE_API_KEY`를 설정하고 `scripts/metabase_snapshot.sh`의 JSON을 카드·Dashboard API payload의 기준으로 사용해 재생성한다. OSS에는 별도 직렬화 Export가 없으므로 PostgreSQL 앱 DB와 이 JSON Snapshot을 함께 보관한다.

화면 증적은 headless Chromium으로 재캡처한다. `docker compose --profile bi up -d metabase` 뒤
`uv run playwright install chromium`(최초 1회)과 `uv run python scripts/capture_metabase_dashboards.py`를
실행하면 Dashboard 2·3·4가 각각 `docs/bi/screenshots/dashboard-{2,3,4}.png`로 저장된다. 스크립트는
`METABASE_API_KEY`를 BrowserContext의 `X-API-Key` 요청 헤더로만 전달하며 URL·출력 파일에는 넣지 않는다.
