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

2026-09-29 캡처에서는 Source 예약이 트리거한 최신 Warehouse 실행(04:30 UTC)이 끝난 뒤 Metabase만 재시작해
교체 전 DuckDB 파일 연결을 끊고 같은 스크립트에 임시 `--output-dir`를 지정했다. 생성된 화면은
`docs/bi/screenshots/dashboard-2-2026-09-29.png`, `dashboard-3-2026-09-29.png`,
`dashboard-4-2026-09-29.png`로 저장했다. 해당 시점 Publish Run은
`c1bfae07-e7e9-4851-bf6b-9e687282a8f3`, `facts.fct_order`는 99,541행이다.
기존 `dashboard-{2,3,4}.png`는 이전 화면 이력으로 보존한다. 다시 캡처할 때도 임시 출력
디렉터리를 써서 날짜가 붙은 새 파일을 만들고 기존 PNG를 덮어쓰지 않는다.

추가 최근 기간 화면은 Dashboard 2·3·4 각각에
`?utc-order-date=2026-09-28~2026-09-29`를 붙여 캡처했다. `UTC 주문일`은 양일을 포함하며,
이 구간의 `facts.fct_order`는 30행(28일 2건·29일 28건), 상품 항목은 56행이다.
최신 Warehouse 실행 `manual__2026-09-29T04:30:00+00:00`이 끝나고 Metabase를 재시작했을 때
Serving의 전체 주문은 99,541행이었다. 새 화면은
`docs/bi/screenshots/dashboard-{2,3,4}-2026-09-29-recent.png`에 저장했다.
기존 전체기간 `dashboard-{2,3,4}-2026-09-29.png`와 이전 `dashboard-{2,3,4}.png`는 보존했다.
Sales의 배송 완료 GMV·AOV와 Customer의 재구매 고객은 최근 구간에 해당 데이터가 없어 빈 차트다.
Customer의 구독 추이·등급 분포·등급 추이·현재 구독 상태 카드는 날짜 필터 매핑이 없으므로 전체기간
또는 현재 상태를 표시한다. 저장된 필드 필터 매핑의 `variable`을 `dimension`으로 수정한 후
Dashboard API에서 날짜 카드 9개와 주문 상태 카드 1개가 실제로 필터링되는 것을 확인했다.

Customer Dashboard의 card 56 `Region Analysis (주문 시점)`은 `fct_order.customer_state`별
`order_count`를 내림차순으로 표시하는 `bar`다. Brazil 주 코드에 대응하는 기본 Region Map이 없는
Metabase 환경에서도 같은 사건 시점 지역 분포를 화면에 안정적으로 표시하도록 `map`을 사용하지 않는다.

card 54 `Subscription Status Trend`는 `metrics.rpt_subscription_funnel_daily`의 이벤트 날짜와
계약 시작·활성·결제 실패·해지 신청·이탈·재가입 측정값을 line 차트로 표시한다. 논리 시각
2026-09-10까지의 재증적은 구독 계약 4건과 실패 Profile·재시도로 생성한 결제 2건을 포함해 빈 차트가 아닌 상태 전이 시계열을
확인했다. 정규 자동 청구는 첫 계약의 예정일인 2026-10-04 이후라 이번 구간에는 없다.

card 59 `Current Subscription Status Distribution`은
`metabase/queries/dashboard_subscription_status_distribution.sql`에서 `is_current = true`인
계약 Version만 상태별로 세는 bar다. 현재 시점 계약 상태 분포이므로, card 54의 상태 전이 이벤트
퍼널 시계열과 혼용하지 않는다.

card 58 `Membership Tier Trend (주문 시점)`은
`metabase/queries/dashboard_membership_tier_trend.sql`의 날짜별 거래 실적 등급 고객 수를 line 차트로
표시한다. `rpt_membership_tier_performance`와 동일하게 주문 Fact와 주문 시점 고객 SCD2 버전을 결합하되,
날짜 축으로 펼친다. 따라서 card 55의 등급별 주문·GMV 집계 및 card 57의 현재 등급 분포와 혼용하지 않는다.
