# Dashboard 증적

| Dashboard | Metabase ID | Dataset / Run / Export | Commit | Snapshot | Screenshot |
| --- | ---: | --- | --- | --- | --- |
| Sales | 2 | `Commerce Mart Serving` / `7472eb21-ef48-4c8d-bd5f-f5c3e6f4c1cf` / `191cafe0-8ce4-4570-a43d-418209dfd948` | `cfe3832e7f3dd38b7f8da719f72bc980d04c4cab` | `metabase/export/dashboard-2.json` | `docs/bi/screenshots/dashboard-2.png` |
| Product | 3 | `Commerce Mart Serving` / `7472eb21-ef48-4c8d-bd5f-f5c3e6f4c1cf` / `191cafe0-8ce4-4570-a43d-418209dfd948` | `cfe3832e7f3dd38b7f8da719f72bc980d04c4cab` | `metabase/export/dashboard-3.json` | `docs/bi/screenshots/dashboard-3.png` |
| Customer | 4 | `Commerce Mart Serving` / `7472eb21-ef48-4c8d-bd5f-f5c3e6f4c1cf` / `191cafe0-8ce4-4570-a43d-418209dfd948` | `cfe3832e7f3dd38b7f8da719f72bc980d04c4cab` | `metabase/export/dashboard-4.json` | `docs/bi/screenshots/dashboard-4.png` |

Serving Manifest의 Mart Hash는 `facts.fct_order=6be296904741b9e991ab45fbb44999dfb750f6f611892732a4889ae52d4e9778`, `facts.fct_order_item=9a76d6f7ae088b58194124865b7cc48aaf6f99b2e4a39c5101912e720c47bbd5`다. Dashboard API JSON은 카드와 Filter 매핑까지 포함하므로 Screenshot 대체 재현 증적이다.

card 56 `Region Analysis (주문 시점)`은 `customer_state`별 주문 수 내림차순 `bar`로 저장했다.
Brazil 주 코드의 기본 Region Map이 없는 Metabase에서 `map`은 지역 선택 경고만 표시하므로 사용하지
않는다. `metabase/export/card-56.json`과 `dashboard-4.json`, `docs/bi/screenshots/dashboard-4.png`는
2026-09-28에 이 설정으로 다시 생성했다.

Commit은 Run `7472eb21`이 Build한 `metrics.rpt_customer_order_activity_daily`와 세 Dashboard Query가 처음 함께 포함된 Commit이다. 직전 Commit `c5acea2`에는 이 Model이 없어 같은 Dataset을 재현할 수 없다.

`P10-23`은 2026-09-28에 완료했다. `scripts/capture_metabase_dashboards.py`가 `METABASE_API_KEY`를
`X-API-Key` 요청 헤더로만 전달하는 headless Chromium BrowserContext에서 세 Dashboard를 열어 표의
PNG를 저장했다. API JSON Snapshot은 Dashboard·카드·Filter의 재생성 기준으로 계속 보존하고, PNG는
실제 화면 증적으로 사용한다.
