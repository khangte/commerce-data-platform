# Dashboard 증적

| Dashboard | Metabase ID | Dataset / Run / Export | Commit | Snapshot |
| --- | ---: | --- | --- | --- |
| Sales | 2 | `Commerce Mart Serving` / `7472eb21-ef48-4c8d-bd5f-f5c3e6f4c1cf` / `191cafe0-8ce4-4570-a43d-418209dfd948` | `cfe3832e7f3dd38b7f8da719f72bc980d04c4cab` | `metabase/export/dashboard-2.json` |
| Product | 3 | `Commerce Mart Serving` / `7472eb21-ef48-4c8d-bd5f-f5c3e6f4c1cf` / `191cafe0-8ce4-4570-a43d-418209dfd948` | `cfe3832e7f3dd38b7f8da719f72bc980d04c4cab` | `metabase/export/dashboard-3.json` |
| Customer | 4 | `Commerce Mart Serving` / `7472eb21-ef48-4c8d-bd5f-f5c3e6f4c1cf` / `191cafe0-8ce4-4570-a43d-418209dfd948` | `cfe3832e7f3dd38b7f8da719f72bc980d04c4cab` | `metabase/export/dashboard-4.json` |

Serving Manifest의 Mart Hash는 `facts.fct_order=6be296904741b9e991ab45fbb44999dfb750f6f611892732a4889ae52d4e9778`, `facts.fct_order_item=9a76d6f7ae088b58194124865b7cc48aaf6f99b2e4a39c5101912e720c47bbd5`다. Dashboard API JSON은 카드와 Filter 매핑까지 포함하므로 Screenshot 대체 재현 증적이다.

Commit은 Run `7472eb21`이 Build한 `metrics.rpt_customer_order_activity_daily`와 세 Dashboard Query가 처음 함께 포함된 Commit이다. 직전 Commit `c5acea2`에는 이 Model이 없어 같은 Dataset을 재현할 수 없다.

Screenshot(`P10-23`)은 아직 없다. 작업 환경에 Browser 캡처 도구가 없어 화면을 찍지 못했다. 위 API JSON Snapshot이 Dashboard 구성과 식별자를 대신 보존한다. 캡처 도구를 쓸 수 있게 되면 `docs/bi/screenshots/dashboard-{2,3,4}.png`로 저장하고 이 표의 식별자를 화면 설명에 연결한다.
