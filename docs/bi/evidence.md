# Dashboard 증적

| Dashboard | Metabase ID | Dataset / Run / Export | Commit | Snapshot | Screenshot |
| --- | ---: | --- | --- | --- | --- |
| Sales | 2 | `Commerce Mart Serving` / `c1bfae07-e7e9-4851-bf6b-9e687282a8f3` / `ae027aa3-c83c-4f61-8b2d-7516d4cedd87` | `454fe40` | `metabase/export/dashboard-2.json` | `docs/bi/screenshots/dashboard-2-2026-09-29.png` |
| Product | 3 | `Commerce Mart Serving` / `c1bfae07-e7e9-4851-bf6b-9e687282a8f3` / `ae027aa3-c83c-4f61-8b2d-7516d4cedd87` | `454fe40` | `metabase/export/dashboard-3.json` | `docs/bi/screenshots/dashboard-3-2026-09-29.png` |
| Customer | 4 | `Commerce Mart Serving` / `c1bfae07-e7e9-4851-bf6b-9e687282a8f3` / `ae027aa3-c83c-4f61-8b2d-7516d4cedd87` | `454fe40` | `metabase/export/dashboard-4.json` | `docs/bi/screenshots/dashboard-4-2026-09-29.png` |
| Sales (최근 주문일) | 2 | `Commerce Mart Serving` / `c1bfae07-e7e9-4851-bf6b-9e687282a8f3` / `ae027aa3-c83c-4f61-8b2d-7516d4cedd87` | `c13b483` | `metabase/export/dashboard-2.json` | `docs/bi/screenshots/dashboard-2-2026-09-29-recent.png` |
| Product (최근 주문일) | 3 | `Commerce Mart Serving` / `c1bfae07-e7e9-4851-bf6b-9e687282a8f3` / `ae027aa3-c83c-4f61-8b2d-7516d4cedd87` | `c13b483` | `metabase/export/dashboard-3.json` | `docs/bi/screenshots/dashboard-3-2026-09-29-recent.png` |
| Customer (최근 주문일) | 4 | `Commerce Mart Serving` / `c1bfae07-e7e9-4851-bf6b-9e687282a8f3` / `ae027aa3-c83c-4f61-8b2d-7516d4cedd87` | `c13b483` | `metabase/export/dashboard-4.json` | `docs/bi/screenshots/dashboard-4-2026-09-29-recent.png` |
| Sales (전체 너비·전체기간) | 2 | `Commerce Mart Serving` / `c1bfae07-e7e9-4851-bf6b-9e687282a8f3` / `ae027aa3-c83c-4f61-8b2d-7516d4cedd87` | `bc70fae` | `metabase/export/dashboard-2.json` | `docs/bi/screenshots/dashboard-2-2026-09-29-full.png` |
| Product (전체 너비·전체기간) | 3 | `Commerce Mart Serving` / `c1bfae07-e7e9-4851-bf6b-9e687282a8f3` / `ae027aa3-c83c-4f61-8b2d-7516d4cedd87` | `bc70fae` | `metabase/export/dashboard-3.json` | `docs/bi/screenshots/dashboard-3-2026-09-29-full.png` |
| Customer (전체 너비·전체기간) | 4 | `Commerce Mart Serving` / `c1bfae07-e7e9-4851-bf6b-9e687282a8f3` / `ae027aa3-c83c-4f61-8b2d-7516d4cedd87` | `bc70fae` | `metabase/export/dashboard-4.json` | `docs/bi/screenshots/dashboard-4-2026-09-29-full.png` |
| Sales (전체 너비·최근 주문일) | 2 | `Commerce Mart Serving` / `c1bfae07-e7e9-4851-bf6b-9e687282a8f3` / `ae027aa3-c83c-4f61-8b2d-7516d4cedd87` | `bc70fae` | `metabase/export/dashboard-2.json` | `docs/bi/screenshots/dashboard-2-2026-09-29-full-recent.png` |
| Product (전체 너비·최근 주문일) | 3 | `Commerce Mart Serving` / `c1bfae07-e7e9-4851-bf6b-9e687282a8f3` / `ae027aa3-c83c-4f61-8b2d-7516d4cedd87` | `bc70fae` | `metabase/export/dashboard-3.json` | `docs/bi/screenshots/dashboard-3-2026-09-29-full-recent.png` |
| Customer (전체 너비·최근 주문일) | 4 | `Commerce Mart Serving` / `c1bfae07-e7e9-4851-bf6b-9e687282a8f3` / `ae027aa3-c83c-4f61-8b2d-7516d4cedd87` | `bc70fae` | `metabase/export/dashboard-4.json` | `docs/bi/screenshots/dashboard-4-2026-09-29-full-recent.png` |

2026-09-29 04:30 UTC Warehouse 실행 `manual__2026-09-29T04:30:00+00:00`은 `success`로 종료됐다. 캡처 직전 Serving Manifest는
Export `ae027aa3-c83c-4f61-8b2d-7516d4cedd87`와 Publish Run
`c1bfae07-e7e9-4851-bf6b-9e687282a8f3`의 Mart Hash를 보존했다. 이 Run은
`facts.fct_order` 99,541행, `facts.fct_order_item` 112,841행,
`dimensions.dim_subscription` 4행, `facts.fct_subscription_payment` 2행,
`metrics.rpt_subscription_funnel_daily` 4행을 Serving에 포함한다. Dashboard API JSON은 카드와
Filter 매핑을 보존하는 설정 Snapshot이며, 위의 PNG는 해당 Serving 파일을 읽어 캡처했다.
기존 `dashboard-{2,3,4}.png`는 이전 시점의 화면 이력으로 그대로 남겨뒀다.

전체 너비 재캡처는 최신 Warehouse 실행 `manual__2026-09-29T04:30:00+00:00`의 `success`와
실행 중인 Warehouse 0건을 확인하고 Metabase를 재시작해 health 200을 받은 뒤 진행했다.
Serving `facts.fct_order`는 99,541행이었다. `full` PNG는 필터 없는 전체기간,
`full-recent` PNG는 `UTC 주문일=2026-09-28~2026-09-29`(양일 포함) 필터 화면이다.
Dashboard 2·3·4의 `width`를 `fixed`에서 `full`로 바꿨다. Customer Dashboard가 여전히 왼쪽에
쏠린 원인은 브라우저 캐시가 아니라 카드 배치였다. 두 카드 행의 너비 합이 12/24칸,
단독 카드 행은 12/24칸 또는 6/24칸에 그쳤다. Customer의 `col`·`size_x`만 조정해
두 카드 행은 각각 12칸씩, 단독 카드 행은 24칸을 채웠다. `size_y`·카드 순서·필터
매개변수와 매핑·카드 SQL은 그대로 유지했다. 기존 PNG는 이력으로 모두 보존했다.

추가 최근 기간 화면은 `UTC 주문일=2026-09-28~2026-09-29`(양일 포함) URL 필터를 적용했다.
이 구간에는 주문 30건(28일 2건·29일 28건)과 상품 항목 56행이 있다. 최신 Warehouse 실행
`manual__2026-09-29T04:30:00+00:00` 종료와 Serving `facts.fct_order` 99,541행을 다시 확인한 뒤
Metabase만 재시작하고 Dashboard 2·3·4를 캡처했다. 최근 주문은 `CREATED` 상태여서 Sales의
배송 완료 GMV·AOV는 빈 차트이고, Customer의 재구매 고객도 이 구간에 없다. Customer의 구독·등급
카드 중 날짜 매핑이 없는 카드는 전체기간 또는 현재 상태를 계속 표시한다. 기존 전체기간
`dashboard-{2,3,4}-2026-09-29.png`도 그대로 보존했다.

추가 캡처 전, Metabase Dashboard의 날짜 필터 칩은 날짜를 표시했지만 카드 질의의 날짜 조건이
빠지는 결함을 확인했다. 저장된 필드 필터 매핑이 `variable`을 가리킨 것이 원인이었다. 수정 전
Metabase 앱 DB와 `metabase/export/` JSON Snapshot을
`data/generated/backups/metabase-filter-2026-09-29/`에 백업했다. 카드 47 하나에서 매핑을
`dimension`으로 바꾸자 Dashboard API 결과가 전체 643일에서 요청한 2일로 줄어드는 것을 확인한 뒤,
날짜 매핑 카드 46·47·48·49·50·51·52·53·56과 카드 47의 주문 상태 매핑에 같은 수정을 적용했다.
Dashboard ID·필터 ID·URL slug·카드 SQL은 유지했고, `scripts/metabase_snapshot.sh`로 JSON Snapshot을
다시 저장했다. Dashboard API에서 날짜 카드 모두 최근 기간 조건이 SQL에 반영됐고, 필터를 비우면
직접 카드 조회와 같은 전체기간 결과를 반환했다. 주문 상태 `CREATED`만 선택하면 전체 105건,
최근 기간을 함께 적용하면 30건이다.
검증 뒤 저장된 마지막 필터값을 세 Dashboard에서 비웠다. 실험 중 카드 47 직접 조회로 생긴
필터 매개변수 ID의 고아 값 1건은 `user_parameter_value` 테이블 백업을 추가로 만든 뒤 해당 행만
트랜잭션에서 제거했다. 이후 API Key 사용자의 Dashboard 2·3·4 `last_used_param_values`는 모두 비었다.

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
등급별 고객 수로 펼친 line 카드다. `metabase/export/card-58.json`에는
`calendar_date`·`membership_tier`·`customer_count` 시각화 설정이 보존돼 있다.
`docs/bi/screenshots/dashboard-4-2026-09-29.png`에는 당시 Dashboard 4의
`(row 12, col 0, 12×4)` 배치가 남아 있으며, 현재 `dashboard-4.json`은 `(row 12, col 0, 24×4)`다.

card 59 `Current Subscription Status Distribution`은 `is_current = true`인
`dimensions.dim_subscription` 계약 Version만 상태별로 중복 없이 세는 bar다.
`metabase/export/card-59.json`에는 `subscription_status`·`contract_count` 시각화 설정이 보존돼 있다.
기존 Customer PNG에는 당시 `(row 16, col 0, 6×4)` 배치와 실제 상태 막대가 남아 있으며,
현재 `dashboard-4.json`은 `(row 16, col 0, 24×4)`다.

이번 화면 증적의 대조 수치와 구독 카드의 이벤트 합계는 [Dashboard 합계 대조](totals-reconciliation.md)에
기록했다.

`P10-23`은 2026-09-28에 완료했다. `scripts/capture_metabase_dashboards.py`가 `METABASE_API_KEY`를
`X-API-Key` 요청 헤더로만 전달하는 headless Chromium BrowserContext에서 세 Dashboard를 열어 표의
PNG를 저장했다. 기본 화면 뒤 8초의 카드 렌더링 대기를 두어 하단의 card 58·59도 골격 화면이 아닌 실제
차트로 저장한다. 논리 시각 2026-09-10까지 재증적에서 card 54의 상태 전이 시계열과 card 59의 현재 상태 분포가
빈 화면이 아닌 것을 확인하기 위해 Snapshot과 Customer PNG를 다시 생성했다. API JSON Snapshot은 Dashboard·카드·Filter의
재생성 기준으로 계속 보존하고, PNG는 실제 화면 증적으로 사용한다.
