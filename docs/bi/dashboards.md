# Phase 10 Dashboard 재현 안내

Metabase Collection `Commerce Mart 대시보드`의 Dashboard ID는 Sales `2`, Product `3`, Customer `4`다. 모두 `Commerce Mart Serving`(database ID `2`)만 읽는다.

| Dashboard | 카드 | Source Model | Filter / 기준 시점 |
| --- | --- | --- | --- |
| Sales | Daily GMV, Daily Orders, Daily AOV | `fct_order`, `dim_date` | UTC 주문일. 주문 상태는 Orders에만 적용하며 GMV·AOV는 항상 `DELIVERED`다. |
| Product | Category GMV Top 10, Product GMV Top 10, Product Item Count Top 10 | `fct_order_item`, `dim_product`, `dim_date` | UTC 주문일. 주문 Fact를 결합하지 않으며 배송 완료 항목만 집계한다. |
| Customer | New/Repeat, Subscription Status Trend·Current Distribution, Membership Tier 집계·추이, Region, Current Tier | `rpt_customer_order_activity_daily`, `rpt_subscription_funnel_daily`, `rpt_membership_tier_performance`, `dim_subscription`, `fct_order`, `dim_customer`, `dim_date` | New/Repeat·구독 추이·등급·지역은 사건 발생 시점, Current Tier·Current Subscription Status는 현재 시점이다. Region은 주별 주문 수 내림차순 막대 차트다. |

세 Dashboard 맨 위의 card 60 `Domain Publish State (도메인 게시 상태 · 필터 제외)`는 `serving_domain_status`(도메인별 게시 상태)에서
주문(`orders`)·구독(`subscriptions`)의 게시 상태, 마지막 정상 게시 시각과 마지막 사건 시각(UTC), 연속 보류 횟수를
표시한다. `보류`는 해당 도메인이 이전 게시 데이터를 사용한다는 뜻이다. 마지막 정상 게시 시각은
`refreshed_at`(마지막 비보류 게시 시각)이며 `as_of`(마지막 사건 시각)가 아니다. 예상 도메인 행이 없거나 상태·시각을 알 수 없으면 `확인 불가`로 표시한다. `정상 (보류 없음)`은 보류가 없다는 뜻이며 최신성이나 마지막 실행의 성공을 보장하지 않는다. 이 표는 지연 임계값을 판정하지 않으며 Dashboard 날짜·주문
상태 필터의 영향을 받지 않는다. 모바일에서는 표를 가로로 스크롤해 남은 열을 볼 수 있다.

2026-11-12 보류 사례 검증: 당시 게시 `e34cbcfc-6ea1-4aee-9eed-97bb927af43b`, 내보내기
`106c5954-1910-4d7a-8b9b-fece9fd30fea`에서 주문 정상·구독 2회 연속 보류를 확인했다.
[보류 화면](screenshots/domain-status-held-2026-11-12.png)과
[모바일 화면](screenshots/domain-status-mobile-2026-11-12.png)은 실제 Serving 결과다.
[정상 화면](screenshots/domain-status-normal-real-2026-11-12.png)은 실제 전체 정상 Serving 내보내기
`ec871ab0-6bb6-428e-9538-a9ff18cce6c6`의 Customer Dashboard를 응답 대체 없이 캡처한 것이다.
주문·구독 모두 정상·연속 보류 0회이며 마지막 정상 게시 시각은 `2026-11-12 02:25:47` UTC다.
이전 [정상 대역 화면](screenshots/domain-status-normal-fixture-2026-11-12.png)은 실제 Serving 파일을 바꾸지
않고 브라우저에서 card 60의 구독 행을 정상·0회·`2026-11-11 09:49:42` UTC로 대체한 검증 이력이다.
빈 결과·HTTP 500·확인 불가 응답도 브라우저에서 대체해 각각 `결과 없음`, 차트
오류, `확인 불가` 표시를 확인했다. NULL 값은 카드 SQL을 DuckDB 임시 테이블에 실행해 별도 확인했다.

BI SQL의 배포 정본은 `metabase/export/card-<id>.json`의 `dataset_query`(카드 쿼리 설정)다.
`metabase/queries/*.sql`은 Template Tag가 없는 대조용 참조이며 배포 입력으로 쓰지 않는다.
빈 Metabase 앱 DB에서는 Connection을 만든 뒤 JSON의 배포 필드를 카드·Dashboard API payload의 기준으로 사용한다.
OSS에는 별도 직렬화 Export가 없으므로 PostgreSQL 앱 DB와 JSON Snapshot을 함께 보관한다.

`uv run python scripts/metabase_snapshot.py`는 Collection 5·6의 카드·Metric·Dataset·Dashboard를
조회해 배포 필드만 저장한다. `metabase/export/`에 커밋하지 않은 변경(미추적 파일 포함)이 있으면
중단하며, 차이를 검토한 뒤 `--force`로 덮어쓸 수 있다.

## 기존 카드의 쿼리 반영

```bash
uv run python scripts/metabase_import.py 49 50 51
uv run python scripts/metabase_import.py 49 50 51 --apply
```

- 기본 동작은 읽기 전용 GET과 차이 출력이다.
- `--apply`는 모든 GET, DB 검사(Export `dataset_query.database` = 운영 `database_id`),
  [Serving 최신 여부 검사](../../metabase/README.md#갱신과-최신-상태-확인)를 통과한 뒤 `dataset_query`만 PUT한다.
- 일부 PUT 실패 시 이미 반영된 카드는 자동으로 되돌리지 않는다. 카드별로
  `GET /api/revision?entity=card&id=<id>`에서 이력을 확인하고 `POST /api/revision/revert`에
  `{"entity": "card", "id": <id>, "revision_id": <rid>}`를 보내 롤백한다.
- 카드 설명·이름·표시 설정은 Metabase에서 수정한 뒤 Snapshot으로 보존한다. 저장소 JSON 편집의 배포 대상은 `dataset_query`뿐이다.
- Snapshot은 운영 설정을 파일로 내보내는 도구다. 가져오기 단계에서 실행하면 저장된 변경을 덮어쓴다.

## 화면 증적

화면 증적은 headless Chromium으로 재캡처한다. `docker compose --profile bi up -d metabase` 뒤
`uv run playwright install chromium`(최초 1회)과 `uv run python scripts/capture_metabase_dashboards.py`를
실행하면 Dashboard 2·3·4가 각각 `docs/bi/screenshots/dashboard-{2,3,4}.png`로 저장된다. 스크립트는
`METABASE_API_KEY`를 BrowserContext의 `X-API-Key` 요청 헤더로만 전달하며 URL·출력 파일에는 넣지 않는다.
각 Dashboard는 `GET /api/dashboard/{id}`의 쿼리 카드가 모두 `completed`로 응답하고 로딩 표시가 사라진 뒤 0.5초 더 기다려 캡처한다. 카드 쿼리가 실패하거나 `--timeout-ms`(기본 60초) 안에 끝나지 않으면 PNG를 저장하지 않고 실패한다. Metabase가 Dashboard 본문을 내부 스크롤 영역에 그리므로, 렌더링 확인 뒤 넘친 높이만큼 viewport를 늘려 마지막 카드까지 한 장에 저장한다(최대 16,000px).

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

전체 너비 화면은 Dashboard 2·3·4의 `width=full` 설정으로 다시 캡처했다.
Customer는 기존 카드 너비의 행별 합이 24칸보다 작아 화면 왼쪽에만 모였다.
두 카드 행은 각각 12칸씩, 단독 카드 행은 24칸으로 `col`·`size_x`만 조정했다.
카드 높이·순서·필터·SQL은 유지했다. `docs/bi/screenshots/dashboard-{2,3,4}-2026-09-29-full.png`는
전체기간, `dashboard-{2,3,4}-2026-09-29-full-recent.png`는
`UTC 주문일=2026-09-28~2026-09-29`(양일 포함)를 보여준다. 캡처 직전 Warehouse 실행
`manual__2026-09-29T04:30:00+00:00`의 성공과 실행 중인 작업 0건을 확인하고 Metabase를
재시작했다. 당시 Serving `facts.fct_order`는 99,541행, 최근 2일 주문은 30건이었다.
이전 전체기간·최근 기간 PNG는 모두 화면 이력으로 보존한다.

Customer Dashboard의 card 56 `Orders by Region (주문 시점 주별 주문 수)`은 `fct_order.customer_state`별
`order_count`를 내림차순으로 표시하는 `bar`다. Brazil 주 코드에 대응하는 기본 Region Map이 없는
Metabase 환경에서도 같은 사건 시점 지역 분포를 화면에 안정적으로 표시하도록 `map`을 사용하지 않는다.

card 54 `Subscription Transition Events (구독 상태 전이 건수 · 전체 기간)`는 `metrics.rpt_subscription_funnel_daily`의 이벤트 날짜와
계약 시작·활성·결제 실패·해지 신청·이탈·재가입 측정값을 막대 차트로 표시한다. 논리 시각
2026-09-10까지의 재증적은 구독 계약 4건과 실패 Profile·재시도로 생성한 결제 2건을 포함해 빈 차트가 아닌 상태 전이 시계열을
확인했다. 정규 자동 청구는 첫 계약의 예정일인 2026-10-04 이후라 이번 구간에는 없다.

card 59 `Current Subscription Status (현재 구독 계약 상태 분포 · 날짜 필터 제외)`는
`metabase/queries/dashboard_subscription_status_distribution.sql`에서 `is_current = true`인
계약 Version만 상태별로 세는 bar다. 현재 시점 계약 상태 분포이므로, card 54의 상태 전이 이벤트
퍼널 시계열과 혼용하지 않는다.

card 58 `Daily Ordering Customers by Tier (주문 시점 등급별 일별 고객 수 · 전체 기간)`는
`metabase/queries/dashboard_membership_tier_trend.sql`의 날짜별 거래 실적 등급 고객 수를 line 차트로
표시한다. `rpt_membership_tier_performance`와 동일하게 주문 Fact와 주문 시점 고객 SCD2 버전을 결합하되,
날짜 축으로 펼친다. 따라서 card 55의 등급별 주문·GMV 집계 및 card 57의 현재 등급 분포와 혼용하지 않는다. 날짜별 고객 수는 서로 다른 날짜나 등급을 넘어 합산하지 않는다.

## 2026-11-12 화면 범위 개선

- 제목은 `English (한국어)` 형식을 사용한다. 변경 금지 대상으로 지정된 독립 카드 43–45는 기존 제목과 정의를 보존했다.
- Sales의 주문 상태 필터는 일별 주문 수(card 47)에만 적용된다. 거래액(card 46)과 주문당 평균 거래액(card 48)은 배송 완료 주문 기준이며 평균의 분모는 같은 날의 배송 완료 주문 수다. 주문일 필터는 세 카드에 모두 적용된다.
- Customer의 주문일 필터는 첫 주문 고객(52), 재구매 고객(53), 지역별 주문(56)에만 적용된다. 구독 전이(54), 주문 시점 등급 지표(55), 등급별 일별 고객(58)은 전체 기간이고 현재 등급(57)과 구독 상태(59)는 현재 시점이다. 첫 주문일은 알려진 주문 이력 기준이며 같은 날 후속 주문은 신규로 분류된다.
- 등급 지표(55)는 단위가 다른 고객 수·주문 수·배송 완료 거래액·주문당 평균 거래액을 별도 열로 표시한다. 고객 수는 날짜나 등급 간 합산할 수 없다. 상품 화면은 배송 완료 항목 기준 상위 10개를 표로 보여 상품 식별자를 읽을 수 있다.
- 도메인 상태의 마지막 사건 시각은 참고용이며 지연 임계값 판정이 아니다. 이 표만으로 전체 게시 실행 상태나 행 수 감소 경보를 판단할 수 없다.

개선 후 실제 화면: [Sales](screenshots/dashboard-2-scope-2026-11-12.png), [Product](screenshots/dashboard-3-scope-2026-11-12.png), [Customer](screenshots/dashboard-4-scope-2026-11-12.png). Sales는 `CANCELED` 주문 상태와 2026-11-10~11 주문일 필터, Customer는 같은 주문일 필터를 적용한 화면이다. Product는 전체 기간이다. 세 화면 모두 실제 Serving 결과를 응답 대체 없이 캡처했다.

## Pipeline Operations Status (Pipeline 운영 상태)

사업 Dashboard 3종과 따로 둔 운영 Dashboard다(ID `5`, Collection 6, [ADR 022](../adr/022-run-report-as-second-metabase-source.md)). 사업 Dashboard와 원천이 다르다. `Pipeline Metadata`(PostgreSQL, database ID `3`) 연결로 `pipeline_metadata`의 View 2개만 읽는다. 연결 방법은 [`metabase/README.md`](../../metabase/README.md)에 있다.

| Card | 원천 View | 내용 |
| --- | --- | --- |
| 61 `Last Run (마지막 실행)` | `warehouse_run_report_status` | 선택 상태, 실행 ID, 논리 시각, 판정 기록 시각(UTC), 기록 후 경과, 게시·내보내기·재지정 상태, 판정 당시 Dashboard 최신 여부 |
| 62 `Verdict Codes (판정 코드)` | `warehouse_run_report_status` | 알림 대상 판정과 전체 판정을 한 코드 한 행으로 보인다. 없으면 `판정 없음` |
| 63 `Lag vs Threshold (지연과 임계값)` | 두 View | 주문 데이터 지연, 주문·구독 도메인 갱신 지연, 임계값(초), 연속 보류 횟수 |
| 64 `Recent Runs (최근 실행 이력)` | `warehouse_run_report_status` | 최근 20건의 기록 시각, 판정, 게시 상태, 알림 대상 코드, 전체 판정 코드, 실행 시간 |

Filter `실행 ID`를 비우면 마지막으로 끝난 실행이다. 실행 ID를 넣으면 card 61~63이 그 실행을 보인다. card 64는 Filter와 무관하다.

읽는 법:

- **기준 시각.** 지연, 임계값, 판정 코드, 게시 상태는 Run report를 기록한 시각의 값이다. 조회 시각으로 계산하는 값은 `기록 후 경과` 하나다.
- **선택 상태.** `정상`, `판정 있음`(기록만 남는 판정), `알림 대상`(`domain_publish_alert`가 실패한 실행), `확인 불가 (지난 실행)`, `기록 없음` 가운데 하나다.
- **`확인 불가`의 뜻.** 기록이 그 실행의 임계값(`stale_after_s`)보다 오래됐다는 뜻이다. 실패가 아니라 "마지막 실행 뒤에 무슨 일이 있었는지 모른다"는 표시다. 상시 운영이 아니므로 평소에는 이 상태다. 그 실행의 게시 상태와 판정 코드는 아래 칸과 card 62에 그대로 보인다.
- **칸의 `확인 불가`.** 값이 NULL인 칸이다. 0이나 `정상`으로 바꾸지 않는다. 전체 차단 실행은 도메인 지연이 없어 `확인 불가`다.
- **card 64의 `판정`.** 기록 당시의 `ok`다(`성공`, `판정 있음`). 조회 시각에 따라 바뀌는 선택 상태를 쓰지 않는다.
- **card 60과 어긋날 때.** card 60은 마지막으로 성공한 내보내기 시점의 값이다. 전체 차단이나 내보내기 실패 뒤에는 card 60이 정상이고 운영 Dashboard가 실패 판정을 보인다. 옳은 쪽은 운영 Dashboard다. 행 수 급감(`ROW_COUNT_DROP`)도 운영 Dashboard에만 나타난다.

보이지 않는 것:

- Run report가 남지 않은 실행(`publish_run_summary` 실패, 저장 실패, 시작하지 못한 DagRun, 진행 중인 실행). 직전 기록이 보이고 오래되면 `확인 불가`가 된다.
- PostgreSQL이 멈춘 상태. Metabase 앱 DB가 같은 PostgreSQL이라 Metabase가 열리지 않는다.
- 알림 시각. 따로 저장하지 않는다. `판정 기록 시각`이 가장 가까운 값이다.

2026-11-12 검증: 실제 기록 3건을 실행 ID로 골라 확인했다. [정상 실행](screenshots/ops-status-success-2026-11-12.png)(`manual__2026-11-12T02:20:00+00:00`), [부분 게시](screenshots/ops-status-partial-2026-11-12.png)(`verify_partial_on`, `PUBLISH_PARTIAL:orders`), [전체 차단](screenshots/ops-status-failed-2026-11-12.png)(`verify_partial_off`, `PUBLISH_FAILED`). 세 기록 모두 조회할 때 임계값보다 오래돼 선택 상태는 `확인 불가 (지난 실행)`이다. 그래서 `정상`과 `알림 대상` 선택 상태는 화면이 아니라 View 통합 테스트(`tests/integration/test_run_report_views_integration.py`)로 확인했다. [없는 실행 ID](screenshots/ops-status-no-match-2026-11-12.png)는 `기록 없음`을 보인다. 실제 이력을 지우지 않으려고 빈 Table 대신 쓴 증적이다. `ROW_COUNT_DROP`은 실제 이력에 없어 PostgreSQL 임시 Table에서 card 62·64의 SQL만 실행해 확인했다. [모바일 화면](screenshots/ops-status-mobile-2026-11-12.png)과 최근 이력의 긴 코드는 표 안에서 스크롤한다.

Snapshot은 `metabase/export/card-{61,62,63,64}.json`과 `dashboard-5.json`이다. Card JSON은 연결 ID `3`을 그대로 담는다. 앱 DB를 새로 만들어 연결 ID가 달라지면 Card JSON의 `database` 값을 새 ID로 맞춘다.
