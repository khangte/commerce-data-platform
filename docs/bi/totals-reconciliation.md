# Dashboard 합계 대조

- Publish Run: `c1bfae07-e7e9-4851-bf6b-9e687282a8f3`
- Serving Export: `ae027aa3-c83c-4f61-8b2d-7516d4cedd87`
- Git Commit: 미커밋
- 기준: `data/serving/mart.duckdb`, `metabase/queries/dashboard_*.sql`
- 캡처 시점: 2026-09-29 04:30 UTC Warehouse 실행 `manual__2026-09-29T04:30:00+00:00` 종료 후 Metabase 재시작. `facts.fct_order` 99,541행, 2026-09-29 UTC 주문 28건.

| Dashboard / 카드 | Metabase 결과 | 기준 Query 결과 | Delta |
| --- | ---: | ---: | ---: |
| Sales / GMV | 15,419,773.75 | 15,419,773.75 | 0 |
| Sales / Orders | 99,541 | 99,541 | 0 |
| Sales / AOV | 159.82683876116835 | 159.82683876116835 | 0 |
| Product / Category GMV 합계 | 15,948,449.40 | 15,948,449.40 | 0 |
| Product / Item row count | 112,841 | 112,841 | 0 |
| Customer / New order count | 96,196 | 96,196 | 0 |
| Customer / Repeat order count | 2,384 | 2,384 | 0 |
| Customer / Region order count | 99,541 | 99,541 | 0 |
| Customer / Current tier customers | 96,196 | 96,196 | 0 |
| Customer / Subscription Status Trend 이벤트 날짜 수 | 4 | 4 | 0 |
| Customer / Subscription Status Trend 시작·활성·실패·해지 신청·이탈 | 3 / 3 / 0 / 1 / 0 | 3 / 3 / 0 / 1 / 0 | 0 |

구독 계약 4건과 실패 Profile·재시도 결제 2건을 생성·수집해 `Subscription Status Trend`가 4개 이벤트 날짜를
표시한다. 시작·활성·결제 실패·해지 신청·이탈 합계는 각각 3·3·0·1·0이다. 정규 자동 청구 예정일은
2026-10-04 이후이므로 이번 2026-09-10까지의 구간에는 정규 자동 청구 결제가 없다. Product 차원 Join 전후
`fct_order_item`은 모두 112,841행, `sum(item_price)`는 모두 13,686,837.39,
`sum(line_gross_value)`는 모두 15,948,449.40으로 Fan-out이 없다.
