# Dashboard 합계 대조

- Publish Run: `8890b135-5391-4290-8b05-bf99f24668a6`
- Serving Export: `0616bd30-7c38-4a3c-b03b-aaecb0cc6cec`
- Git Commit: `5e149ef534aca5d39e2461ba211497e3081e0a2f`
- 기준: `data/serving/mart.duckdb`, `metabase/queries/dashboard_*.sql`

| Dashboard / 카드 | Metabase 결과 | 기준 Query 결과 | Delta |
| --- | ---: | ---: | ---: |
| Sales / GMV | 15,419,773.75 | 15,419,773.75 | 0 |
| Sales / Orders | 99,511 | 99,511 | 0 |
| Sales / AOV | 159.82683876116835 | 159.82683876116835 | 0 |
| Product / Category GMV 합계 | 15,927,032.75 | 15,927,032.75 | 0 |
| Product / Item row count | 112,793 | 112,793 | 0 |
| Customer / New order count | 96,166 | 96,166 | 0 |
| Customer / Repeat order count | 2,384 | 2,384 | 0 |
| Customer / Region order count | 99,511 | 99,511 | 0 |
| Customer / Current tier customers | 96,166 | 96,166 | 0 |
| Customer / Subscription Status Trend 이벤트 날짜 수 | 4 | 4 | 0 |
| Customer / Subscription Status Trend 시작·활성·실패·이탈 | 1 / 1 / 1 / 2 | 1 / 1 / 1 / 2 | 0 |

구독 계약 4건과 자동 청구 결제 1건을 생성·수집해 `Subscription Status Trend`가 4개 이벤트 날짜를
표시한다. 시작·활성·결제 실패·이탈 합계는 각각 1·1·1·2다. Product 차원 Join 전후
`fct_order_item`은 모두 112,793행, `sum(item_price)`는 모두 13,667,738.32,
`sum(line_gross_value)`는 모두 15,927,032.75로 Fan-out이 없다.
