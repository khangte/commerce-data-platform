# Dashboard 합계 대조

- Publish Run: `7472eb21-ef48-4c8d-bd5f-f5c3e6f4c1cf`
- Serving Export: `191cafe0-8ce4-4570-a43d-418209dfd948`
- Git Commit: `c5acea27b8be7f5d160c638cfbde8d9886bf1c2c`
- 기준: `data/serving/mart.duckdb`, `metabase/queries/dashboard_*.sql`

| Dashboard / 카드 | Metabase 결과 | 기준 Query 결과 | Delta |
| --- | ---: | ---: | ---: |
| Sales / GMV | 15,419,773.75 | 15,419,773.75 | 0 |
| Sales / Orders | 99,441 | 99,441 | 0 |
| Sales / AOV | 159.82683876116835 | 159.82683876116835 | 0 |
| Product / Category GMV 합계 | 15,843,553.24 | 15,843,553.24 | 0 |
| Product / Item row count | 112,650 | 112,650 | 0 |
| Customer / New order count | 97,026 | 97,026 | 0 |
| Customer / Repeat order count | 2,415 | 2,415 | 0 |
| Customer / Region order count | 99,441 | 99,441 | 0 |
| Customer / Current tier customers | 96,096 | 96,096 | 0 |

구독 원본은 0행이므로 `Subscription Status Trend`도 0행이다. Product 차원 Join 전후 `fct_order_item`은 모두 112,650행, `sum(item_price)`는 모두 13,591,643.70, `sum(line_gross_value)`는 모두 15,843,553.24로 Fan-out이 없다.
