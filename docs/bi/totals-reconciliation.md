# Dashboard 합계 대조

- Publish Run: `cef6b673-dc20-49bd-8f40-d615b2146796`
- Serving Export: `a9ffd423-4e3d-4e20-9362-c2f4d6fb2679`
- Git Commit: 048 복구 작업 중(미커밋)
- 기준: `data/serving/mart.duckdb`, `metabase/queries/dashboard_*.sql`

| Dashboard / 카드 | Metabase 결과 | 기준 Query 결과 | Delta |
| --- | ---: | ---: | ---: |
| Sales / GMV | 15,419,773.75 | 15,419,773.75 | 0 |
| Sales / Orders | 99,511 | 99,511 | 0 |
| Sales / AOV | 159.82683876116835 | 159.82683876116835 | 0 |
| Product / Category GMV 합계 | 15,919,976.99 | 15,919,976.99 | 0 |
| Product / Item row count | 112,785 | 112,785 | 0 |
| Customer / New order count | 96,166 | 96,166 | 0 |
| Customer / Repeat order count | 2,384 | 2,384 | 0 |
| Customer / Region order count | 99,511 | 99,511 | 0 |
| Customer / Current tier customers | 96,166 | 96,166 | 0 |
| Customer / Subscription Status Trend 이벤트 날짜 수 | 4 | 4 | 0 |
| Customer / Subscription Status Trend 시작·활성·실패·해지 신청·이탈 | 3 / 3 / 0 / 1 / 0 | 3 / 3 / 0 / 1 / 0 | 0 |

구독 계약 4건과 실패 Profile·재시도 결제 2건을 생성·수집해 `Subscription Status Trend`가 4개 이벤트 날짜를
표시한다. 시작·활성·결제 실패·해지 신청·이탈 합계는 각각 3·3·0·1·0이다. 정규 자동 청구 예정일은
2026-10-04 이후이므로 이번 2026-09-10까지의 구간에는 정규 자동 청구 결제가 없다. Product 차원 Join 전후
`fct_order_item`은 모두 112,785행, `sum(item_price)`는 모두 13,661,196.11,
`sum(line_gross_value)`는 모두 15,919,976.99로 Fan-out이 없다.
