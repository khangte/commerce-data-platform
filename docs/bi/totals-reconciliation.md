# Dashboard 합계 대조

## 2026-10-16 주문 항목 상태·배송 완료 상품 지표 재대조

- 입력: `data/warehouse/warehouse.duckdb`의 실제 Bronze 목록을 유지한 복사본 `/tmp/item-status-check/warehouse.duckdb`.
- 방식: 복사본에서 `dbt build --full-refresh`를 실행한 뒤 수정한 card 49~51의 SQL과 `metabase/queries/dashboard_product.sql`을 같은 데이터에 실행했다. 카드의 선택 날짜 필터는 적용하지 않았다.
- 범위: 아래 수치는 복사본 SQL 재실행 결과다. 운영 Metabase API 결과나 새 Serving Export·화면 캡처가 아니며, 이전 운영 화면 대조는 아래에 보존한다.
- 결과: dbt 품질 테스트 143개 통과, 상태 불일치 0건, 주문 99,729행, 항목 113,215행, 배송 완료 항목 110,707행.

| 대조 대상 | 카드 SQL 결과 | 기준 SQL 결과 | Delta |
| --- | ---: | ---: | ---: |
| Product / Category GMV 합계 (card 49) | 15,696,682.34 | 15,696,682.34 | 0 |
| Sales GMV와 Category GMV 합계 | 15,696,682.34 | 15,696,682.34 | 0 |
| Product / 상품별 GMV 상위 10개 (card 50) | 10행 | 기준 Query와 모든 행 동일 | 0 |
| Product / 배송 완료 판매 수량 상위 10개 (card 51) | 10행 | 기준 Query와 모든 행 동일 | 0 |
| Product / 전체 항목 행 수 (상태 필터 전) | 113,215 | 113,215 | 0 |

새 불변식: 동일한 기간에서 Category GMV 합계는 배송 완료 주문의 Sales GMV와 같아야 한다.
상품별 판매 수량도 `order_status = 'DELIVERED'`인 항목 행만 센다.
상품·날짜 차원 결합 전후 전체 항목은 113,215행, `sum(item_price)`는 13,869,355.09,
`sum(line_gross_value)`는 16,150,129.06으로 같아 행 수와 금액이 늘어나지 않는다.
`tests/serving/test_product_status_metrics.py`는 취소·진행 주문을 섞은 데이터로 두 Query 파일과 카드 49~51을 실제 실행해 배송 완료 범위와 주문·카테고리 합계를 검증한다.

빌드·대조 근거는 `/tmp/item-status-check/build-result.json`, `summary.json`, `target/run_results.json`이다.
`MART_HASH_TARGETS`(논리 해시 비교 대상)는 9개를 유지한다. 운영 대비 해시 변화는 주문 항목 Fact의 새 상태 컬럼과,
선행 구독 결제 상태 표준화 구현이 전체 재계산에 반영된 구독 결제 Fact다. 후자는 기존 5행의 `payment_status`를 대문자로 바꾸면 모든 컬럼이 복사본과 일치함을 양방향 `EXCEPT ALL`로 확인했다.
운영 Warehouse는 전후 SHA-256이 같고 Serving 파일도 수정하지 않았다.

`on_schema_change`(증분 실행의 스키마 변경 정책)를 설정하지 않았으므로 운영 반영에는 사용자가
`uv run python -m src.warehouse.publish --full-refresh`를 실행해야 한다. 이번 작업에서는 운영 Publish를 실행하지 않았다.

## 이전 운영 화면 대조 (2026-09-29)

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
