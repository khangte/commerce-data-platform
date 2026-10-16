# 지표 사전

> 기준: [Mart Grain 계약](mart-grain.md)의 Measure 계약. Metabase는 아래 값을 다시 정의하지 않고 지정한 Fact·Report Model에서 집계한다.

| 지표 | 계산식 | 원본 Model·Grain | 가산성 | 계약 근거 |
| --- | --- | --- | --- | --- |
| GMV | 배송 완료 주문의 `sum(gross_order_value)` | `fct_order`, 주문 1건 | 가산 | §3.1 `gross_order_value` |
| Orders | `sum(order_count)` | `fct_order`, 주문 1건 | 가산 | §3.1 `order_count` |
| AOV | 배송 완료 GMV ÷ 배송 완료 `sum(order_count)` | `fct_order`, 주문 1건 | 비가산 비율 | §3.1 `gross_order_value`, `order_count` |
| 카테고리 GMV | 배송 완료(`order_status = 'DELIVERED'`) 항목의 카테고리별 `sum(line_gross_value)` | `fct_order_item`, 주문 상품 항목 1건 | 가산 | §3.2 `line_gross_value` |
| 상품 판매 수량 | 배송 완료(`order_status = 'DELIVERED'`) 항목의 상품별 행 수 | `fct_order_item`, 주문 상품 항목 1건 | 가산 | §3.2 Grain |
| 고객 수(주문 시점 거래 실적 등급) | `rpt_membership_tier_performance.customer_count`를 등급별로 그대로 조회 | `rpt_membership_tier_performance`, 거래 실적 등급 1건 | 비가산 | §4.1.3 `customer_count` |

`payment_total`과 `payment_value`는 결제 금액이며 GMV가 아니다. `carrier_handoff_days`, `delivery_days`, `delivery_delay_days`, `is_late`는 비가산 값으로 Metabase에서 상세 전용으로 표시한다. 모든 날짜 Filter와 `*_date_key`는 UTC 기준이다. `fct_order_payment.purchase_date_key`는 결제일이 아니라 결제가 속한 주문의 구매일이므로, 결제 상태(`payment_status`)별 기간 집계는 주문 구매일 기준이다(§3.3).
