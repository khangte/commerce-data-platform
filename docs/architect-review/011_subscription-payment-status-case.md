# 011. `fct_subscription_payment.payment_status`는 소문자 Domain을 유지한다

- 판정: **반려 — Test 완화가 아니라 Test 자체를 원복한다**
- 관련: Phase 7 구현 계획 Task 8, `docs/reference/mart-grain.md` 3장, PRD v1.13 §11 Domain
- 기록일: 2026-09-20
- 대상 Publish Run: `f7b2b395-0d47-4acb-9a51-357e2ebe31a9` (DBT_TEST_ERROR)

## 1. 보고 내용

실제 dbt Publish Gate 실행에서 아래 Node가 1건 실패했다.

```text
accepted_values_fct_subscription_payment_payment_status__COMPLETED__FAILED
```

실패 Build는 `data/warehouse/failed/`로 격리됐고 Published 파일은 교체되지 않았다. Gate는 설계대로 동작했다.

## 2. 판정

데이터 결함이 아니다. Test 선언의 회귀다. `dbt/models/marts/facts/schema.yml`의 다음 변경이 원인이다.

```diff
-                values: [completed, failed]
+                values: [COMPLETED, FAILED]
```

근거:

| 근거 | 내용 |
| ---- | ---- |
| Source 계약 | `sql/source/001_create_source_tables.sql`의 `subscription_payments` CHECK는 `('completed', 'failed')` 소문자다. |
| Mart 계약 | `docs/reference/mart-grain.md` `fct_subscription_payment` 행: `accepted_values: completed, failed`. |
| Model 구현 | `fct_subscription_payment`는 `payment_status`를 Staging에서 그대로 투영한다. `standardized_payment_status` Macro를 거치지 않는다. `int_subscription_payments_enriched.sql`도 `payment_status = 'completed'`로 비교한다. |
| 계획 범위 | Phase 7 계획 Task 8은 이 Model에 `payment_value`의 `non_negative`만 추가하도록 했다. 상태 도메인 대문자화는 `fct_order`와 `fct_order_payment`에만 해당한다. |

대문자 표준화는 주문 계열(`fct_order.order_status`, `fct_order_payment.payment_status`)에만 적용한다. 두 계열은 `standardized_*` Macro를 통과한다. 구독 결제 계열은 Phase 6 계약 그대로 소문자를 유지한다.

격리 Build 실측이 이를 확인한다. `facts.fct_subscription_payment`의 상태 집계는 `[('completed', 1)]`이다. 저장된 값은 소문자이고, 새 Test만 대문자를 요구했다.

## 3. 지시

`dbt/models/marts/facts/schema.yml`의 `fct_subscription_payment.payment_status`를 원복한다.

```yaml
      - name: payment_status
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: [completed, failed]
```

- `payment_value`의 `non_negative` 추가는 유지한다.
- 주문 계열 두 Model의 대문자 `accepted_values`는 유지한다.
- `tests/test_warehouse_quality_contract.py`가 구독 결제 상태를 대문자로 단언하면 소문자로 고친다. 이 Model의 상태 값을 단언하지 않으면 그대로 둔다.
- 원복 뒤 다시 실행한다.

```bash
uv run pytest tests/test_warehouse_quality_contract.py -v
uv run python -m src.warehouse.publish
```

- Publish가 `"status": "PUBLISHED"`로 끝나야 한다.
- 실패 Run `f7b2b395-0d47-4acb-9a51-357e2ebe31a9`의 격리 Build는 근거로 남긴다. 삭제하지 않는다.
- 재실행 뒤 `docs/phases/phase-07-data-quality-publish.md`의 Publish Gate 근거에 두 Run ID(실패·성공)를 모두 적는다.

## 4. 남기는 사실

`fct_subscription_payment` Row가 1건뿐이다. 현재 Bronze에 구독 결제 데이터가 거의 없다는 뜻이다. Domain Test의 실효 Coverage는 낮다. Phase 7 종료 전에 Generator로 구독 결제를 더 만들어 재Publish할지는 lead가 정한다. 이번 판정 범위 밖이다.

Mart 상태 Column의 대소문자는 Model마다 다르다. 계약 기준은 `docs/reference/mart-grain.md`다. 새 `accepted_values`를 붙일 때는 Macro 통과 여부를 먼저 확인한다.
