# 012. E2E Fixture Warehouse에 Product·Seller Dimension을 채우고, Publish 연결 Test는 전역 Chain을 전제한다

- 판정: **수정 필요 2건 — 모두 Test 측 수정. 설계·Production Code는 유지한다**
- 관련: Phase 7 구현 계획 Task 8(P7-10)·Task 10(P7-18), `docs/reference/mart-grain.md`, ADR 016
- 기록일: 2026-09-20
- 선행: `011_subscription-payment-status-case.md`

## 1. 지적 1 — E2E Fixture Warehouse에 `dim_product`·`dim_seller`가 비어 있다

**현상.** P7-10에서 `fct_order_item.product_id → dim_product.product_id`, `seller_id → dim_seller.seller_id` relationships Test를 추가했다. `tests/integration/test_order_e2e_and_late_order_mart_integration.py`의 두 테스트가 실패한다. Fixture Warehouse에는 `products`·`sellers` Bronze Object가 없어 두 Dimension이 비었다.

**판정.** Test가 옳고 Fixture가 불완전하다. relationships Test를 완화하지 않는다.

- Fixture Bundle의 Line은 Seed Catalog의 실제 `product_id`·`seller_id`를 참조한다. 그 Parent를 수집하지 않은 Warehouse는 실제 Pipeline이 만드는 Warehouse가 아니다. 실 DAG는 9개 Table을 모두 수집한다.
- 따라서 AC-12(정상 데이터 False Positive 0) 위반이 아니다. Fixture가 만든 부분 Warehouse에서만 나는 실패다. Mart 계약(`mart-grain.md`)은 Line Fact의 Product·Seller 참조를 요구한다.

**지시.** 두 테스트의 수집 Table에 `products`와 `sellers`를 추가한다. 의존 순서를 지킨다.

```python
        for source_table in (
            "customers",
            "customer_subscriptions",
            "customer_membership_tiers",
            "products",
            "sellers",
            "orders",
            "order_items",
            "order_payments",
        ):
```

- `_seed_watermarks`에는 `products`·`sellers` Watermark를 넣지 않는다. Watermark가 없으면 전체 Table을 수집한다. 이 Fixture에서는 그게 목적이다. Docstring의 "6개 Table"을 "8개 Table 중 6개"로 고치고, Product·Seller는 전체 수집임을 한 줄로 남긴다.
- Row 수 단언을 고정 List에서 Table 이름 기준으로 바꾼다. 현재 Seed는 `products` 32,951건, `sellers` 3,095건이다. 숫자를 Test에 박지 않는다.

```python
        counts = {result.run.source_table: result.row_count for result in results}
        assert counts["orders"] == 1
        assert counts["order_items"] == len(bundle.items)
        assert counts["order_payments"] == 1
        assert counts["customers"] == 1
        assert counts["customer_subscriptions"] == 1
        assert counts["customer_membership_tiers"] == 1
        assert counts["products"] > 0
        assert counts["sellers"] > 0
```

- `test_fixed_order_is_traceable_from_source_to_fact`의 Catalog 단언은 그대로 둔다. `results`에 두 Table이 추가되면 Catalog 비교도 자동으로 8개를 비교한다.
- `_cleanup`은 `results`를 순회하므로 추가 Object도 같이 지워진다. 수정하지 않는다.
- 두 테스트의 실행 시간이 늘어난다. Integration 전용이므로 수용한다.

## 2. 지적 2 — `previous_publish_run_id` 전역 조회는 설계대로다

**현상.** `test_full_success_transition_links_previous_published_run`이 실패했다. `start_publish_run`이 011 처리 중 수행된 실제 수동 Publish Run `5a15484a-b831-4535-904d-1d9a6709228b`을 직전 PUBLISHED로 골랐다.

**판정.** Production Code가 옳다. Test의 격리 가정이 틀렸다.

- Published 파일은 `data/warehouse/warehouse.duckdb` 하나다. 직전 PUBLISHED는 그 파일을 마지막으로 쓴 Run이다. `pipeline_name`으로 나누면 수동 Publish와 DAG Publish가 서로 다른 Chain을 만들어 Hash 비교 기준이 깨진다. 전역 조회를 유지한다 (ADR 016 Decision).
- 실패 원인은 Test가 `NOW = datetime(2026, 9, 18)` 고정 시각을 쓰는 것이다. 그보다 뒤에 끝난 실제 PUBLISHED Run이 생기면 Test Row가 최신이 아니다.

**지시.** `tests/integration/test_publish_metadata_integration.py`를 고친다. DB를 분리하지 않는다.

- 기존 PUBLISHED Row의 최대 `finished_at` 이후 시각을 기준으로 Test Run 시각을 잡는다.

```python
def _after_latest_publish(settings: PostgresSettings) -> datetime:
    """실제 Publish Run이 있어도 Test Run이 항상 최신이 되도록 기준 시각을 잡는다."""
    with settings.pipeline_connection() as connection:
        latest = connection.execute(
            "SELECT max(finished_at) FROM mart_publish_runs WHERE status = 'PUBLISHED'"
        ).fetchone()[0]
    return max(latest or NOW, NOW) + timedelta(minutes=1)
```

- `test_full_success_transition_links_previous_published_run`에서 `NOW` 대신 이 기준 시각을 쓴다. `first`는 `base`, `mark_published`는 `base + 1분`, `second`는 `base + 2분`으로 준다.
- 다른 테스트는 최신 여부에 의존하지 않으므로 `NOW`를 그대로 둔다.
- 이 Test는 `previous_publish_run_id`가 "전역 최신 PUBLISHED"라는 계약을 검증한다는 문장을 Docstring에 남긴다.

## 3. 관찰 — phase-07 문서 Checkbox

`docs/phases/phase-07-data-quality-publish.md`의 Task·DoD Checkbox가 대부분 `[ ]`인데 변경 요약 절은 완료로 서술한다. 위 두 수정과 Publish 재실행이 끝난 뒤 한 번에 정리한다.

- 근거가 실제로 통과한 항목만 `[x]`로 바꾼다.
- 통과하지 못한 항목은 `[ ]`로 두고 이유를 남긴다. 임의로 체크하지 않는다.
- Publish Gate 근거에는 실패 Run `f7b2b395-0d47-4acb-9a51-357e2ebe31a9`와 성공 Run ID를 함께 적는다.

## 4. 검증 명령

```bash
uv run pytest tests/test_warehouse_quality_contract.py -v
RUN_POSTGRES_INTEGRATION=1 uv run pytest tests/integration/test_publish_metadata_integration.py -v
RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 \
  uv run pytest tests/integration/test_order_e2e_and_late_order_mart_integration.py -v
```

세 명령이 모두 통과해야 Phase 7 종료 판정을 진행한다.
