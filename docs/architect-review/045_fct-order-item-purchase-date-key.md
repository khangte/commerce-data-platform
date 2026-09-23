# 045. `fct_order_item.purchase_date_key` 추가 (Mart Grain 계약 §3.2 개정)

- 날짜: 2026-09-23
- 판정: 승인 (계약 개정). 사용자가 lead를 통해 A안을 승인함
- 계기: P10-06 설계 이탈 판단 요청. Phase 10 계획은 `fct_order_item → dim_date`를 요구하지만 계약 §3.2에는 날짜 FK가 없었음
- 관련: Phase 10 계획 Task 9·Task 11 (`docs/superpowers/plans/2026-09-22-phase10-bi.md`), 계약 §6 Fan-out 방지

## 1. 문제

Product Dashboard(Task 11)의 Category GMV, Top Products, Sales Volume 카드는 Item Grain에서 기간 필터와 기간별 Fan-out 검증이 필요하다. `fct_order_item`에는 날짜 컬럼이 없다. 날짜를 얻는 방법은 `fct_order`와 Join하는 것뿐이고, 이 Join은 §6("Fact끼리는 서로 직접 Join하지 않는다")에 어긋난다.

## 2. 검토한 대안

| 안 | 내용 | 판정 |
| -- | ---- | ---- |
| A | `fct_order_item`에 `purchase_date_key` FK 추가 | 채택 |
| B | Product 카드를 전체 기간 전용으로 축소 | 기각. 기간별 검증과 Date 필터 요구를 충족하지 못함 |
| C | Metabase Question에서 `fct_order`와 Join해 날짜 획득 | 기각. §6 위반. BI 쪽에 Grain 결합 로직이 생김 |

A안은 Line Fact에 Header의 날짜 FK를 복제하는 Kimball 표준 방식이다. 항목 1건은 주문 1건에 속하므로 행 수와 Grain은 바뀌지 않는다.

## 3. 계약 개정 내용

`docs/reference/mart-grain.md` §3.2 `fct_order_item`:

- Dimension 참조에 `dim_date`를 추가한다.
- 컬럼 `purchase_date_key INTEGER`를 추가한다. 종류는 Dimension FK, Null 불가.
- 정의: 항목이 속한 주문의 구매일. `stg_orders.purchase_at`에서 `fct_order.purchase_date_key`와 같은 식으로 파생한다.
- Test: `not_null`, `relationships → dim_date.date_key`, 같은 `order_id`의 `fct_order.purchase_date_key`와 일치.

`fct_order_payment`에는 customer·date FK를 추가하지 않는다. Phase 10에서 결제 Fact를 날짜나 고객으로 자르는 카드가 없다 (YAGNI).

## 4. 구현 규칙

1. 파생 위치는 `int_order_items_enriched`다. `stg_orders`를 `order_id`로 Join해 `purchase_at`을 얻고, `int_order_fact_ready`와 같은 식으로 변환한다:
   `cast(strftime(date_trunc('day', purchase_at), '%Y%m%d') as integer) as purchase_date_key`
2. `int_order_fact_ready`나 `int_orders_enriched`를 참조하지 않는다. `int_order_item_totals → int_order_fact_ready`가 `int_order_items_enriched`에 의존하므로 순환 위험이 있고, `int_orders_enriched`는 불필요한 `dim_customer` 의존을 끌고 온다.
3. `stg_orders`는 주문 1건 1행이므로 Join 후 항목 행 수가 바뀌지 않아야 한다.
4. `fct_order_item.sql`은 `purchase_date_key`를 투영만 한다. 컬럼 순서는 계약 표를 따른다(`seller_id` 다음).
5. 날짜 식이 두 곳에 있으므로 일치 여부를 Singular Test로 강제한다. Macro로 추출하지 않는다.

## 5. 검증 요구

- `schema.yml`: `purchase_date_key`에 `not_null`, `relationships → ref('dim_date').date_key`.
- Singular Test `dbt/tests/fct_order_item_date_matches_order.sql`: `fct_order_item`과 `fct_order`의 `purchase_date_key`가 다른 `order_id`가 0건.
- `tests/test_warehouse_quality_contract.py`에 새 relationships와 Singular Test 존재를 검증하는 단언 추가.
- 적용 전후 `fct_order_item`의 행 수와 `sum(item_price)`가 같다.

## 6. 적용 절차 (2026-09-23 개정)

초판의 `dbt run --full-refresh --select fct_order_item` 절차는 폐기한다. 실행 결과 두 가지 문제가 확인됐다.

- Published 파일의 staging·intermediate View는 그 파일을 만든 Build 파일의 Catalog 이름(`build/<publish_run_id>`)을 참조한다. Published 파일에 dbt를 직접 붙이면 Catalog 이름이 달라 Binder Error가 난다. 단일 모델 선택 실행은 상류 View를 다시 만들지 않으므로 항상 실패한다.
- Published 파일을 직접 고치면 `mart_publish_runs`를 거치지 않는다. 기록된 Hash와 파일이 어긋나고, Publish Gate(dbt test 통과 후 원자 교체)를 우회한다.

규칙:

1. Published 파일에 dbt를 직접 실행하지 않는다. 모든 Mart 변경은 `src.warehouse.publish` 경로(Build 파일 복사, dbt build, Hash 기록, 원자 교체)로만 반영한다.
2. `src.warehouse.publish` CLI에 `--full-refresh` 플래그를 추가한다. 이 플래그는 `run_dbt_build`의 `extra_args`에 `--full-refresh`를 붙인다. `--dbt-vars`와 함께 쓸 수 있어야 한다.
3. 전체 프로젝트를 `--full-refresh`로 build한다. `--select`로 범위를 줄이지 않는다. 선택되지 않은 모델의 View가 이전 Build Catalog를 참조한 채 Publish된다. Dimension은 `table`이라 매번 재생성되고, Fact의 Full Refresh와 Incremental 결과가 같다는 것은 R12 신뢰성 테스트와 `test_incremental_full_refresh_hash_integration`이 이미 보장한다.
4. 실행: `python -m src.warehouse.publish --full-refresh --pipeline-name contract_045_full_refresh`.

## 7. Publish Metadata 영향과 검증

- `mart_publish_runs`에 새 PUBLISHED Row가 생기고 `previous_publish_run_id`는 직전 Run을 가리킨다.
- `changed_relations`는 `fct_order_item` 하나여야 한다. 직전 Publish 이후 새 Bronze Batch가 없다는 전제다. 다른 Relation이 바뀌면 새 Batch 유입 여부를 먼저 확인하고, 유입이 없으면 Full Refresh와 Incremental 불일치로 보고 중단한다.
- `mart_row_counts`의 `fct_order_item` 값은 직전 Run과 같아야 한다.
- 실행 전 Published 파일이 직전 PUBLISHED Run의 기록 Hash와 일치하는지 확인한다. 실패한 직접 실행이 파일을 바꿨을 수 있다. 불일치하면 진행하지 않고 보고한다.
- Publish 후 `RUN_PUBLISHED_MART_CHECK=1`로 `tests/test_published_mart_queryable.py`를 실행한다 (판정 043).

`mart_hash`의 `fct_order_item` Logical Hash는 컬럼이 늘어나 값이 바뀐다. 개정 전 Hash와 비교하는 기존 증거(Phase 9 벤치마크 등)는 재계산하지 않는다. 개정 이후 실행 결과끼리만 비교한다.
