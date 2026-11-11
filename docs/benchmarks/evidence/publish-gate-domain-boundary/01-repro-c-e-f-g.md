# 재현 실험 C·E·F·G (실제 dbt Build)

- 수집 시각: 2026-11-11T09:41Z (UTC)
- 환경: HEAD 93f3c7a에 작업 트리 변경(`tests/reliability/faults.py`, `tests/reliability/test_domain_gate_experiment.py`)을 얹었다. Bronze는 로컬 SeaweedFS, Catalog는 Postgres에서 동기화했다.
- 결과 JSON: `data/reliability/gate-domain-split/20261111T094058794989Z.json` (gitignore 대상이어서 아래에 발췌했다)

## 판정: PASS

- C: 스위치 켬 + `publish_gate_canary` 실패(Model 의존 없음, 귀속 불가) → `FAILED`, Run report는 `PUBLISH_FAILED`
- E: 스위치 켬 + `dim_subscription_publish_gate_canary` 실패(상위 Dimension 실패로 하위 건너뜀) → `PUBLISHED`, `held_domains=["subscriptions"]`, 하위 28개 Node 건너뜀, Run report는 `PUBLISH_PARTIAL:subscriptions`
- F: 두 Canary 동시 실패 → 끔·켬 모두 `FAILED`/`DBT_TEST_ERROR`, `held_domains=[]`, 게시본 Hash 불변, `PUBLISH_FAILED`. 이때 오류에 `partial publish fallback`은 없다. 보류 전에 판정이 끝났다는 뜻이다.
- G: 구독 보류 + 되돌린 `customer_key`가 새 `dim_customer`에 없음 → 경계 Test 실패로 전체 차단(`FAILED`/`DBT_TEST_ERROR`, `held_domains=[]`). `failed/`에는 되돌리기 전 파일이 남고, 오류에는 `partial publish fallback`이 들어간다.

## 실행 명령

```bash
cd /home/kang/projects/commerce-data-platform
.venv/bin/python -m pytest -m integration tests/reliability/test_domain_gate_experiment.py -q -s
```

```text
1 passed in 663.94s (0:11:03)
```

## 결과 JSON 발췌 (C·E·F·G)

```json
{
 "c": {"off_status": "FAILED", "on_status": "FAILED", "on_error_type": "DBT_TEST_ERROR",
       "on_held_domains": [], "on_failures": ["PUBLISH_FAILED"]},
 "e": {"off_status": "FAILED", "off_withheld_order_rows": 20,
       "on_status": "PUBLISHED", "on_held_domains": ["subscriptions"], "on_skipped_nodes": 28,
       "on_withheld_order_rows": 0, "on_failures": ["PUBLISH_PARTIAL:subscriptions"]},
 "f": {"failed_nodes": ["test.commerce_data_platform.orders_publish_gate_canary",
                        "test.commerce_data_platform.subscriptions_publish_gate_canary"],
       "off_status": "FAILED", "off_error_type": "DBT_TEST_ERROR",
       "on_status": "FAILED", "on_error_type": "DBT_TEST_ERROR", "on_held_domains": [],
       "on_published_hash_unchanged": true, "on_failures": ["PUBLISH_FAILED"]},
 "g": {"customer_id": "24379aebec3f657fed011e713367ba81", "membership_tier": "BRONZE",
       "replaced_customer_key": "b16e4b48769e623a27bf335c3dce43bc",
       "baseline_payment_rows_on_replaced_key": 3,
       "injected_object_key": "reliability-faults/customer_membership_tiers/ingestion_date=2026-11-11/batch_id=warehouse_pipeline_dag__20261111T061000Z_fault_backdated_tier/data.parquet",
       "control_status": "PUBLISHED", "control_held_domains": [],
       "on_status": "FAILED", "on_error_type": "DBT_TEST_ERROR", "on_held_domains": [],
       "on_failed_nodes": ["test.commerce_data_platform.subscriptions_publish_gate_canary"],
       "on_boundary_node": "test.commerce_data_platform.relationships_fct_subscription_payment_customer_key__customer_key__ref_dim_customer_.1b82cc3bde",
       "on_error_head": "DBT_TEST_ERROR: partial publish fallback: subscriptions: boundary tests failed: test.commerce_data_platform.relationships_fct_subscription_payment_customer_key__customer_key__ref_dim_customer_.1b82cc3bde",
       "on_failed_copy": "be0a3986-7a69-4e98-aba7-2d7f095e4c17.duckdb",
       "on_published_hash_unchanged": true, "on_failures": ["PUBLISH_FAILED"]}
}
```

같은 실행에서 기록한 나머지 값은 다음과 같다.

- 경계 시각: 2026-10-19 11:50:09.506602+00:00
- D·D-orders 불일치 없음
- 기준 Build 시간: 27.37s

## G 조건을 만든 방법

`customer_key`는 `md5(customer_id|valid_from|attribute_hash)`이다. 평소에는 두 가지 이유로 Key가 바뀌지 않는다.

- Cursor 수집이 Watermark 이후 행만 가져온다. 따라서 관측이 뒤에 붙기만 한다.
- 등급은 내려가지 않는다. 이는 `dim_customer_membership_tier_non_decreasing`이 강제한다.

이 때문에 운영 Bronze만으로는 G를 만들 수 없다. 그래서 늦게 도착한 과거 관측을 합성했다.

1. 구독 결제가 있는 고객 중 첫 버전(`effective_from = '-infinity'`)인 고객을 `customer_id` 순으로 고른다.
2. 그 고객의 Bronze 행을 복사한다. `created_at`·`updated_at`만 하루 앞당기고, 등급은 그대로 둔다.
3. 복사한 행을 새 `_batch_id`(`<최대 batch>_fault_backdated_tier`)로 SeaweedFS에 올린다. Catalog 행은 실험 Build 파일에만 넣는다.
4. 결과: `dim_customer` 첫 버전의 `valid_from`이 바뀌어 새 Key가 생긴다. 반면 `subscriptions`를 보류하면 `fct_subscription_payment`는 직전 게시본으로 돌아가 옛 Key(`b16e4b48…`)를 그대로 가리킨다.

주입은 합성이다. 하지만 이후의 dbt Build, 도메인 되돌리기, 경계 Test 재실행, 전체 차단 판정은 운영과 같은 코드 경로로 실행됐다.

## 대조군과 단언

| Attempt | Canary | 결과 | 확인한 값 |
|---|---|---|---|
| g-control | 없음 | `PUBLISHED`, `held_domains=[]` | 새 Build에서 옛 Key 행 수 `(dim 0, payment 0)`. 즉 도메인을 보류하지 않으면 결제도 새 Key로 다시 계산된다 |
| g-on | `subscriptions` | `FAILED`/`DBT_TEST_ERROR`, `held_domains=[]` | `failed/` 복사본의 옛 Key 행 수 `(0, 0)`. 이는 되돌리기 전 상태다. Build 파일은 삭제됐고 게시본 Hash는 불변이다 |
| 기준 게시본 | - | - | 옛 Key 행 수: dim 1, payment 3 |

## 실패 이력

- 첫 전체 실행에서는 g-control이 `dim_customer_membership_tier_non_decreasing` 실패로 `FAILED`가 됐다.
  - 원인: 처음 주입은 GOLD 관측을 과거에 넣었는데, 구독 결제 고객 3명이 모두 BRONZE였다. 그래서 GOLD → BRONZE라는 등급 하락 이력이 생겼다.
  - 조치: 같은 등급을 시각만 앞당겨 넣도록 고쳤고, 재실행(위 결과)이 통과했다.
- stg View로 주입 여부를 확인하려 하면 주입 행이 보이지 않는다. `bronze_source`가 컴파일 시점에 객체 Key 목록을 고정하기 때문이다. 그래서 parquet 행과 Build 파일 Catalog 행을 직접 조회해 확인했다.
