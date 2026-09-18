# Phase 6 Affected Key와 Incremental 재계산 Implementation Plan

> **For agentic workers:** 이 계획은 Task 순서대로 구현한다. Step은 Checkbox(`- [ ]`)로 추적한다. 각 Task는 마지막 Step의 Commit까지 끝내고 다음 Task로 넘어간다.

**Goal:** Late Arrival이 바꾸는 Key만 다시 계산하도록 영향 Key 모델과 Fact Incremental 필터를 구현한다.

**Architecture:** 처리 Watermark로 재계산 경계를 정하고, 도메인별 영향 Key Intermediate 두 개(`int_affected_order_keys`, `int_affected_subscription_payment_keys`)가 그 경계 이후 변경으로 영향받는 Key를 산출한다. Fact는 그 모델을 `is_incremental()` 필터로 직접 참조해 `delete+insert`한다. `control.affected_keys`는 감사 기록으로만 남는다.

**Tech Stack:** dbt-core 1.12.3, dbt-duckdb 1.11.0, DuckDB 1.5.5, Python 3.12, pytest 9.1.1

**Spec:** `docs/architecture/06-late-arrival-affected-keys-and-incremental.md`

## Global Constraints

- 모든 Class·Function 선언 바로 아래에 한국어 Docstring을 쓴다. SQL 모델은 첫 줄에 한국어 주석으로 Grain 문장을 쓴다.
- 한국어 문자열은 UTF-8 그대로 쓴다. `\uXXXX` Escape를 쓰지 않는다.
- `dbt` 실행은 항상 `--project-dir dbt --profiles-dir dbt`를 붙인다.
- Intermediate/Mart는 Raw Source Prefix를 참조하지 않는다. 입력은 Staging이다.
- Mart Grain 계약(`docs/reference/mart-grain.md`)의 Grain·Unique Key 문장은 바꾸지 않는다. 예외는 Task 2의 Materialization 표기뿐이다.
- Dimension은 `table`이다. Dimension을 Incremental로 바꾸지 않는다.
- 작업 브랜치는 `feature/phase6-remaining`이다.
- Commit 메시지는 Conventional Commits 형식으로 쓴다.

---

## File Structure

| 경로 | 책임 |
| ---- | ---- |
| `dbt/macros/processed_batch_watermark.sql` | 재계산 경계(Watermark) 생성·조회·전진 |
| `dbt/macros/record_affected_keys.sql` | 영향 Key를 `control.affected_keys`에 감사 기록 |
| `dbt/models/intermediate/int_affected_order_keys.sql` | 주문 축 영향 Key 산출 |
| `dbt/models/intermediate/int_affected_subscription_payment_keys.sql` | 구독 결제 축 영향 Key 산출 |
| `dbt/models/marts/facts/*.sql` | 영향 Key 필터와 교체 단위 설정 |
| `dbt/tests/int_affected_*.sql` | 영향 Key 모델의 Grain·참조 무결성 검증 |
| `tests/test_batch_identity_ordering.py` | `_batch_id` 사전식 비교 가정 고정 |
| `tests/test_dbt_watermark_macro.py` | Watermark 매크로 경계 동작 검증 |
| `tests/integration/test_subscription_payment_temporal_join_integration.py` | 구독 축 Late Arrival·다중 Batch E2E 검증 |

---

### Task 1: 처리 Watermark 매크로와 Hook

**Files:**
- Create: `dbt/macros/processed_batch_watermark.sql`
- Create: `tests/test_batch_identity_ordering.py`
- Create: `tests/test_dbt_watermark_macro.py`
- Modify: `dbt/dbt_project.yml`

**Interfaces:**
- Consumes: 없음
- Produces: `ensure_processed_batch_watermark()`, `processed_batch_lower_bound()`, `advance_processed_batch_watermark()`, `batch_id_source_models()`. 이후 Task는 모델 SQL에서 `{{ processed_batch_lower_bound() }}`를 문자열 비교 대상으로 쓴다. 이 매크로는 `(select ...)` 형태의 Scalar Subquery SQL을 반환한다.

- [ ] **Step 1: `_batch_id` 정렬 가정을 고정하는 실패 Test 작성**

`tests/test_batch_identity_ordering.py`:

```python
"""Watermark 비교가 의존하는 Batch ID 정렬 가정을 고정한다."""

from __future__ import annotations

from datetime import UTC, datetime

from src.ingestion.batch import BatchIdentity


def test_batch_id_lexicographic_order_matches_logical_date_order() -> None:
    """같은 dag_id에서 Batch ID의 사전식 순서가 Logical Date 순서와 일치한다."""
    earlier = BatchIdentity(
        dag_id="warehouse_pipeline", logical_date=datetime(2026, 9, 9, 3, 0, tzinfo=UTC)
    )
    later = BatchIdentity(
        dag_id="warehouse_pipeline", logical_date=datetime(2026, 9, 10, 3, 0, tzinfo=UTC)
    )

    assert earlier.batch_id < later.batch_id


def test_batch_id_order_holds_across_a_year_boundary() -> None:
    """연도 경계에서도 사전식 비교가 시간 순서를 뒤집지 않는다."""
    december = BatchIdentity(
        dag_id="warehouse_pipeline", logical_date=datetime(2026, 12, 31, 23, 0, tzinfo=UTC)
    )
    january = BatchIdentity(
        dag_id="warehouse_pipeline", logical_date=datetime(2027, 1, 1, 0, 0, tzinfo=UTC)
    )

    assert december.batch_id < january.batch_id
```

- [ ] **Step 2: Test 실행해 통과를 확인**

Run: `uv run pytest tests/test_batch_identity_ordering.py -v`
Expected: PASS. 이 Test는 기존 동작을 고정하는 회귀 방어용이다. 실패하면 `src/ingestion/batch.py`의 `batch_id` 형식이 바뀐 것이므로 Task를 중단하고 아키텍트에게 보고한다.

- [ ] **Step 3: Watermark 매크로 작성**

`dbt/macros/processed_batch_watermark.sql`:

```sql
{% macro ensure_processed_batch_watermark() -%}
    {#- 재계산 경계 Table을 Build 시작 시점에 보장한다. -#}
    {%- if execute -%}
        {%- do run_query("CREATE SCHEMA IF NOT EXISTS control") -%}
        {%- do run_query("
            CREATE TABLE IF NOT EXISTS control.dbt_processed_batch (
                processed_batch_id VARCHAR NOT NULL,
                invocation_id VARCHAR NOT NULL,
                processed_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
            )
        ") -%}
    {%- endif -%}
{%- endmacro %}


{% macro processed_batch_lower_bound() -%}
    {#- 이번 Build가 재계산해야 하는 _batch_id의 하한을 Scalar Subquery로 돌려준다. -#}
    (select coalesce(max(processed_batch_id), '') from control.dbt_processed_batch)
{%- endmacro %}


{% macro batch_id_source_models() -%}
    {#- _batch_id를 노출하는 Staging Model 목록을 돌려준다. -#}
    {{ return([
        'stg_orders',
        'stg_order_items',
        'stg_payments',
        'stg_products',
        'stg_sellers',
        'stg_customer_tier_observations',
        'stg_customer_subscription_observations',
        'stg_subscription_payments',
    ]) }}
{%- endmacro %}


{% macro advance_processed_batch_watermark() -%}
    {#- 모든 Fact를 함께 Build한 경우에만 재계산 경계를 전진시킨다. -#}
    {%- if execute -%}
        {%- set required_facts = [
            'model.commerce_data_platform.fact_orders',
            'model.commerce_data_platform.fact_order_items',
            'model.commerce_data_platform.fact_payments',
            'model.commerce_data_platform.fact_subscription_payments',
        ] -%}
        {%- set selected = selected_resources | list -%}
        {%- set missing = required_facts | reject('in', selected) | list -%}
        {%- if missing | length > 0 -%}
            {%- do log("Skipping watermark advance: facts not in this selection " ~ missing, info=true) -%}
        {%- else -%}
            {%- set selects = [] -%}
            {%- for model_name in batch_id_source_models() -%}
                {%- do selects.append("select max(_batch_id) as batch_id from " ~ ref(model_name)) -%}
            {%- endfor -%}
            {%- set advance_sql -%}
                insert into control.dbt_processed_batch (processed_batch_id, invocation_id)
                select max(batch_id), '{{ invocation_id }}'
                from ({{ selects | join(' union all ') }})
                where batch_id is not null
                having max(batch_id) is not null
            {%- endset -%}
            {%- do run_query(advance_sql) -%}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
```

- [ ] **Step 4: `dbt_project.yml`에 Hook 등록**

`dbt/dbt_project.yml`의 `on-run-end` 블록을 아래로 교체한다. `-- depends_on:` 주석은 dbt가 Hook 안의 `ref()`를 Graph에서 해석하게 하려고 필요하다.

```yaml
on-run-start:
  - "{{ ensure_processed_batch_watermark() }}"

on-run-end:
  - "-- depends_on: {{ ref('int_affected_business_dates') }}\n{{ record_affected_keys() }}"
  - "-- depends_on: {{ ref('stg_orders') }}\n-- depends_on: {{ ref('stg_order_items') }}\n-- depends_on: {{ ref('stg_payments') }}\n-- depends_on: {{ ref('stg_products') }}\n-- depends_on: {{ ref('stg_sellers') }}\n-- depends_on: {{ ref('stg_customer_tier_observations') }}\n-- depends_on: {{ ref('stg_customer_subscription_observations') }}\n-- depends_on: {{ ref('stg_subscription_payments') }}\n{{ advance_processed_batch_watermark() }}"
```

`record_affected_keys()` Hook의 `ref()`는 Task 4에서 새 모델 이름으로 바꾼다. 지금은 기존 이름을 그대로 둔다.

- [ ] **Step 5: Watermark 매크로 Test 작성**

`tests/test_dbt_watermark_macro.py`:

```python
"""재계산 경계 Watermark 매크로의 생성과 조회 경계를 검증한다."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_watermark_table_is_created_and_starts_empty(tmp_path) -> None:
    """Watermark Table이 없으면 만들고, 비어 있으면 하한이 빈 문자열이 된다."""
    warehouse_path = tmp_path / "warehouse.duckdb"

    result = _run_operation(warehouse_path, "ensure_processed_batch_watermark")

    assert result.returncode == 0, _combined_output(result)

    with duckdb.connect(str(warehouse_path)) as connection:
        rows = connection.execute(
            "SELECT coalesce(max(processed_batch_id), '') FROM control.dbt_processed_batch"
        ).fetchall()

    assert rows == [("",)]


def test_watermark_table_creation_is_idempotent(tmp_path) -> None:
    """이미 기록이 있는 Watermark Table은 다시 만들어도 값을 잃지 않는다."""
    warehouse_path = tmp_path / "warehouse.duckdb"
    assert _run_operation(warehouse_path, "ensure_processed_batch_watermark").returncode == 0
    with duckdb.connect(str(warehouse_path)) as connection:
        connection.execute(
            "INSERT INTO control.dbt_processed_batch (processed_batch_id, invocation_id)"
            " VALUES ('warehouse_pipeline__20260909T030000Z', 'invocation-1')"
        )

    result = _run_operation(warehouse_path, "ensure_processed_batch_watermark")

    assert result.returncode == 0, _combined_output(result)
    with duckdb.connect(str(warehouse_path)) as connection:
        rows = connection.execute(
            "SELECT max(processed_batch_id) FROM control.dbt_processed_batch"
        ).fetchall()

    assert rows == [("warehouse_pipeline__20260909T030000Z",)]


def _run_operation(warehouse_path: Path, operation: str) -> subprocess.CompletedProcess[str]:
    """격리된 Warehouse에 dbt Operation 하나를 실행한다."""
    environment = {
        **os.environ,
        "WAREHOUSE_PATH": str(warehouse_path),
        "SEAWEEDFS_BUCKET": "test-bucket",
        "SEAWEEDFS_ACCESS_KEY": "test-access-key",
        "SEAWEEDFS_SECRET_KEY": "test-secret-key",
    }
    return subprocess.run(
        [
            str(Path(sys.executable).with_name("dbt")),
            "run-operation",
            operation,
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt",
        ],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


def _combined_output(result: subprocess.CompletedProcess[str]) -> str:
    """dbt 버전에 따라 달라지는 표준 출력·오류 출력을 함께 비교한다."""
    return f"{result.stdout}\n{result.stderr}"
```

- [ ] **Step 6: Test 실행**

Run: `uv run pytest tests/test_dbt_watermark_macro.py -v`
Expected: PASS 2건. 실패 시 `dbt run-operation` 출력의 첫 에러 줄을 확인한다.

- [ ] **Step 7: Commit**

```bash
git add dbt/macros/processed_batch_watermark.sql dbt/dbt_project.yml tests/test_batch_identity_ordering.py tests/test_dbt_watermark_macro.py
git commit -m "feat: add dbt processed batch watermark"
```

---

### Task 2: Dimension Materialization 계약 정정

**Files:**
- Modify: `dbt/models/marts/dimensions/dim_subscription.sql:1`
- Modify: `dbt/models/marts/dimensions/dim_customer.sql`
- Modify: `docs/reference/mart-grain.md`
- Test: `tests/test_dimension_materialization_contract.py` (Create)

**Interfaces:**
- Consumes: 없음
- Produces: Dimension은 `table`이라는 고정. 이후 Task는 Dimension에 Incremental 설정을 추가하지 않는다.

- [ ] **Step 1: 실패 Test 작성**

`tests/test_dimension_materialization_contract.py`:

```python
"""Dimension이 Table Materialization 계약을 지키는지 검증한다."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DIMENSION_MODELS = (
    "dbt/models/marts/dimensions/dim_customer.sql",
    "dbt/models/marts/dimensions/dim_subscription.sql",
    "dbt/models/marts/dimensions/dim_date.sql",
    "dbt/models/marts/dimensions/dim_product.sql",
    "dbt/models/marts/dimensions/dim_seller.sql",
)


def test_dimension_models_declare_no_incremental_config() -> None:
    """Dimension은 Table이므로 무효한 Incremental 설정을 두지 않는다."""
    for relative_path in DIMENSION_MODELS:
        sql = (PROJECT_ROOT / relative_path).read_text().lower()

        assert "incremental_strategy" not in sql, relative_path
        assert "unique_key" not in sql, relative_path


def test_mart_grain_contract_declares_dimensions_as_table() -> None:
    """Mart Grain 계약이 Dimension Materialization을 table로 적는다."""
    contract = (PROJECT_ROOT / "docs/reference/mart-grain.md").read_text()

    assert "Materialization: incremental\n" not in contract.split("## 3. Fact")[0]
```

- [ ] **Step 2: Test 실행해 실패 확인**

Run: `uv run pytest tests/test_dimension_materialization_contract.py -v`
Expected: FAIL. `dim_subscription.sql`에 `incremental_strategy`가 있고 계약 2.5절이 `incremental`로 적혀 있다.

- [ ] **Step 3: Dimension 모델에서 무효 설정 제거**

`dbt/models/marts/dimensions/dim_subscription.sql`의 첫 줄 `{{ config(unique_key='subscription_key', incremental_strategy='delete+insert') }}`와 그 다음 빈 줄을 지운다. 파일은 다음 줄로 시작한다.

```sql
-- 한 행은 구독 계약 상태 버전 1건이다. Materialization은 dbt_project.yml의 table을 따른다.
select
    md5(concat(subscription_id, '|', cast(valid_from as varchar), '|', attribute_hash)) as subscription_key,
```

`dim_customer.sql`에는 `config` 블록이 없다. 첫 줄에 Grain 주석만 추가한다.

```sql
-- 한 행은 고객 거래 실적 등급 버전 1건이다. Materialization은 dbt_project.yml의 table을 따른다.
select
```

- [ ] **Step 4: Mart Grain 계약 문서 수정**

`docs/reference/mart-grain.md`에서 Dimension 절(2장)의 `- Materialization: incremental` 줄을 모두 `- Materialization: table`로 바꾼다. Fact 절(3장)의 표기는 건드리지 않는다.

Run: `grep -n "Materialization" docs/reference/mart-grain.md`
Expected: 2장 항목은 모두 `table`, 3장 Fact 항목은 `incremental`.

- [ ] **Step 5: Test 실행해 통과 확인**

Run: `uv run pytest tests/test_dimension_materialization_contract.py -v`
Expected: PASS 2건.

- [ ] **Step 6: Commit**

```bash
git add dbt/models/marts/dimensions/dim_subscription.sql dbt/models/marts/dimensions/dim_customer.sql docs/reference/mart-grain.md tests/test_dimension_materialization_contract.py
git commit -m "fix: declare dimensions as table materialization"
```

---

### Task 3: 주문 축 영향 Key 모델 수정과 개명

**Files:**
- Create: `dbt/models/intermediate/int_affected_order_keys.sql` (기존 `int_affected_business_dates.sql`을 `git mv`)
- Delete: `dbt/models/intermediate/int_affected_business_dates.sql`
- Create: `dbt/tests/int_affected_order_keys_unique.sql`
- Modify: `dbt/dbt_project.yml` (Hook의 `ref` 이름)
- Modify: `dbt/macros/record_affected_keys.sql` (`ref` 이름만, 스키마는 Task 5)

**Interfaces:**
- Consumes: Task 1의 `processed_batch_lower_bound()`
- Produces: `int_affected_order_keys(order_id VARCHAR, business_date_key INTEGER)`. Task 6의 `fact_orders`, `fact_order_items`, `fact_payments`가 `order_id`로 필터한다.

- [ ] **Step 1: 모델 파일 이름 변경**

```bash
git mv dbt/models/intermediate/int_affected_business_dates.sql dbt/models/intermediate/int_affected_order_keys.sql
```

- [ ] **Step 2: 모델 내용 교체**

`dbt/models/intermediate/int_affected_order_keys.sql` 전체를 아래로 바꾼다. 구독 관측 참조를 제거하고, 고객 경계를 `customer_id`별로 좁히고, Batch 경계를 Watermark로 바꾼 것이 변경점이다.

```sql
-- 한 행은 주문 축 Fact가 다시 계산해야 하는 (주문, 구매일) 1건이다.
{% set lower_bound = processed_batch_lower_bound() %}

with tier_axis_changes as (
    -- 이번 경계에서 등급 관측이 바뀐 고객과 그 최소 변경 시각.
    select
        customer_id,
        min(updated_at) as changed_from
    from {{ ref('stg_customer_tier_observations') }}
    where _batch_id > {{ lower_bound }}
    group by customer_id
),
affected_orders as (
    select order_id, purchase_at
    from {{ ref('stg_orders') }}
    where _batch_id > {{ lower_bound }}
),
affected_from_items as (
    select orders.order_id, orders.purchase_at
    from {{ ref('stg_order_items') }} as order_items
    inner join {{ ref('stg_orders') }} as orders
        on order_items.order_id = orders.order_id
    where order_items._batch_id > {{ lower_bound }}
),
affected_from_payments as (
    select orders.order_id, orders.purchase_at
    from {{ ref('stg_payments') }} as payments
    inner join {{ ref('stg_orders') }} as orders
        on payments.order_id = orders.order_id
    where payments._batch_id > {{ lower_bound }}
),
affected_from_customers as (
    select orders.order_id, orders.purchase_at
    from {{ ref('stg_orders') }} as orders
    inner join tier_axis_changes
        on orders.customer_id = tier_axis_changes.customer_id
        and orders.purchase_at >= tier_axis_changes.changed_from
),
affected_from_products as (
    select orders.order_id, orders.purchase_at
    from {{ ref('stg_products') }} as products
    inner join {{ ref('stg_order_items') }} as order_items
        on products.product_id = order_items.product_id
    inner join {{ ref('stg_orders') }} as orders
        on order_items.order_id = orders.order_id
    where products._batch_id > {{ lower_bound }}
),
affected_from_sellers as (
    select orders.order_id, orders.purchase_at
    from {{ ref('stg_sellers') }} as sellers
    inner join {{ ref('stg_order_items') }} as order_items
        on sellers.seller_id = order_items.seller_id
    inner join {{ ref('stg_orders') }} as orders
        on order_items.order_id = orders.order_id
    where sellers._batch_id > {{ lower_bound }}
),
unioned as (
    select * from affected_orders
    union
    select * from affected_from_items
    union
    select * from affected_from_payments
    union
    select * from affected_from_customers
    union
    select * from affected_from_products
    union
    select * from affected_from_sellers
)
select
    order_id,
    cast(strftime(date_trunc('day', purchase_at), '%Y%m%d') as integer) as business_date_key
from unioned
```

- [ ] **Step 3: 이름 참조 두 곳 갱신**

`dbt/macros/record_affected_keys.sql`의 `ref('int_affected_business_dates')`를 `ref('int_affected_order_keys')`로 바꾼다.
`dbt/dbt_project.yml`의 `on-run-end` 첫 항목 `-- depends_on: {{ ref('int_affected_business_dates') }}`도 같은 이름으로 바꾼다.

Run: `grep -rn "int_affected_business_dates" dbt/ tests/ docs/`
Expected: `docs/phases/phase-06-dimensional-modeling.md`의 과거 기록 외에는 결과 없음. 남은 코드 참조가 있으면 모두 바꾼다.

- [ ] **Step 4: Grain Test 추가**

`dbt/tests/int_affected_order_keys_unique.sql`:

```sql
-- 주문 축 영향 Key는 (order_id, business_date_key)로 유일하다.
select
    order_id,
    business_date_key,
    count(*) as row_count
from {{ ref('int_affected_order_keys') }}
group by order_id, business_date_key
having count(*) > 1
```

- [ ] **Step 5: 구독 관측 참조가 사라졌는지 확인**

Run: `grep -n "subscription" dbt/models/intermediate/int_affected_order_keys.sql`
Expected: 출력 없음.

- [ ] **Step 6: Commit**

```bash
git add dbt/models/intermediate/int_affected_order_keys.sql dbt/tests/int_affected_order_keys_unique.sql dbt/macros/record_affected_keys.sql dbt/dbt_project.yml
git commit -m "fix: scope order affected keys to tier axis and watermark"
```

---

### Task 4: 구독 결제 축 영향 Key 모델

**Files:**
- Create: `dbt/models/intermediate/int_affected_subscription_payment_keys.sql`
- Create: `dbt/tests/int_affected_subscription_payment_keys_unique.sql`
- Create: `dbt/tests/int_affected_subscription_payment_keys_resolve.sql`

**Interfaces:**
- Consumes: Task 1의 `processed_batch_lower_bound()`
- Produces: `int_affected_subscription_payment_keys(payment_id VARCHAR, business_date_key INTEGER)`. Task 6의 `fact_subscription_payments`가 `payment_id`로 필터한다.

- [ ] **Step 1: 모델 작성**

`dbt/models/intermediate/int_affected_subscription_payment_keys.sql`:

```sql
-- 한 행은 구독 결제 Fact가 다시 계산해야 하는 (결제 시도, 결제일) 1건이다.
{% set lower_bound = processed_batch_lower_bound() %}

with subscription_axis_changes as (
    -- 이번 경계에서 계약 관측이 바뀐 계약과 그 최소 변경 시각.
    select
        subscription_id,
        min(updated_at) as changed_from
    from {{ ref('stg_customer_subscription_observations') }}
    where _batch_id > {{ lower_bound }}
    group by subscription_id
),
tier_axis_changes as (
    -- 이번 경계에서 등급 관측이 바뀐 고객과 그 최소 변경 시각.
    select
        customer_id,
        min(updated_at) as changed_from
    from {{ ref('stg_customer_tier_observations') }}
    where _batch_id > {{ lower_bound }}
    group by customer_id
),
affected_payments as (
    select payment_id, payment_at
    from {{ ref('stg_subscription_payments') }}
    where _batch_id > {{ lower_bound }}
),
affected_from_subscription_versions as (
    select payments.payment_id, payments.payment_at
    from {{ ref('stg_subscription_payments') }} as payments
    inner join subscription_axis_changes
        on payments.subscription_id = subscription_axis_changes.subscription_id
        and payments.payment_at >= subscription_axis_changes.changed_from
),
affected_from_customer_versions as (
    select payments.payment_id, payments.payment_at
    from {{ ref('stg_subscription_payments') }} as payments
    inner join tier_axis_changes
        on payments.customer_id = tier_axis_changes.customer_id
        and payments.payment_at >= tier_axis_changes.changed_from
),
unioned as (
    select * from affected_payments
    union
    select * from affected_from_subscription_versions
    union
    select * from affected_from_customer_versions
)
select
    payment_id,
    cast(strftime(date_trunc('day', payment_at), '%Y%m%d') as integer) as business_date_key
from unioned
```

- [ ] **Step 2: Grain Test 작성**

`dbt/tests/int_affected_subscription_payment_keys_unique.sql`:

```sql
-- 구독 결제 축 영향 Key는 (payment_id, business_date_key)로 유일하다.
select
    payment_id,
    business_date_key,
    count(*) as row_count
from {{ ref('int_affected_subscription_payment_keys') }}
group by payment_id, business_date_key
having count(*) > 1
```

- [ ] **Step 3: 참조 무결성 Test 작성**

`dbt/tests/int_affected_subscription_payment_keys_resolve.sql`:

```sql
-- 영향 Key는 Staging에 실재하는 결제 시도만 가리킨다.
select affected.payment_id
from {{ ref('int_affected_subscription_payment_keys') }} as affected
left join {{ ref('stg_subscription_payments') }} as payments
    on affected.payment_id = payments.payment_id
where payments.payment_id is null
```

- [ ] **Step 4: Commit**

```bash
git add dbt/models/intermediate/int_affected_subscription_payment_keys.sql dbt/tests/int_affected_subscription_payment_keys_unique.sql dbt/tests/int_affected_subscription_payment_keys_resolve.sql
git commit -m "feat: add subscription payment affected keys"
```

---

### Task 5: `control.affected_keys` 감사 기록 스키마 변경

**Files:**
- Modify: `dbt/macros/record_affected_keys.sql`
- Modify: `dbt/dbt_project.yml` (Hook의 `depends_on` 추가)
- Create: `tests/test_dbt_affected_keys_macro.py`

**Interfaces:**
- Consumes: Task 3의 `int_affected_order_keys`, Task 4의 `int_affected_subscription_payment_keys`
- Produces: `control.affected_keys(invocation_id, affected_domain, entity_key, business_date_key, recorded_at)`. `affected_domain`은 `'order'` 또는 `'subscription_payment'`다.

- [ ] **Step 1: 매크로 교체**

`dbt/macros/record_affected_keys.sql` 전체를 아래로 바꾼다. 기존 Warehouse에는 `order_id` 컬럼을 가진 구 Table이 남아 있으므로 한 번 감지해 버린다.

```sql
{% macro record_affected_keys() -%}
    {#- 이번 Invocation이 재계산한 영향 Key를 도메인과 함께 감사 기록에 남긴다. -#}
    {%- if execute -%}
        {%- do run_query("CREATE SCHEMA IF NOT EXISTS control") -%}
        {%- set legacy_columns = run_query("
            select count(*) as legacy_count
            from information_schema.columns
            where table_schema = 'control'
              and table_name = 'affected_keys'
              and column_name = 'order_id'
        ") -%}
        {%- if legacy_columns and legacy_columns.rows | length > 0 and legacy_columns.rows[0][0] > 0 -%}
            {%- do log("Dropping legacy control.affected_keys with order_id column", info=true) -%}
            {%- do run_query("DROP TABLE control.affected_keys") -%}
        {%- endif -%}
        {%- do run_query("
            CREATE TABLE IF NOT EXISTS control.affected_keys (
                invocation_id VARCHAR NOT NULL,
                affected_domain VARCHAR NOT NULL,
                entity_key VARCHAR NOT NULL,
                business_date_key INTEGER NOT NULL,
                recorded_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
            )
        ") -%}
        {%- set insert_sql -%}
            insert into control.affected_keys (invocation_id, affected_domain, entity_key, business_date_key)
            select '{{ invocation_id }}', 'order', order_id, business_date_key
            from {{ ref('int_affected_order_keys') }}
            union all
            select '{{ invocation_id }}', 'subscription_payment', payment_id, business_date_key
            from {{ ref('int_affected_subscription_payment_keys') }}
        {%- endset -%}
        {%- do run_query(insert_sql) -%}
    {%- endif -%}
{%- endmacro %}
```

- [ ] **Step 2: Hook의 `depends_on` 갱신**

`dbt/dbt_project.yml`의 `on-run-end` 첫 항목을 아래로 바꾼다.

```yaml
  - "-- depends_on: {{ ref('int_affected_order_keys') }}\n-- depends_on: {{ ref('int_affected_subscription_payment_keys') }}\n{{ record_affected_keys() }}"
```

- [ ] **Step 3: 구 Table 감지 Test 작성**

`tests/test_dbt_affected_keys_macro.py`:

```python
"""영향 Key 감사 Table의 스키마 전환을 검증한다."""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_record_macro_declares_the_domain_schema() -> None:
    """감사 Table이 도메인과 Entity Key 컬럼을 갖는다."""
    macro = (PROJECT_ROOT / "dbt/macros/record_affected_keys.sql").read_text()

    assert "affected_domain VARCHAR NOT NULL" in macro
    assert "entity_key VARCHAR NOT NULL" in macro
    assert "order_id VARCHAR NOT NULL" not in macro


def test_record_macro_drops_the_legacy_table() -> None:
    """구 스키마 Table이 남아 있으면 감지해서 버린다."""
    macro = (PROJECT_ROOT / "dbt/macros/record_affected_keys.sql").read_text()

    assert "column_name = 'order_id'" in macro
    assert "DROP TABLE control.affected_keys" in macro


def test_record_macro_writes_both_domains() -> None:
    """주문 축과 구독 결제 축을 모두 기록한다."""
    macro = (PROJECT_ROOT / "dbt/macros/record_affected_keys.sql").read_text()

    assert "'order', order_id" in macro
    assert "'subscription_payment', payment_id" in macro
```

- [ ] **Step 4: Test 실행**

Run: `uv run pytest tests/test_dbt_affected_keys_macro.py -v`
Expected: PASS 3건.

- [ ] **Step 5: Commit**

```bash
git add dbt/macros/record_affected_keys.sql dbt/dbt_project.yml tests/test_dbt_affected_keys_macro.py
git commit -m "feat: record affected keys per domain"
```

---

### Task 6: Fact Incremental 필터와 교체 단위

**Files:**
- Modify: `dbt/models/marts/facts/fact_orders.sql`
- Modify: `dbt/models/marts/facts/fact_order_items.sql`
- Modify: `dbt/models/marts/facts/fact_payments.sql`
- Modify: `dbt/models/marts/facts/fact_subscription_payments.sql`
- Create: `tests/test_fact_incremental_contract.py`

**Interfaces:**
- Consumes: Task 3·4의 영향 Key 모델
- Produces: 없음. 이 Task가 `P6-22`의 마지막 코드 변경이다.

- [ ] **Step 1: 실패 Test 작성**

`tests/test_fact_incremental_contract.py`:

```python
"""Fact가 영향 Key로만 재계산하고 교체 단위를 올바르게 선언하는지 검증한다."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

ORDER_AXIS_FACTS = (
    "dbt/models/marts/facts/fact_orders.sql",
    "dbt/models/marts/facts/fact_order_items.sql",
    "dbt/models/marts/facts/fact_payments.sql",
)


def test_order_axis_facts_filter_by_affected_order_keys() -> None:
    """주문 축 Fact는 Incremental 실행에서 영향 주문만 다시 계산한다."""
    for relative_path in ORDER_AXIS_FACTS:
        sql = (PROJECT_ROOT / relative_path).read_text()

        assert "is_incremental()" in sql, relative_path
        assert "ref('int_affected_order_keys')" in sql, relative_path


def test_order_axis_facts_replace_whole_orders() -> None:
    """자식 Fact가 주문 단위로 교체되어야 사라진 행이 남지 않는다."""
    for relative_path in ORDER_AXIS_FACTS:
        sql = (PROJECT_ROOT / relative_path).read_text()

        assert "unique_key='order_id'" in sql, relative_path
        assert "order_item_id'," not in sql, relative_path


def test_subscription_payment_fact_filters_by_affected_payment_keys() -> None:
    """구독 결제 Fact는 영향 결제 시도만 다시 계산한다."""
    sql = (PROJECT_ROOT / "dbt/models/marts/facts/fact_subscription_payments.sql").read_text()

    assert "is_incremental()" in sql
    assert "ref('int_affected_subscription_payment_keys')" in sql
    assert "unique_key='payment_id'" in sql
```

- [ ] **Step 2: Test 실행해 실패 확인**

Run: `uv run pytest tests/test_fact_incremental_contract.py -v`
Expected: FAIL 3건. 아직 어떤 Fact에도 `is_incremental()`이 없다.

- [ ] **Step 3: `fact_orders.sql` 수정**

```sql
{{
    config(
        unique_key='order_id',
        incremental_strategy='delete+insert'
    )
}}

-- dbt unique_key는 교체 단위다. Grain 유일성은 schema.yml의 unique Test가 강제한다.
select
    order_id,
    customer_key,
    purchase_date_key,
    order_status,
    customer_city,
    customer_state,
    gross_order_value,
    payment_total,
    order_count,
    carrier_handoff_days,
    delivery_days,
    delivery_delay_days,
    is_late
from {{ ref('int_order_fact_ready') }}
{% if is_incremental() %}
where order_id in (select order_id from {{ ref('int_affected_order_keys') }})
{% endif %}
```

- [ ] **Step 4: `fact_order_items.sql` 수정**

`unique_key`를 `order_id`로 바꾼다. 복합 Key로 두면 원본에서 사라진 Line이 Fact에 남는다.

```sql
{{
    config(
        unique_key='order_id',
        incremental_strategy='delete+insert'
    )
}}

-- dbt unique_key는 교체 단위인 주문이다. Grain 유일성은 tests/fact_order_items_unique.sql이 강제한다.
select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price as item_price,
    freight_value,
    line_gross_value
from {{ ref('int_order_items_enriched') }}
{% if is_incremental() %}
where order_id in (select order_id from {{ ref('int_affected_order_keys') }})
{% endif %}
```

- [ ] **Step 5: `fact_payments.sql` 수정**

```sql
{{
    config(
        unique_key='order_id',
        incremental_strategy='delete+insert'
    )
}}

-- dbt unique_key는 교체 단위인 주문이다. Grain 유일성은 tests/fact_payments_unique.sql이 강제한다.
select
    order_id,
    payment_sequence,
    payment_status,
    payment_value
from {{ ref('stg_payments') }}
{% if is_incremental() %}
where order_id in (select order_id from {{ ref('int_affected_order_keys') }})
{% endif %}
```

- [ ] **Step 6: `fact_subscription_payments.sql` 수정**

```sql
{{ config(unique_key='payment_id', incremental_strategy='delete+insert') }}

-- dbt unique_key는 교체 단위인 결제 시도다. Grain 유일성은 schema.yml의 unique Test가 강제한다.
select
    payment_id,
    subscription_key,
    customer_key,
    payment_date_key,
    subscription_id,
    billing_cycle_sequence,
    attempt_sequence,
    provider_payment_id,
    payment_status,
    payment_method_type,
    payment_provider,
    failure_code,
    currency_code,
    payment_at,
    billing_period_start_at,
    billing_period_end_at,
    payment_value,
    completed_payment_value,
    attempt_count
from {{ ref('int_subscription_payments_enriched') }}
{% if is_incremental() %}
where payment_id in (select payment_id from {{ ref('int_affected_subscription_payment_keys') }})
{% endif %}
```

- [ ] **Step 7: Test 실행해 통과 확인**

Run: `uv run pytest tests/test_fact_incremental_contract.py tests/test_fact_layer_contract.py -v`
Expected: PASS. `test_fact_layer_contract.py`는 Fact가 집계·파생을 하지 않는지 검사하므로 함께 돌려 회귀를 막는다.

- [ ] **Step 8: 전체 Full Refresh Build로 회귀 확인**

컨테이너를 띄운 상태에서 실행한다.

Run: `uv run dbt build --project-dir dbt --profiles-dir dbt --full-refresh`
Expected: 모든 Model과 Test 통과. 실패하면 첫 실패 Model 이름과 에러 한 줄을 확인하고 해당 Task로 돌아간다.

- [ ] **Step 9: Incremental Build 한 번 더 실행**

Run: `uv run dbt build --project-dir dbt --profiles-dir dbt`
Expected: 통과. 새 Batch가 없으므로 영향 Key가 0행이고 Fact는 아무 행도 교체하지 않는다.

- [ ] **Step 10: Commit**

```bash
git add dbt/models/marts/facts tests/test_fact_incremental_contract.py
git commit -m "feat: recompute facts from affected keys only"
```

---

### Task 7: 구독 축 Late Arrival 통합 Test

**Files:**
- Modify: `tests/integration/test_subscription_payment_temporal_join_integration.py`

**Interfaces:**
- Consumes: Task 1~6 전체
- Produces: 없음. `P6-10`/`P6-22`의 합격 증거다.

이 Task의 Test는 기존 파일의 Helper(`_generator_config`, `_set_watermark`, `_ingest`, `_create_fixture_catalog`, `_run_dbt_build`, `_cleanup`, `_combined_output`)를 그대로 쓴다. 새 Helper는 Step 1에서 하나만 추가한다.

모든 새 Test는 `_ingest`의 `sequence` 인자에 **같은 값 1**을 넘긴다. `_ingest`는 `sequence`로 `dag_id`를 만들고 `_batch_id`는 `{dag_id}__{logical_date}`이므로, `dag_id`를 고정해야 Watermark의 사전식 비교가 운영과 같은 의미를 갖는다. Batch 구분은 `logical_date`가 한다.

- [ ] **Step 1: Import 추가와 Catalog 추가 등록 Helper 작성**

파일 상단 Import 블록을 먼저 고친다.

- `from dataclasses import replace`를 `from datetime import ...` 위에 추가한다.
- `from src.generator.customers import (...)` 목록에 `persist_subscription_records`와 `subscription_transition_records`를 알파벳 순서에 맞게 추가한다.

그 다음 기존 `_create_fixture_catalog` 아래에 Helper를 붙인다. 기존 Helper는 `control` Schema를 새로 만들기 때문에 두 번째 Batch에는 쓸 수 없다.

```python
def _append_fixture_catalog(
    postgres: PostgresSettings,
    warehouse_path: Path,
    results: list[TableIngestionResult],
) -> None:
    """이미 만들어진 Bronze Catalog에 추가 Batch의 Object만 등록한다."""
    table_batch_ids = [f"{result.run.batch_id}__{result.run.source_table}" for result in results]
    rows: list[tuple[object, ...]] = []
    with postgres.pipeline_connection() as connection:
        for table_batch_id in table_batch_ids:
            row = connection.execute(
                """
                SELECT source_table, object_key, schema_version, batch_id,
                       committed_at, row_count, logical_hash
                FROM bronze_objects
                WHERE table_batch_id = %s AND status = 'COMMITTED'
                """,
                (table_batch_id,),
            ).fetchone()
            if row is None:
                raise RuntimeError(f"Fixture Bronze object is missing: {table_batch_id}")
            rows.append(tuple(row))

    with duckdb.connect(str(warehouse_path)) as connection:
        connection.executemany(
            "INSERT INTO control.bronze_files VALUES (?, ?, ?, ?, ?, ?, ?)", rows
        )
```

- [ ] **Step 2: 지연 구독 결제 Test 작성**

기존 Test 함수 아래, Helper 정의 위에 붙인다.

```python
@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
def test_late_subscription_payment_updates_the_past_payment_date_fact(tmp_path) -> None:
    """과거 결제 시각을 가진 지연 결제가 두 번째 Build에서 과거 날짜 Fact로 들어온다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    pipeline_name = f"test_late_subscription_payment_{uuid.uuid4().hex}"
    ingested_at = datetime.now(UTC)
    initial_config = _generator_config(FIXTURE_START, anomaly_profile="default")
    customer = new_customer_record(initial_config, 1)
    subscription = new_subscription_record(customer)
    tier = new_membership_tier_record(customer)
    first_payment_at = FIXTURE_START + timedelta(days=1)
    first_payment = plan_subscription_payment(
        _generator_config(first_payment_at, anomaly_profile="default"),
        subscription.subscription_id,
        billing_cycle_sequence=1,
        attempt_sequence=1,
        billing_period_start_at=FIXTURE_START,
    )
    late_payment_at = FIXTURE_START + timedelta(days=2)
    late_arrival_at = FIXTURE_START + timedelta(days=5)
    late_payment = replace(
        plan_subscription_payment(
            _generator_config(late_payment_at, anomaly_profile="default"),
            subscription.subscription_id,
            billing_cycle_sequence=2,
            attempt_sequence=1,
            billing_period_start_at=late_payment_at,
        ),
        updated_at=late_arrival_at,
    )
    results: list[TableIngestionResult] = []
    warehouse_path = tmp_path / "warehouse.duckdb"

    try:
        with postgres.source_connection() as connection:
            assert persist_customer_records(connection, (customer,)).inserted == 1
            assert ensure_subscription_records(connection, (subscription,)).inserted == 1
            assert ensure_membership_tier_records(connection, (tier,)).inserted == 1
            assert persist_subscription_payments(connection, (first_payment,)) == 1
            connection.commit()

        for source_table, cursor_at, cursor_key in (
            ("customer_subscriptions", subscription.updated_at, str(subscription.subscription_id)),
            ("customer_membership_tiers", tier.updated_at, customer.customer_unique_id),
            ("subscription_payments", first_payment.updated_at, str(first_payment.payment_id)),
        ):
            _set_watermark(
                postgres,
                pipeline_name,
                source_table,
                CursorPosition(cursor_at - timedelta(microseconds=1), (cursor_key,)),
                now=ingested_at,
            )
            results.append(
                _ingest(
                    postgres,
                    storage,
                    pipeline_name,
                    source_table,
                    cursor_at,
                    1,
                    tmp_path,
                    ingested_at,
                )
            )

        _create_fixture_catalog(postgres, warehouse_path, results)
        first_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert first_build.returncode == 0, _combined_output(first_build)

        with postgres.source_connection() as connection:
            assert persist_subscription_payments(connection, (late_payment,)) == 1
            connection.commit()

        late_results = [
            _ingest(
                postgres,
                storage,
                pipeline_name,
                "subscription_payments",
                late_arrival_at,
                1,
                tmp_path,
                ingested_at,
            )
        ]
        results.extend(late_results)
        _append_fixture_catalog(postgres, warehouse_path, late_results)
        second_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert second_build.returncode == 0, _combined_output(second_build)

        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            rows = connection.execute(
                """
                SELECT payment_id, payment_date_key
                FROM facts.fact_subscription_payments
                ORDER BY payment_date_key
                """
            ).fetchall()

        assert rows == [
            (str(first_payment.payment_id), 21000102),
            (str(late_payment.payment_id), 21000103),
        ]
    finally:
        _cleanup(postgres, storage, pipeline_name, results, customer.customer_unique_id)
```

`FIXTURE_START`는 `2100-01-01`이므로 `+1일`은 `21000102`, `+2일`은 `21000103`이다.

- [ ] **Step 3: Test 실행**

컨테이너를 띄운 뒤 실행한다.

Run: `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/integration/test_subscription_payment_temporal_join_integration.py::test_late_subscription_payment_updates_the_past_payment_date_fact -v`
Expected: PASS. 지연 결제가 Fact에 없으면 Task 4의 `affected_payments` 조건이나 Task 6의 필터를 확인한다.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/test_subscription_payment_temporal_join_integration.py
git commit -m "test: verify late subscription payment recompute"
```

- [ ] **Step 5: 계약 Version 변경 재결합 Test 작성**

같은 파일에 이어서 붙인다.

```python
@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
def test_late_contract_observation_rebinds_following_payment_versions(tmp_path) -> None:
    """지연 도착한 계약 상태 관측이 그 이후 결제의 계약 Version을 다시 묶는다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    pipeline_name = f"test_late_contract_observation_{uuid.uuid4().hex}"
    ingested_at = datetime.now(UTC)
    initial_config = _generator_config(FIXTURE_START, anomaly_profile="default")
    customer = new_customer_record(initial_config, 1)
    subscription = new_subscription_record(customer)
    tier = new_membership_tier_record(customer)
    early_payment_at = FIXTURE_START + timedelta(days=1)
    early_payment = plan_subscription_payment(
        _generator_config(early_payment_at, anomaly_profile="default"),
        subscription.subscription_id,
        billing_cycle_sequence=1,
        attempt_sequence=1,
        billing_period_start_at=FIXTURE_START,
    )
    later_payment_at = FIXTURE_START + timedelta(days=40)
    later_payment = plan_subscription_payment(
        _generator_config(later_payment_at, anomaly_profile="default"),
        subscription.subscription_id,
        billing_cycle_sequence=2,
        attempt_sequence=1,
        billing_period_start_at=FIXTURE_START + timedelta(days=31),
    )
    transition_at = FIXTURE_START + timedelta(days=20)
    (transitioned_subscription,) = subscription_transition_records(
        _generator_config(transition_at, anomaly_profile="default"),
        (subscription,),
        "PAYMENT_FAILED",
    )
    results: list[TableIngestionResult] = []
    warehouse_path = tmp_path / "warehouse.duckdb"

    try:
        with postgres.source_connection() as connection:
            assert persist_customer_records(connection, (customer,)).inserted == 1
            assert ensure_subscription_records(connection, (subscription,)).inserted == 1
            assert ensure_membership_tier_records(connection, (tier,)).inserted == 1
            assert persist_subscription_payments(connection, (early_payment, later_payment)) == 2
            connection.commit()

        for source_table, cursor_at, cursor_key in (
            ("customer_subscriptions", subscription.updated_at, str(subscription.subscription_id)),
            ("customer_membership_tiers", tier.updated_at, customer.customer_unique_id),
            ("subscription_payments", later_payment.updated_at, str(later_payment.payment_id)),
        ):
            _set_watermark(
                postgres,
                pipeline_name,
                source_table,
                CursorPosition(cursor_at - timedelta(microseconds=1), (cursor_key,)),
                now=ingested_at,
            )
            results.append(
                _ingest(
                    postgres,
                    storage,
                    pipeline_name,
                    source_table,
                    cursor_at,
                    1,
                    tmp_path,
                    ingested_at,
                )
            )

        _create_fixture_catalog(postgres, warehouse_path, results)
        first_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert first_build.returncode == 0, _combined_output(first_build)

        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            before = dict(
                connection.execute(
                    "SELECT payment_id, subscription_key FROM facts.fact_subscription_payments"
                ).fetchall()
            )

        with postgres.source_connection() as connection:
            assert persist_subscription_records(connection, (transitioned_subscription,)).updated == 1
            connection.commit()

        transition_results = [
            _ingest(
                postgres,
                storage,
                pipeline_name,
                "customer_subscriptions",
                transitioned_subscription.updated_at,
                1,
                tmp_path,
                ingested_at,
            )
        ]
        results.extend(transition_results)
        _append_fixture_catalog(postgres, warehouse_path, transition_results)
        second_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert second_build.returncode == 0, _combined_output(second_build)

        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            after = dict(
                connection.execute(
                    """
                    SELECT fact.payment_id, subscription.subscription_status
                    FROM facts.fact_subscription_payments AS fact
                    JOIN dimensions.dim_subscription AS subscription USING (subscription_key)
                    """
                ).fetchall()
            )
            keys_after = dict(
                connection.execute(
                    "SELECT payment_id, subscription_key FROM facts.fact_subscription_payments"
                ).fetchall()
            )

        assert after[str(early_payment.payment_id)] == "ACTIVE"
        assert after[str(later_payment.payment_id)] == "PAYMENT_FAILED"
        assert keys_after[str(early_payment.payment_id)] == before[str(early_payment.payment_id)]
        assert keys_after[str(later_payment.payment_id)] != before[str(later_payment.payment_id)]
    finally:
        _cleanup(postgres, storage, pipeline_name, results, customer.customer_unique_id)
```

- [ ] **Step 6: Test 실행**

Run: `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/integration/test_subscription_payment_temporal_join_integration.py::test_late_contract_observation_rebinds_following_payment_versions -v`
Expected: PASS. 뒤 결제의 `subscription_key`가 그대로면 Task 4의 `affected_from_subscription_versions` 조건을 확인한다.

- [ ] **Step 7: Commit**

```bash
git add tests/integration/test_subscription_payment_temporal_join_integration.py
git commit -m "test: verify contract version rebind on late observation"
```

- [ ] **Step 8: 다중 Batch 경계 Test 작성**

같은 파일에 이어서 붙인다. Watermark가 없으면 마지막 Batch만 재계산되어 첫 번째 결제가 누락된다.

```python
@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
def test_two_batches_before_one_build_are_both_recomputed(tmp_path) -> None:
    """dbt Build 없이 쌓인 두 Batch의 변경이 한 번의 Build에서 모두 반영된다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    pipeline_name = f"test_two_batches_one_build_{uuid.uuid4().hex}"
    ingested_at = datetime.now(UTC)
    initial_config = _generator_config(FIXTURE_START, anomaly_profile="default")
    customer = new_customer_record(initial_config, 1)
    subscription = new_subscription_record(customer)
    tier = new_membership_tier_record(customer)
    first_payment = plan_subscription_payment(
        _generator_config(FIXTURE_START + timedelta(days=1), anomaly_profile="default"),
        subscription.subscription_id,
        billing_cycle_sequence=1,
        attempt_sequence=1,
        billing_period_start_at=FIXTURE_START,
    )
    second_payment = plan_subscription_payment(
        _generator_config(FIXTURE_START + timedelta(days=32), anomaly_profile="default"),
        subscription.subscription_id,
        billing_cycle_sequence=2,
        attempt_sequence=1,
        billing_period_start_at=FIXTURE_START + timedelta(days=31),
    )
    results: list[TableIngestionResult] = []
    warehouse_path = tmp_path / "warehouse.duckdb"

    try:
        with postgres.source_connection() as connection:
            assert persist_customer_records(connection, (customer,)).inserted == 1
            assert ensure_subscription_records(connection, (subscription,)).inserted == 1
            assert ensure_membership_tier_records(connection, (tier,)).inserted == 1
            connection.commit()

        for source_table, cursor_at, cursor_key in (
            ("customer_subscriptions", subscription.updated_at, str(subscription.subscription_id)),
            ("customer_membership_tiers", tier.updated_at, customer.customer_unique_id),
        ):
            _set_watermark(
                postgres,
                pipeline_name,
                source_table,
                CursorPosition(cursor_at - timedelta(microseconds=1), (cursor_key,)),
                now=ingested_at,
            )
            results.append(
                _ingest(
                    postgres,
                    storage,
                    pipeline_name,
                    source_table,
                    cursor_at,
                    1,
                    tmp_path,
                    ingested_at,
                )
            )

        _create_fixture_catalog(postgres, warehouse_path, results)
        baseline_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert baseline_build.returncode == 0, _combined_output(baseline_build)

        _set_watermark(
            postgres,
            pipeline_name,
            "subscription_payments",
            CursorPosition(
                first_payment.updated_at - timedelta(microseconds=1),
                (str(first_payment.payment_id),),
            ),
            now=ingested_at,
        )
        staged_results: list[TableIngestionResult] = []
        for payment in (first_payment, second_payment):
            with postgres.source_connection() as connection:
                assert persist_subscription_payments(connection, (payment,)) == 1
                connection.commit()
            staged_results.append(
                _ingest(
                    postgres,
                    storage,
                    pipeline_name,
                    "subscription_payments",
                    payment.updated_at,
                    1,
                    tmp_path,
                    ingested_at,
                )
            )

        results.extend(staged_results)
        _append_fixture_catalog(postgres, warehouse_path, staged_results)
        single_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert single_build.returncode == 0, _combined_output(single_build)

        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            payment_ids = {
                row[0]
                for row in connection.execute(
                    "SELECT payment_id FROM facts.fact_subscription_payments"
                ).fetchall()
            }

        assert payment_ids == {str(first_payment.payment_id), str(second_payment.payment_id)}
    finally:
        _cleanup(postgres, storage, pipeline_name, results, customer.customer_unique_id)
```

- [ ] **Step 9: Test 실행**

Run: `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/integration/test_subscription_payment_temporal_join_integration.py -v`
Expected: 파일 안 4개 Test 모두 PASS. 첫 결제만 빠지면 Watermark가 전진하지 않았거나 경계 비교가 `=`로 들어간 것이다.

- [ ] **Step 10: 전체 회귀 실행**

Run: `uv run pytest -m "not integration" -q`
Expected: PASS.

Run: `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/integration/test_order_e2e_and_late_order_mart_integration.py -v`
Expected: PASS. AC-01과 AC-11이 주문 축에서 유지되는지 확인한다.

- [ ] **Step 11: Commit**

```bash
git add tests/integration/test_subscription_payment_temporal_join_integration.py
git commit -m "test: verify multi batch recompute boundary"
```

---

## 완료 조건

- `P6-10`: 주문 축과 구독 결제 축 영향 Key 모델이 존재하고 Grain Test가 통과한다.
- `P6-22`: 네 Fact가 모두 영향 Key로만 재계산하고, 다중 Batch·지연 도착 Test가 통과한다.
- `docs/phases/phase-06-dimensional-modeling.md`의 `P6-10`, `P6-22` Checkbox는 **아키텍트가 검수한 뒤** 갱신한다. developer는 건드리지 않는다.

## 이 계획의 범위 밖

- `P6-23` Incremental과 Full Refresh Logical Hash 비교
- `P6-24` Bronze Replay와 Re-extract 입력 경계
- `P6-25` Phase 4 DAG Build 경계 문서 마감
