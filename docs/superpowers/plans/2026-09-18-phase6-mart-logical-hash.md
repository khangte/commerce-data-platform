# Mart Logical Hash 비교 Implementation Plan

> **For agentic workers:** Task 순서대로 구현한다. Step은 Checkbox(`- [ ]`)로 추적한다. 각 Task는 마지막 Step의 Commit까지 끝내고 다음 Task로 넘어간다.

**Goal:** Incremental Build 결과가 같은 입력의 Full Refresh 결과와 논리적으로 같은지 Mart Logical Hash로 검증한다.

**Architecture:** Python 모듈이 Warehouse의 Mart 9개를 Key 정렬 Canonical JSON 누적 SHA-256으로 Hash한다. 비교는 Incremental Build를 마친 Warehouse 파일을 복사해 복사본에만 `dbt build --full-refresh`를 돌리고 두 Hash Set을 맞춰 보는 방식이다. Hash가 다르면 Model·Key 단위 진단을 출력한다.

**Tech Stack:** Python 3.12, DuckDB 1.5.5, dbt-core 1.12.3, pytest 9.1.1

**Spec:** `docs/architecture/07-incremental-full-refresh-logical-hash.md`

## Global Constraints

- 모든 Class·Function 선언 바로 아래에 한국어 Docstring을 쓴다.
- 한국어 문자열은 UTF-8 그대로 쓴다. `\uXXXX` Escape를 쓰지 않는다.
- `dbt` 실행은 항상 `--project-dir dbt --profiles-dir dbt`를 붙인다.
- `dbt/` 아래 파일은 이 계획에서 바꾸지 않는다. Hash 비교는 Build 산출물을 읽기만 한다.
- Hash 대상은 `dimensions` 5개와 `facts` 4개다. `metrics`, `staging`, `intermediate`는 제외한다.
- 정렬 Key는 Model이 실제로 내보내는 컬럼 이름을 쓴다. `docs/reference/mart-grain.md`의 `product_key` / `seller_key` 표기는 구현과 다르며, 이 계획에서 고치지 않는다.
- 작업 브랜치는 `feature/phase6-remaining`이다.
- Commit 메시지는 Conventional Commits 형식으로 쓴다.

---

## File Structure

| 경로 | 책임 |
| ---- | ---- |
| `src/warehouse/__init__.py` | Warehouse 검증 도구 Package |
| `src/warehouse/mart_hash.py` | Hash 대상 목록, Hash 계산, 불일치 진단, CLI |
| `tests/test_mart_hash.py` | Canonical 표현·정렬 무관성·목록 누락 방지·진단 Unit Test |
| `tests/integration/test_incremental_full_refresh_hash_integration.py` | 지연 도착 뒤 Incremental과 Full Refresh 비교 |

---

### Task 1: Mart Logical Hash 계산

**Files:**
- Create: `src/warehouse/__init__.py`
- Create: `src/warehouse/mart_hash.py`
- Create: `tests/test_mart_hash.py`

**Interfaces:**
- Consumes: 없음
- Produces: `MartTarget(schema, model, order_by)`와 `MartTarget.relation -> str`, `MART_HASH_TARGETS: tuple[MartTarget, ...]`, `mart_logical_hash(connection: duckdb.DuckDBPyConnection, target: MartTarget) -> str`, `mart_logical_hashes(warehouse_path: Path) -> dict[str, str]`. Dict의 Key는 `"dimensions.dim_customer"` 형태의 Relation 이름이다.

- [ ] **Step 1: 실패 Test 작성**

`tests/test_mart_hash.py`:

```python
"""Mart Logical Hash의 값 표현과 정렬 무관성을 검증한다."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import duckdb
import pytest

from src.warehouse.mart_hash import MartTarget, _canonical_row_json, mart_logical_hash


def test_canonical_row_json_normalizes_deterministic_values() -> None:
    """Timestamp는 UTC ISO, Decimal은 고정 소수점, UUID는 문자열, None은 null로 적는다."""
    row = {
        "paid_at": datetime(2026, 9, 18, 3, 0, tzinfo=timezone(timedelta(hours=9))),
        "amount": Decimal("10.50"),
        "payment_id": UUID("2f1b3d4e-5a6b-7c8d-9e0f-1a2b3c4d5e6f"),
        "failure_code": None,
        "billing_date": date(2026, 9, 18),
    }

    assert _canonical_row_json(row) == (
        '{"amount":"10.50",'
        '"billing_date":"2026-09-18",'
        '"failure_code":null,'
        '"paid_at":"2026-09-17T18:00:00+00:00",'
        '"payment_id":"2f1b3d4e-5a6b-7c8d-9e0f-1a2b3c4d5e6f"}'
    )


def test_canonical_row_json_rejects_a_naive_timestamp() -> None:
    """Timezone 없는 Timestamp는 Hash를 실행 환경에 의존하게 만들므로 막는다."""
    with pytest.raises(ValueError, match="UTC offset"):
        _canonical_row_json({"paid_at": datetime(2026, 9, 18, 3, 0)})


def test_hash_ignores_physical_row_order() -> None:
    """같은 논리 내용이면 저장 순서가 달라도 Hash가 같다."""
    target = MartTarget("main", "sample", ("id",))
    first = _sample_connection([(2, "b"), (1, "a")])
    second = _sample_connection([(1, "a"), (2, "b")])

    try:
        assert mart_logical_hash(first, target) == mart_logical_hash(second, target)
    finally:
        first.close()
        second.close()


def test_hash_changes_when_one_value_changes() -> None:
    """값이 하나라도 다르면 Hash가 달라진다."""
    target = MartTarget("main", "sample", ("id",))
    first = _sample_connection([(1, "a"), (2, "b")])
    second = _sample_connection([(1, "a"), (2, "B")])

    try:
        assert mart_logical_hash(first, target) != mart_logical_hash(second, target)
    finally:
        first.close()
        second.close()


def _sample_connection(rows: list[tuple[int, str]]) -> duckdb.DuckDBPyConnection:
    """지정한 순서로 행을 넣은 메모리 DuckDB 연결을 만든다."""
    connection = duckdb.connect()
    connection.execute("CREATE TABLE sample (id INTEGER, label VARCHAR)")
    connection.executemany("INSERT INTO sample VALUES (?, ?)", rows)
    return connection
```

- [ ] **Step 2: Test 실행해 실패 확인**

Run: `uv run pytest tests/test_mart_hash.py -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'src.warehouse'"

- [ ] **Step 3: Package 생성**

`src/warehouse/__init__.py`:

```python
"""Warehouse 산출물을 읽어 검증하는 도구를 담는다."""
```

- [ ] **Step 4: Hash 계산 구현**

`src/warehouse/mart_hash.py`:

```python
"""Mart Table의 Logical Hash를 계산하고 Incremental과 Full Refresh 결과를 비교한다."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import duckdb

ROW_BATCH_SIZE = 10_000


@dataclass(frozen=True)
class MartTarget:
    """Hash 비교 대상 Mart 하나의 Schema, Model 이름, 정렬 Key를 담는다."""

    schema: str
    model: str
    order_by: tuple[str, ...]

    @property
    def relation(self) -> str:
        """Warehouse에서 조회할 Relation 이름을 반환한다."""
        return f"{self.schema}.{self.model}"


MART_HASH_TARGETS: tuple[MartTarget, ...] = (
    MartTarget("dimensions", "dim_customer", ("customer_key",)),
    MartTarget("dimensions", "dim_date", ("date_key",)),
    MartTarget("dimensions", "dim_product", ("product_id",)),
    MartTarget("dimensions", "dim_seller", ("seller_id",)),
    MartTarget("dimensions", "dim_subscription", ("subscription_key",)),
    MartTarget("facts", "fact_orders", ("order_id",)),
    MartTarget("facts", "fact_order_items", ("order_id", "order_item_id")),
    MartTarget("facts", "fact_payments", ("order_id", "payment_sequence")),
    MartTarget("facts", "fact_subscription_payments", ("payment_id",)),
)


def mart_logical_hash(connection: duckdb.DuckDBPyConnection, target: MartTarget) -> str:
    """Mart 한 개를 Key 정렬 Canonical JSON 누적 SHA-256 Hash로 변환한다."""
    order_by = ", ".join(f'"{column}"' for column in target.order_by)
    cursor = connection.execute(f"SELECT * FROM {target.relation} ORDER BY {order_by}")
    column_names = [descriptor[0] for descriptor in cursor.description]
    digest = hashlib.sha256()
    while True:
        rows = cursor.fetchmany(ROW_BATCH_SIZE)
        if not rows:
            break
        for row in rows:
            payload = _canonical_row_json(dict(zip(column_names, row, strict=True)))
            digest.update(payload.encode("utf-8"))
            digest.update(b"\n")
    return digest.hexdigest()


def mart_logical_hashes(warehouse_path: Path) -> dict[str, str]:
    """Hash 대상 Mart 전부를 Relation 이름 기준 Hash Dict로 반환한다."""
    with duckdb.connect(str(warehouse_path), read_only=True) as connection:
        return {
            target.relation: mart_logical_hash(connection, target)
            for target in MART_HASH_TARGETS
        }


def _canonical_row_json(row: dict[str, object]) -> str:
    """컬럼 이름을 정렬하고 값 표현을 고정한 행 JSON을 반환한다."""
    return json.dumps(
        row,
        default=_json_default,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _json_default(value: object) -> str:
    """JSON 기본 형식에 없는 Warehouse 값의 결정적 문자열 표현을 반환한다."""
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Mart timestamp must include a UTC offset")
        return value.astimezone(UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, UUID):
        return str(value)
    raise TypeError(f"Unsupported mart value: {type(value).__name__}")
```

- [ ] **Step 5: Test 실행해 통과 확인**

Run: `uv run pytest tests/test_mart_hash.py -v`
Expected: PASS 4건.

- [ ] **Step 6: Commit**

```bash
git add src/warehouse tests/test_mart_hash.py
git commit -m "feat: add mart logical hash"
```

---

### Task 2: Hash 대상 목록 누락 방지

**Files:**
- Modify: `tests/test_mart_hash.py`

**Interfaces:**
- Consumes: Task 1의 `MART_HASH_TARGETS`
- Produces: 없음

- [ ] **Step 1: 실패 Test 작성**

`tests/test_mart_hash.py`의 Import 블록에 `from pathlib import Path`와 `from src.warehouse.mart_hash import MART_HASH_TARGETS`를 추가하고, 기존 Test 아래에 붙인다.

```python
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_hash_targets_cover_every_materialized_mart() -> None:
    """Mart를 추가하고 Hash 대상 목록에 넣지 않으면 조용히 비교에서 빠지므로 막는다."""
    expected = set()
    for schema in ("dimensions", "facts"):
        for path in (PROJECT_ROOT / "dbt/models/marts" / schema).glob("*.sql"):
            expected.add(f"{schema}.{path.stem}")

    assert {target.relation for target in MART_HASH_TARGETS} == expected


def test_hash_targets_declare_an_order_key() -> None:
    """정렬 Key가 없으면 행 순서가 Hash를 좌우하므로 모든 대상이 Key를 가진다."""
    for target in MART_HASH_TARGETS:
        assert target.order_by, target.relation
```

- [ ] **Step 2: Test 실행**

Run: `uv run pytest tests/test_mart_hash.py -v`
Expected: PASS 6건. 실패하면 `MART_HASH_TARGETS`와 `dbt/models/marts` 파일 목록의 차집합을 확인한다. Model을 뺀 것이 아니라면 목록에 추가한다.

- [ ] **Step 3: Commit**

```bash
git add tests/test_mart_hash.py
git commit -m "test: pin mart hash target coverage"
```

---

### Task 3: 불일치 진단

**Files:**
- Modify: `src/warehouse/mart_hash.py`
- Modify: `tests/test_mart_hash.py`

**Interfaces:**
- Consumes: Task 1의 `MartTarget`, `mart_logical_hash`
- Produces: `mismatched_relations(left: dict[str, str], right: dict[str, str]) -> tuple[str, ...]`, `describe_mart_difference(left_path: Path, right_path: Path, target: MartTarget, *, limit: int = DIFF_LIMIT) -> str`, `target_for(relation: str) -> MartTarget`, 상수 `DIFF_LIMIT = 20`

- [ ] **Step 1: 실패 Test 작성**

`tests/test_mart_hash.py`에 붙인다. Import에 `from src.warehouse.mart_hash import describe_mart_difference, mismatched_relations, target_for`를 추가한다.

```python
def test_mismatched_relations_reports_changed_and_missing_entries() -> None:
    """값이 다르거나 한쪽에만 있는 Relation 이름을 정렬해 돌려준다."""
    left = {"facts.fact_orders": "aaa", "facts.fact_payments": "bbb", "dimensions.dim_date": "ccc"}
    right = {"facts.fact_orders": "aaa", "facts.fact_payments": "zzz"}

    assert mismatched_relations(left, right) == ("dimensions.dim_date", "facts.fact_payments")


def test_describe_mart_difference_separates_missing_keys_from_changed_keys(tmp_path) -> None:
    """한쪽에만 있는 Key와 값이 다른 Key를 나눠서 보고한다."""
    left_path = tmp_path / "left.duckdb"
    right_path = tmp_path / "right.duckdb"
    _write_sample_database(left_path, [(1, "a"), (2, "b"), (3, "c")])
    _write_sample_database(right_path, [(1, "a"), (2, "CHANGED")])

    report = describe_mart_difference(left_path, right_path, MartTarget("main", "sample", ("id",)))

    assert "left 3 rows, right 2 rows" in report
    assert "only in left (1): 3" in report
    assert "only in right (0): -" in report
    assert "changed (1): 2" in report
    assert "CHANGED" in report


def test_target_for_returns_the_declared_target() -> None:
    """Relation 이름으로 Hash 대상 정의를 찾는다."""
    assert target_for("facts.fact_orders").order_by == ("order_id",)

    with pytest.raises(KeyError):
        target_for("facts.fact_unknown")


def _write_sample_database(path: Path, rows: list[tuple[int, str]]) -> None:
    """진단 Test용 Sample Table을 가진 DuckDB 파일을 만든다."""
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE TABLE sample (id INTEGER, label VARCHAR)")
        connection.executemany("INSERT INTO sample VALUES (?, ?)", rows)
```

- [ ] **Step 2: Test 실행해 실패 확인**

Run: `uv run pytest tests/test_mart_hash.py -v`
Expected: FAIL with "ImportError: cannot import name 'describe_mart_difference'"

- [ ] **Step 3: 진단 구현**

`src/warehouse/mart_hash.py`의 `ROW_BATCH_SIZE` 아래에 `DIFF_LIMIT = 20`을 추가하고, `_canonical_row_json` 위에 아래 함수를 넣는다.

```python
def target_for(relation: str) -> MartTarget:
    """Relation 이름에 해당하는 Hash 대상 정의를 반환한다."""
    for target in MART_HASH_TARGETS:
        if target.relation == relation:
            return target
    raise KeyError(f"Unknown mart relation: {relation}")


def mismatched_relations(left: dict[str, str], right: dict[str, str]) -> tuple[str, ...]:
    """두 Hash Dict에서 값이 다르거나 한쪽에만 있는 Relation 이름을 정렬해 반환한다."""
    relations = set(left) | set(right)
    return tuple(sorted(name for name in relations if left.get(name) != right.get(name)))


def describe_mart_difference(
    left_path: Path, right_path: Path, target: MartTarget, *, limit: int = DIFF_LIMIT
) -> str:
    """Hash가 다른 Mart의 행 수, 한쪽에만 있는 Key, 값이 다른 Key를 사람이 읽을 형태로 만든다."""
    left_rows = _keyed_rows(left_path, target)
    right_rows = _keyed_rows(right_path, target)
    only_left = sorted(set(left_rows) - set(right_rows))
    only_right = sorted(set(right_rows) - set(left_rows))
    changed = sorted(
        key for key in set(left_rows) & set(right_rows) if left_rows[key] != right_rows[key]
    )
    lines = [
        f"{target.relation}: left {len(left_rows)} rows, right {len(right_rows)} rows",
        f"  only in left ({len(only_left)}): {_format_keys(only_left, limit)}",
        f"  only in right ({len(only_right)}): {_format_keys(only_right, limit)}",
        f"  changed ({len(changed)}): {_format_keys(changed, limit)}",
    ]
    for key in changed[:limit]:
        lines.append(f"  {_format_key(key)} left : {left_rows[key]}")
        lines.append(f"  {_format_key(key)} right: {right_rows[key]}")
    return "\n".join(lines)


def _keyed_rows(warehouse_path: Path, target: MartTarget) -> dict[tuple[str, ...], str]:
    """Mart를 정렬 Key Tuple에서 행 Canonical JSON으로 가는 Dict로 읽는다."""
    order_by = ", ".join(f'"{column}"' for column in target.order_by)
    with duckdb.connect(str(warehouse_path), read_only=True) as connection:
        cursor = connection.execute(f"SELECT * FROM {target.relation} ORDER BY {order_by}")
        column_names = [descriptor[0] for descriptor in cursor.description]
        rows: dict[tuple[str, ...], str] = {}
        for row in cursor.fetchall():
            values = dict(zip(column_names, row, strict=True))
            key = tuple(str(values[column]) for column in target.order_by)
            rows[key] = _canonical_row_json(values)
    return rows


def _format_keys(keys: list[tuple[str, ...]], limit: int) -> str:
    """진단 출력에 넣을 Key 목록을 상한까지만 한 줄로 만든다."""
    if not keys:
        return "-"
    shown = ", ".join(_format_key(key) for key in keys[:limit])
    if len(keys) > limit:
        return f"{shown}, ... (+{len(keys) - limit})"
    return shown


def _format_key(key: tuple[str, ...]) -> str:
    """Key Tuple을 사람이 읽을 한 덩어리 문자열로 만든다."""
    return "|".join(key)
```

- [ ] **Step 4: Test 실행해 통과 확인**

Run: `uv run pytest tests/test_mart_hash.py -v`
Expected: PASS 9건.

- [ ] **Step 5: Commit**

```bash
git add src/warehouse/mart_hash.py tests/test_mart_hash.py
git commit -m "feat: describe mart hash differences"
```

---

### Task 4: CLI

**Files:**
- Modify: `src/warehouse/mart_hash.py`
- Modify: `tests/test_mart_hash.py`

**Interfaces:**
- Consumes: Task 1~3 전부
- Produces: `main(argv: list[str] | None = None) -> int`. `python -m src.warehouse.mart_hash <warehouse>`는 Hash를 JSON으로 출력하고 0을 반환한다. `--compare <other>`를 주면 불일치가 없을 때 0, 있을 때 1을 반환한다.

- [ ] **Step 1: 실패 Test 작성**

`tests/test_mart_hash.py`에 붙인다. Import에 `from src.warehouse.mart_hash import main`을 추가한다.

```python
def test_main_reports_a_mismatch_with_a_non_zero_exit_code(tmp_path, capsys) -> None:
    """비교 Mode는 불일치를 발견하면 진단을 출력하고 1을 반환한다."""
    left_path = tmp_path / "left.duckdb"
    right_path = tmp_path / "right.duckdb"
    _write_sample_database(left_path, [(1, "a")])
    _write_sample_database(right_path, [(1, "CHANGED")])
    target = MartTarget("main", "sample", ("id",))

    exit_code = main(
        [str(left_path), "--compare", str(right_path)],
        targets=(target,),
    )

    assert exit_code == 1
    assert "main.sample" in capsys.readouterr().out


def test_main_prints_hashes_when_no_comparison_is_requested(tmp_path, capsys) -> None:
    """단일 Mode는 Relation별 Hash를 JSON으로 출력하고 0을 반환한다."""
    warehouse_path = tmp_path / "left.duckdb"
    _write_sample_database(warehouse_path, [(1, "a")])
    target = MartTarget("main", "sample", ("id",))

    exit_code = main([str(warehouse_path)], targets=(target,))

    assert exit_code == 0
    assert "main.sample" in capsys.readouterr().out
```

- [ ] **Step 2: Test 실행해 실패 확인**

Run: `uv run pytest tests/test_mart_hash.py -v`
Expected: FAIL with "ImportError: cannot import name 'main'"

- [ ] **Step 3: `targets` 인자 추가와 CLI 구현**

`mart_logical_hashes`를 대상 주입이 가능하도록 고친다. Test가 실제 Mart 없이도 CLI를 돌릴 수 있어야 한다.

```python
def mart_logical_hashes(
    warehouse_path: Path, targets: tuple[MartTarget, ...] = MART_HASH_TARGETS
) -> dict[str, str]:
    """Hash 대상 Mart 전부를 Relation 이름 기준 Hash Dict로 반환한다."""
    with duckdb.connect(str(warehouse_path), read_only=True) as connection:
        return {target.relation: mart_logical_hash(connection, target) for target in targets}
```

파일 끝에 CLI를 추가하고, 파일 상단 Import에 `import argparse`와 `import sys`를 넣는다.

```python
def main(
    argv: list[str] | None = None, *, targets: tuple[MartTarget, ...] = MART_HASH_TARGETS
) -> int:
    """Mart Hash를 출력하거나 두 Warehouse의 Hash를 비교한다."""
    parser = argparse.ArgumentParser(description="Compare mart logical hashes between warehouses")
    parser.add_argument("warehouse", type=Path, help="Warehouse DuckDB file to hash")
    parser.add_argument(
        "--compare", type=Path, default=None, help="Second warehouse to compare against"
    )
    arguments = parser.parse_args(argv)

    left = mart_logical_hashes(arguments.warehouse, targets)
    if arguments.compare is None:
        print(json.dumps(left, ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    right = mart_logical_hashes(arguments.compare, targets)
    mismatched = mismatched_relations(left, right)
    if not mismatched:
        print(f"All {len(left)} mart hashes match")
        return 0

    print(f"Mismatched marts: {', '.join(mismatched)}")
    for relation in mismatched:
        target = next(item for item in targets if item.relation == relation)
        print(describe_mart_difference(arguments.warehouse, arguments.compare, target))
    return 1


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Test 실행해 통과 확인**

Run: `uv run pytest tests/test_mart_hash.py -v`
Expected: PASS 11건.

- [ ] **Step 5: Commit**

```bash
git add src/warehouse/mart_hash.py tests/test_mart_hash.py
git commit -m "feat: add mart hash comparison cli"
```

---

### Task 5: Incremental과 Full Refresh 비교 통합 Test

**Files:**
- Create: `tests/integration/test_incremental_full_refresh_hash_integration.py`

**Interfaces:**
- Consumes: Task 1~4 전부와 `tests/integration/test_subscription_payment_temporal_join_integration.py`의 Fixture Helper
- Produces: 없음. `P6-23`의 합격 증거다.

이 Test는 기존 구독 Fixture Helper를 그대로 Import해 쓴다. Helper를 공용 Module로 옮기지 않는 이유는 그 파일에서 developer가 `P6-10` / `P6-22` 통합 E2E를 돌리고 있어 같은 파일을 동시에 고치면 충돌하기 때문이다. Helper 이동은 그 검증이 끝난 뒤 별도로 다룬다.

`--full-refresh` Build Runner는 이 파일 안에 따로 둔다. 기존 `_run_dbt_build`는 Flag를 받지 않으며, 같은 이유로 지금 고치지 않는다.

- [ ] **Step 1: Test 파일 작성**

`tests/integration/test_incremental_full_refresh_hash_integration.py`:

```python
"""Incremental Build 결과가 같은 입력의 Full Refresh와 논리적으로 같은지 검증한다."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from src.common.database import PostgresSettings
from src.generator.customers import (
    ensure_membership_tier_records,
    ensure_subscription_records,
    new_customer_record,
    new_membership_tier_record,
    new_subscription_record,
    persist_customer_records,
)
from src.generator.subscription_payments import (
    persist_subscription_payments,
    plan_subscription_payment,
)
from src.ingestion.metadata import CursorPosition
from src.ingestion.service import TableIngestionResult
from src.ingestion.storage import SeaweedFSSettings
from src.warehouse.mart_hash import describe_mart_difference, mart_logical_hashes, target_for
from tests.integration.test_subscription_payment_temporal_join_integration import (
    FIXTURE_START,
    _append_fixture_catalog,
    _cleanup,
    _combined_output,
    _create_fixture_catalog,
    _generator_config,
    _ingest,
    _run_dbt_build,
    _set_watermark,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
def test_incremental_marts_match_a_full_refresh_of_the_same_input(tmp_path) -> None:
    """지연 도착 Batch를 Incremental로 반영한 Mart가 같은 입력의 Full Refresh와 Hash까지 같다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    pipeline_name = f"test_full_refresh_hash_{uuid.uuid4().hex}"
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
    incremental_path = tmp_path / "warehouse.duckdb"
    full_refresh_path = tmp_path / "warehouse-full-refresh.duckdb"

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

        _create_fixture_catalog(postgres, incremental_path, results)
        first_build = _run_dbt_build(incremental_path, storage, tmp_path)
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
        _append_fixture_catalog(postgres, incremental_path, late_results)
        second_build = _run_dbt_build(incremental_path, storage, tmp_path)
        assert second_build.returncode == 0, _combined_output(second_build)

        shutil.copy2(incremental_path, full_refresh_path)
        full_refresh_build = _run_full_refresh_build(full_refresh_path, storage, tmp_path)
        assert full_refresh_build.returncode == 0, _combined_output(full_refresh_build)

        incremental_hashes = mart_logical_hashes(incremental_path)
        full_refresh_hashes = mart_logical_hashes(full_refresh_path)
        mismatched = [
            relation
            for relation in incremental_hashes
            if incremental_hashes[relation] != full_refresh_hashes[relation]
        ]
        report = "\n".join(
            describe_mart_difference(incremental_path, full_refresh_path, target_for(relation))
            for relation in mismatched
        )

        assert mismatched == [], report
        assert len(incremental_hashes) == 9
    finally:
        _cleanup(postgres, storage, pipeline_name, results, customer.customer_unique_id)


def _run_full_refresh_build(
    warehouse_path: Path, storage: SeaweedFSSettings, tmp_path: Path
) -> subprocess.CompletedProcess[str]:
    """복사한 Warehouse에만 Full Refresh Build를 실행한다."""
    environment = {
        **os.environ,
        "WAREHOUSE_PATH": str(warehouse_path),
        "SEAWEEDFS_HOST": storage.host,
        "SEAWEEDFS_S3_PORT": str(storage.port),
        "SEAWEEDFS_BUCKET": storage.bucket,
        "SEAWEEDFS_ACCESS_KEY": storage.access_key,
        "SEAWEEDFS_SECRET_KEY": storage.secret_key,
    }
    return subprocess.run(
        [
            str(Path(sys.executable).with_name("dbt")),
            "build",
            "--full-refresh",
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt",
            "--target-path",
            str(tmp_path / "dbt-target-full-refresh"),
        ],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
```

- [ ] **Step 2: Test 실행**

컨테이너를 띄운 뒤 실행한다.

Run: `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/integration/test_incremental_full_refresh_hash_integration.py -v`
Expected: PASS.

불일치가 나면 실패 메시지의 진단을 읽는다. 해석은 다음과 같다.

- `only in left`: Incremental Mart에만 있는 행이다. 원천에서 사라진 자식 행이 교체되지 않고 남았다는 뜻이다. 교체 단위(`unique_key`)를 확인한다.
- `only in right`: Full Refresh에만 있는 행이다. 영향 Key가 그 행을 놓쳤다는 뜻이다. `int_affected_*` 모델의 조건과 Watermark 경계를 확인한다.
- `changed`: 두 쪽 모두 있으나 값이 다르다. 시점 결합이 재배열된 구간을 영향 Key가 놓쳤다는 뜻이다.

어느 경우든 임의로 고치지 말고 architect에 판단을 요청한다. 세 가지 모두 설계 문서 `docs/architecture/06-late-arrival-affected-keys-and-incremental.md`의 결정과 직접 얽힌다.

- [ ] **Step 3: 전체 회귀 실행**

Run: `uv run pytest -m "not integration" -q`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/test_incremental_full_refresh_hash_integration.py
git commit -m "test: compare incremental and full refresh marts"
```

---

## 완료 조건

- `P6-23`: Mart 9개의 Logical Hash가 Incremental과 Full Refresh에서 같고, 그 비교가 Test로 자동 실행된다.
- `docs/phases/phase-06-dimensional-modeling.md`의 `P6-23` Checkbox와 DoD "Incremental과 Full Refresh의 Logical Hash가 같다"는 **아키텍트가 검수한 뒤** 갱신한다. developer는 건드리지 않는다.

## 이 계획의 범위 밖

- `docs/reference/mart-grain.md`와 구현의 계약 불일치 (`product_key`, `seller_key`, `full_date`, `fct_*` 이름). 설계 문서 7장에 정리했고 lead 판단을 기다린다.
- `dim_product`, `dim_seller`, `dim_date`의 Primary Key Test 부재
- `P6-24` Bronze Replay와 Re-extract 입력 경계
- `P6-25` Phase 4 DAG Build 경계 문서 마감
