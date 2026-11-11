"""도메인 경계 기준(ADR 021 R1·R2)이 dbt Group 선언과 맞는지 Manifest 계보로 단언한다."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from src.warehouse.dbt_runner import DBT_PROJECT_DIR, _default_dbt_executable
from src.warehouse.domain_gate import DOMAIN_TIME_COLUMN, SHARED_GROUP, is_mart

PROJECT_ROOT = DBT_PROJECT_DIR.parent


def mart_components(nodes: dict) -> set[frozenset[str]]:
    """shared Group을 뺀 Mart Model을 ref 하위 닫힘·error Test 공동 참조로 이은 연결 요소를 반환한다.

    ref는 staging·intermediate를 거쳐 가장 가까운 상위 Mart까지 잇는다(R1). error Test 하나가 함께
    참조하는 Mart끼리 잇는다(R2). 공유 Model을 거치는 간선은 끊는다.
    """
    models = {uid: node for uid, node in nodes.items() if node["resource_type"] == "model"}
    marts = {
        uid for uid, node in models.items()
        if is_mart(node) and node["config"].get("group") != SHARED_GROUP
    }
    parent = {uid: uid for uid in marts}

    def find(uid: str) -> str:
        while parent[uid] != uid:
            uid = parent[uid]
        return uid

    def union(left: str, right: str) -> None:
        parent[find(left)] = find(right)

    def nearest_marts(uid: str, seen: set[str]) -> set[str]:
        """상위 Model을 따라가며 Mart에서 멈춘다. 공유 Mart도 Mart라서 그 너머로 가지 않는다."""
        found = set()
        for dep in models[uid]["depends_on"]["nodes"]:
            if dep not in models or dep in seen:
                continue
            seen.add(dep)
            found |= {dep} if is_mart(models[dep]) else nearest_marts(dep, seen)
        return found

    for uid in marts:
        for upstream in nearest_marts(uid, set()) & marts:
            union(uid, upstream)
    for node in nodes.values():
        if node["resource_type"] != "test" or node["config"]["severity"].lower() != "error":
            continue
        referenced = [dep for dep in node["depends_on"]["nodes"] if dep in marts]
        for other in referenced[1:]:
            union(referenced[0], other)

    components: dict[str, set[str]] = {}
    for uid in marts:
        components.setdefault(find(uid), set()).add(models[uid]["name"])
    return {frozenset(names) for names in components.values()}


def declared_groups(nodes: dict) -> set[frozenset[str]]:
    """shared가 아닌 Group마다 소속 Mart 이름 집합을 반환한다. Group이 없는 Mart는 빠진다."""
    groups: dict[str, set[str]] = {}
    for node in nodes.values():
        group = node["config"].get("group") if node["resource_type"] == "model" else None
        if group and group != SHARED_GROUP and is_mart(node):
            groups.setdefault(group, set()).add(node["name"])
    return {frozenset(names) for names in groups.values()}


def assert_groups_match_lineage(nodes: dict) -> None:
    """계보로 계산한 연결 요소와 선언한 Group이 정확히 같지 않으면 실패한다."""
    assert mart_components(nodes) == declared_groups(nodes)


def _model(name: str, group: str | None, *upstream: str) -> tuple[str, dict]:
    config = {"group": group} if group else {}
    node = {
        "resource_type": "model",
        "name": name,
        "config": config,
        "depends_on": {"nodes": [f"model.p.{dep}" for dep in upstream]},
    }
    return f"model.p.{name}", node


def _test(name: str, *models: str, severity: str = "ERROR") -> tuple[str, dict]:
    node = {
        "resource_type": "test",
        "name": name,
        "config": {"severity": severity},
        "depends_on": {"nodes": [f"model.p.{model}" for model in models]},
    }
    return f"test.p.{name}", node


def _manifest(**overrides: str | None) -> dict:
    """주문·구독·공유 축소판. overrides로 Model의 Group을 바꾼다."""
    groups = {
        "dim_customer": "shared",
        "fct_order": "orders",
        "fct_order_item": "orders",
        "rpt_orders": "orders",
        "fct_sub": "subscriptions",
        "rpt_sub": "subscriptions",
    } | overrides
    return dict(
        [
            _model("stg_orders", None),
            _model("int_customer", None),
            _model("dim_customer", groups["dim_customer"], "int_customer"),
            _model("fct_order", groups["fct_order"], "stg_orders", "dim_customer"),
            _model("fct_order_item", groups["fct_order_item"], "stg_orders"),
            _model("rpt_orders", groups["rpt_orders"], "fct_order", "dim_customer"),
            _model("fct_sub", groups["fct_sub"], "dim_customer"),
            _model("rpt_sub", groups["rpt_sub"], "fct_sub"),
            _test("order_item_total", "fct_order", "fct_order_item"),
            _test("fk_order_customer", "fct_order", "dim_customer"),
            _test("fk_sub_customer", "fct_sub", "dim_customer"),
            _test("warn_across", "fct_order", "fct_sub", severity="warn"),
        ]
    )


def test_components_cut_at_shared_and_follow_staging() -> None:
    """공유 Model에서 끊기고, staging을 거친 ref와 error Test 공동 참조로 이어진다."""
    assert mart_components(_manifest()) == {
        frozenset({"fct_order", "fct_order_item", "rpt_orders"}),
        frozenset({"fct_sub", "rpt_sub"}),
    }
    assert_groups_match_lineage(_manifest())


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"rpt_orders": "subscriptions"}, id="ref-child-in-other-group"),
        pytest.param({"fct_order_item": "subscriptions"}, id="test-partner-in-other-group"),
        pytest.param({"rpt_sub": None}, id="ungrouped-mart"),
        pytest.param({"dim_customer": "orders"}, id="shared-model-moved-into-domain"),
    ],
)
def test_wrong_membership_fails(overrides: dict[str, str | None]) -> None:
    """Model 하나를 다른 Group으로 옮기거나 Group을 빼면 기준 단언이 실패한다."""
    with pytest.raises(AssertionError):
        assert_groups_match_lineage(_manifest(**overrides))


def test_error_test_across_domains_merges_components() -> None:
    """두 도메인을 함께 참조하는 error Test가 생기면 연결 요소가 하나로 합쳐져 실패한다."""
    nodes = _manifest() | dict([_test("across", "fct_order", "fct_sub")])
    with pytest.raises(AssertionError):
        assert_groups_match_lineage(nodes)


def test_real_manifest_groups_match_lineage(tmp_path: Path) -> None:
    """dbt parse로 만든 실제 Manifest에서 선언한 Group이 계보 기준과 같다."""
    subprocess.run(
        [
            _default_dbt_executable(), "parse",
            "--project-dir", str(DBT_PROJECT_DIR),
            "--profiles-dir", str(DBT_PROJECT_DIR),
            "--target-path", str(tmp_path),
        ],
        cwd=PROJECT_ROOT,
        env={**os.environ, "WAREHOUSE_PATH": str(tmp_path / "parse.duckdb")},
        check=True,
        capture_output=True,
    )
    nodes = json.loads((tmp_path / "manifest.json").read_text())["nodes"]
    assert len(declared_groups(nodes)) == 2
    assert_groups_match_lineage(nodes)
    groups = {node["config"].get("group") for node in nodes.values() if is_mart(node)}
    assert groups - {SHARED_GROUP} == DOMAIN_TIME_COLUMN.keys()
