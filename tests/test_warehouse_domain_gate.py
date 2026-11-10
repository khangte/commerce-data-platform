"""도메인 귀속·되돌림·경계 Test 재검증을 합성 DuckDB 파일과 dbt 산출물로 검증한다."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from src.warehouse.domain_gate import (
    DOMAINS,
    SHARED,
    attribute_failure,
    boundary_failures,
    revert_domain,
)
from src.warehouse.mart_hash import MartTarget, mart_logical_hash

SCHEMAS = {"dim": "dimensions", "fct": "facts", "rpt": "metrics"}
INVOCATION = "invocation-now"


def _model_id(name: str) -> str:
    return f"model.p.{name}"


def _test_id(name: str) -> str:
    return f"test.p.{name}"


def _schema(name: str) -> str:
    return SCHEMAS[name.split("_")[0]]


def _model(name: str) -> dict:
    prefix = name.split("_")[0]
    return {
        "resource_type": "model",
        "name": name,
        "alias": name,
        "schema": SCHEMAS.get(prefix, "staging"),
        "depends_on": {"nodes": []},
        "config": {},
    }


def _test(name: str, *models: str, severity: str = "ERROR") -> dict:
    return {
        "resource_type": "test",
        "name": name,
        "depends_on": {"nodes": [_model_id(model) for model in models]},
        "config": {"severity": severity, "where": None},
    }


def _relationship(
    name: str, child: str, parent: str, column: str, field: str, *, where: str | None = None
) -> dict:
    node = _test(name, parent, child)
    node["attached_node"] = _model_id(child)
    node["test_metadata"] = {
        "name": "relationships",
        "kwargs": {"column_name": column, "to": f"ref('{parent}')", "field": field},
    }
    node["config"]["where"] = where
    return node


def _write_target(
    target: Path,
    nodes: dict[str, dict],
    results: dict[str, str] | None = None,
    compiled: dict[str, str] | None = None,
) -> Path:
    """manifest.json과 run_results.json을 쓴다. results는 Node ID별 상태다."""
    target.mkdir(parents=True, exist_ok=True)
    (target / "manifest.json").write_text(json.dumps({"nodes": nodes}))
    rows = [
        {"unique_id": unique_id, "status": status, "compiled_code": (compiled or {}).get(unique_id)}
        for unique_id, status in (results or {}).items()
    ]
    (target / "run_results.json").write_text(json.dumps({"results": rows}))
    return target


def _attribute(tmp_path: Path, nodes: dict[str, dict], results: dict[str, str]) -> str | None:
    return attribute_failure(_write_target(tmp_path / "target", nodes, results))


def _all_models() -> dict[str, dict]:
    names = (*SHARED, *DOMAINS["orders"], *DOMAINS["subscriptions"])
    return {_model_id(name): _model(name) for name in names}


FCT_ORDER_TEST = _test("t_order", "fct_order")
FCT_SUB_TEST = _test("t_sub", "fct_subscription_payment")


@pytest.mark.parametrize(
    ("nodes", "results", "expected"),
    [
        pytest.param({_test_id("a"): FCT_ORDER_TEST}, {_test_id("a"): "fail"}, "orders", id="domain-test"),
        pytest.param(
            {_test_id("a"): _test("a", "fct_order", "dim_customer")},
            {_test_id("a"): "fail"},
            "orders",
            id="domain-plus-shared-test",
        ),
        pytest.param(
            {_test_id("a"): FCT_ORDER_TEST, _model_id("rpt_membership_tier_performance"): _model("rpt_membership_tier_performance")},
            {_test_id("a"): "fail", _model_id("rpt_membership_tier_performance"): "skipped"},
            "orders",
            id="skipped-domain-model",
        ),
        pytest.param(
            {_test_id("a"): FCT_ORDER_TEST, _test_id("b"): _test("b", "fct_order_item", "dim_date")},
            {_test_id("a"): "fail", _test_id("b"): "skipped"},
            "orders",
            id="skipped-domain-test",
        ),
        pytest.param(
            {_test_id("a"): FCT_ORDER_TEST, _model_id("dim_customer"): _model("dim_customer")},
            {_test_id("a"): "fail", _model_id("dim_customer"): "skipped"},
            None,
            id="skipped-shared-model",
        ),
        pytest.param(
            {_test_id("a"): FCT_ORDER_TEST, _test_id("b"): _test("b", "dim_customer")},
            {_test_id("a"): "fail", _test_id("b"): "skipped"},
            None,
            id="skipped-shared-only-test",
        ),
        pytest.param(
            {_test_id("a"): FCT_ORDER_TEST, _test_id("b"): FCT_SUB_TEST},
            {_test_id("a"): "fail", _test_id("b"): "skipped"},
            None,
            id="skipped-test-on-other-domain",
        ),
        pytest.param(
            {_test_id("a"): FCT_ORDER_TEST, _model_id("int_x"): _model("int_x"), _model_id("stg_y"): _model("stg_y")},
            {_test_id("a"): "fail", _model_id("int_x"): "skipped", _model_id("stg_y"): "skipped"},
            "orders",
            id="skipped-staging-intermediate-excluded",
        ),
        pytest.param(
            {_test_id("a"): FCT_ORDER_TEST, _test_id("b"): _test("b", "int_x", "stg_y")},
            {_test_id("a"): "fail", _test_id("b"): "skipped"},
            "orders",
            id="skipped-non-mart-test-excluded",
        ),
        pytest.param(
            {_test_id("a"): FCT_ORDER_TEST, _test_id("b"): _test("b", "fct_order", "int_x")},
            {_test_id("a"): "fail", _test_id("b"): "skipped"},
            None,
            id="skipped-test-mixing-mart-and-intermediate",
        ),
        pytest.param(
            {_model_id("rpt_new"): _model("rpt_new"), _test_id("a"): FCT_ORDER_TEST},
            {_test_id("a"): "fail", _model_id("rpt_new"): "skipped"},
            None,
            id="skipped-model-not-in-domain-table",
        ),
        pytest.param(
            {_model_id("fct_order"): _model("fct_order")}, {_model_id("fct_order"): "error"}, None, id="error-model"
        ),
        pytest.param(
            {"seed.p.s": {"resource_type": "seed", "name": "s"}}, {"seed.p.s": "error"}, None, id="error-seed"
        ),
        pytest.param(
            {"snapshot.p.s": {"resource_type": "snapshot", "name": "s"}},
            {"snapshot.p.s": "error"},
            None,
            id="error-snapshot",
        ),
        pytest.param({_test_id("a"): _test("a")}, {_test_id("a"): "fail"}, None, id="test-without-dependency"),
        pytest.param(
            {_test_id("a"): _test("a", "dim_customer", "dim_date")}, {_test_id("a"): "fail"}, None, id="test-shared-only"
        ),
        pytest.param({_test_id("a"): _test("a", "stg_y")}, {_test_id("a"): "fail"}, None, id="test-on-staging"),
        pytest.param(
            {_test_id("a"): _test("a", "fct_order", "int_x")}, {_test_id("a"): "fail"}, None, id="test-domain-and-intermediate"
        ),
        pytest.param(
            {_test_id("a"): _test("a", "fct_order", "fct_subscription_payment")},
            {_test_id("a"): "fail"},
            None,
            id="test-across-domains",
        ),
        pytest.param(
            {_test_id("a"): FCT_ORDER_TEST, _test_id("b"): FCT_SUB_TEST},
            {_test_id("a"): "fail", _test_id("b"): "fail"},
            None,
            id="two-domains-failed",
        ),
        pytest.param(
            {_test_id("a"): FCT_ORDER_TEST, _test_id("b"): _test("b", "dim_date", "rpt_new")},
            {_test_id("a"): "fail", _test_id("b"): "fail"},
            None,
            id="test-on-mart-outside-table",
        ),
        pytest.param({_test_id("a"): FCT_ORDER_TEST}, {_test_id("a"): "warn"}, None, id="warn-is-not-attributed"),
        pytest.param(
            {_test_id("a"): FCT_ORDER_TEST, _test_id("b"): FCT_SUB_TEST},
            {_test_id("a"): "fail", _test_id("b"): "warn"},
            "orders",
            id="warn-on-other-domain-ignored",
        ),
        pytest.param({}, {_test_id("gone"): "fail"}, None, id="node-missing-from-manifest"),
        pytest.param({_model_id("int_x"): _model("int_x")}, {_model_id("int_x"): "skipped"}, None, id="only-excluded-skips"),
    ],
)
def test_attribute_failure_follows_the_table(
    tmp_path: Path, nodes: dict[str, dict], results: dict[str, str], expected: str | None
) -> None:
    """귀속 표의 각 행은 도메인 하나 또는 전체 차단(None)으로 판정된다."""
    assert _attribute(tmp_path, nodes, results) == expected


def test_dim_subscription_failure_with_skipped_intermediate_attributes_to_subscriptions(
    tmp_path: Path,
) -> None:
    """Phase 0의 dim_subscription 실패 구성은 건너뛴 intermediate와 무관하게 구독으로 귀속된다."""
    relationship = _relationship(
        "r", "fct_subscription_payment", "dim_customer", "customer_key", "customer_key"
    )
    nodes = {
        _test_id("canary"): _test("canary", "dim_subscription"),
        _model_id("int_subscription_payments_enriched"): _model("int_subscription_payments_enriched"),
        _model_id("fct_subscription_payment"): _model("fct_subscription_payment"),
        _model_id("rpt_subscription_funnel_daily"): _model("rpt_subscription_funnel_daily"),
        _test_id("not_null"): _test("not_null", "fct_subscription_payment"),
        _test_id("r"): relationship,
    }
    results = {
        _test_id("canary"): "fail",
        _model_id("int_subscription_payments_enriched"): "skipped",
        _model_id("fct_subscription_payment"): "skipped",
        _model_id("rpt_subscription_funnel_daily"): "skipped",
        _test_id("not_null"): "skipped",
        _test_id("r"): "skipped",
    }

    assert _attribute(tmp_path, nodes, results) == "subscriptions"


def test_attribute_failure_is_none_without_artifacts(tmp_path: Path) -> None:
    """산출물이 없으면 귀속하지 않고 전체 차단한다."""
    assert attribute_failure(tmp_path / "missing") is None


def test_attribute_failure_is_none_for_corrupt_artifacts(tmp_path: Path) -> None:
    """해석할 수 없는 산출물은 전체 차단으로 처리한다."""
    target = _write_target(tmp_path / "target", {}, {})
    (target / "manifest.json").write_text("{not json")
    assert attribute_failure(target) is None


def test_attribute_failure_is_none_for_unexpected_manifest_shape(tmp_path: Path) -> None:
    """필수 Key가 빠진 Node도 예외 없이 전체 차단으로 처리한다."""
    nodes = {_test_id("a"): {"resource_type": "test", "name": "a"}}
    assert _attribute(tmp_path, nodes, {_test_id("a"): "fail"}) is None


def _create_warehouse(path: Path, value: str, column_type: str) -> None:
    """Domain·공유 Table과 affected_keys를 가진 합성 Warehouse 파일을 만든다."""
    with duckdb.connect(str(path)) as connection:
        for name in (*SHARED, *DOMAINS["orders"], *DOMAINS["subscriptions"]):
            connection.execute(f"CREATE SCHEMA IF NOT EXISTS {_schema(name)}")
            connection.execute(
                f"CREATE TABLE {_schema(name)}.{name} AS"
                f" SELECT i::{column_type} AS id, '{value}' AS value FROM range(3) t(i)"
            )
        connection.execute("CREATE SCHEMA control")
        connection.execute(
            """
            CREATE TABLE control.affected_keys AS
            SELECT * FROM (VALUES
                ('invocation-now', 'order', 'o1'),
                ('invocation-now', 'subscription_payment', 's1'),
                ('invocation-old', 'order', 'o0')
            ) t(invocation_id, affected_domain, entity_key)
            """
        )


@pytest.fixture
def warehouses(tmp_path: Path) -> tuple[Path, Path, Path]:
    """Build·Published 파일과 Manifest가 있는 Target Directory를 만든다."""
    build, published = tmp_path / "build.duckdb", tmp_path / "published.duckdb"
    _create_warehouse(build, "new", "BIGINT")
    _create_warehouse(published, "old", "INTEGER")
    return build, published, _write_target(tmp_path / "target", _all_models())


def _hash(path: Path, name: str) -> str:
    with duckdb.connect(str(path), read_only=True) as connection:
        return mart_logical_hash(connection, MartTarget(_schema(name), name, ("id",)))


def _affected_keys(path: Path) -> list[tuple[str, str]]:
    with duckdb.connect(str(path), read_only=True) as connection:
        return connection.execute(
            "SELECT invocation_id, affected_domain FROM control.affected_keys ORDER BY ALL"
        ).fetchall()


def _columns(path: Path, name: str) -> list[tuple]:
    with duckdb.connect(str(path), read_only=True) as connection:
        return connection.execute(f"DESCRIBE {_schema(name)}.{name}").fetchall()


def test_revert_restores_only_the_held_domain(warehouses: tuple[Path, Path, Path]) -> None:
    """보류 도메인만 Published 값·타입으로 돌아오고 나머지는 Build 값을 유지한다."""
    build, published, target = warehouses
    other = {name: _hash(build, name) for name in (*SHARED, *DOMAINS["subscriptions"])}

    revert_domain(build, published, "orders", target, INVOCATION)

    for name in DOMAINS["orders"]:
        assert _hash(build, name) == _hash(published, name)
        assert _columns(build, name) == _columns(published, name)
    assert {name: _hash(build, name) for name in other} == other


def test_revert_deletes_only_this_invocation_keys_of_the_held_domain(
    warehouses: tuple[Path, Path, Path],
) -> None:
    """affected_keys에서 이번 실행의 보류 도메인 행만 지운다."""
    build, published, target = warehouses

    revert_domain(build, published, "orders", target, INVOCATION)

    assert _affected_keys(build) == [
        ("invocation-now", "subscription_payment"),
        ("invocation-old", "order"),
    ]


def test_revert_failure_leaves_build_untouched(
    warehouses: tuple[Path, Path, Path],
) -> None:
    """Published에 Relation이 하나라도 없으면 예외를 내고 Build는 되돌림 전 그대로다."""
    build, published, target = warehouses
    missing = DOMAINS["orders"][-1]
    with duckdb.connect(str(published)) as connection:
        connection.execute(f"DROP TABLE {_schema(missing)}.{missing}")
    before = {name: _hash(build, name) for name in DOMAINS["orders"]}
    keys_before = _affected_keys(build)

    with pytest.raises(duckdb.Error):
        revert_domain(build, published, "orders", target, INVOCATION)

    assert {name: _hash(build, name) for name in DOMAINS["orders"]} == before
    assert _affected_keys(build) == keys_before
    assert not build.with_name(f"{build.name}.wal").exists()


def test_revert_without_published_file_raises(warehouses: tuple[Path, Path, Path]) -> None:
    """Published 파일이 없으면 되돌릴 수 없으므로 예외로 전체 차단한다."""
    build, published, target = warehouses
    published.unlink()

    with pytest.raises(duckdb.Error):
        revert_domain(build, published, "orders", target, INVOCATION)


@pytest.fixture
def boundary_build(tmp_path: Path) -> Path:
    """고객 Key 1·2를 가진 Dimension과 이를 참조하는 Fact가 있는 Build 파일을 만든다."""
    path = tmp_path / "boundary.duckdb"
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE SCHEMA dimensions")
        connection.execute("CREATE SCHEMA facts")
        connection.execute(
            "CREATE TABLE dimensions.dim_customer AS"
            " SELECT * FROM (VALUES (1), (2), (NULL)) t(customer_key)"
        )
        connection.execute(
            "CREATE TABLE facts.fct_order AS"
            " SELECT * FROM (VALUES (1), (2), (NULL)) t(customer_key)"
        )
    return path


def _boundary_nodes(*tests: tuple[str, dict]) -> dict[str, dict]:
    return {_model_id(name): _model(name) for name in ("fct_order", "dim_customer")} | dict(tests)


RELATIONSHIP = _relationship("r", "fct_order", "dim_customer", "customer_key", "customer_key")


def _remove_customer(path: Path, key: int) -> None:
    with duckdb.connect(str(path)) as connection:
        connection.execute("DELETE FROM dimensions.dim_customer WHERE customer_key = ?", [key])


def test_boundary_relationship_passes_when_keys_exist(boundary_build: Path, tmp_path: Path) -> None:
    """참조가 모두 살아 있으면 실패가 없고, NULL 자식 값은 실패로 세지 않는다."""
    target = _write_target(
        tmp_path / "t", _boundary_nodes((_test_id("r"), RELATIONSHIP)), {_test_id("r"): "pass"}
    )
    assert boundary_failures(boundary_build, "orders", target) == ()


def test_boundary_relationship_fails_when_shared_key_is_gone(
    boundary_build: Path, tmp_path: Path
) -> None:
    """공유 Dimension에서 Key가 빠지면 부모에 NULL이 있어도 경계 Test 실패로 잡힌다."""
    _remove_customer(boundary_build, 2)
    target = _write_target(
        tmp_path / "t", _boundary_nodes((_test_id("r"), RELATIONSHIP)), {_test_id("r"): "pass"}
    )
    assert boundary_failures(boundary_build, "orders", target) == (_test_id("r"),)


def test_boundary_checks_skipped_relationship_without_compiled_code(
    boundary_build: Path, tmp_path: Path
) -> None:
    """dbt가 건너뛰어 compiled_code가 없는 relationships Test도 Manifest로 검증한다."""
    nodes = _boundary_nodes((_test_id("r"), RELATIONSHIP))
    results = {_test_id("r"): "skipped"}
    target = _write_target(tmp_path / "t", nodes, results)

    assert boundary_failures(boundary_build, "orders", target) == ()
    _remove_customer(boundary_build, 1)
    assert boundary_failures(boundary_build, "orders", target) == (_test_id("r"),)


def test_boundary_relationship_with_where_uses_compiled_code(
    boundary_build: Path, tmp_path: Path
) -> None:
    """where 설정이 있으면 SQL을 만들지 않고 compiled_code 규칙을 따른다."""
    node = _relationship("r", "fct_order", "dim_customer", "customer_key", "customer_key", where="1=1")
    nodes = _boundary_nodes((_test_id("r"), node))

    skipped = _write_target(tmp_path / "skipped", nodes, {_test_id("r"): "skipped"})
    assert boundary_failures(boundary_build, "orders", skipped) == (_test_id("r"),)

    clean = _write_target(
        tmp_path / "clean", nodes, {_test_id("r"): "pass"}, {_test_id("r"): "select 1 where false"}
    )
    assert boundary_failures(boundary_build, "orders", clean) == ()


@pytest.mark.parametrize(
    ("compiled", "failures"),
    [
        pytest.param("select 1 as failure", (_test_id("b"),), id="rows-mean-failure"),
        pytest.param("select 1 as failure where false", (), id="no-rows-pass"),
        pytest.param(None, (_test_id("b"),), id="missing-compiled-code-blocks"),
    ],
)
def test_boundary_runs_compiled_code_for_other_tests(
    boundary_build: Path, tmp_path: Path, compiled: str | None, failures: tuple[str, ...]
) -> None:
    """relationships가 아닌 경계 Test는 compiled_code 결과로 판정하고 없으면 막는다."""
    node = _test("b", "fct_order", "dim_customer")
    code = {_test_id("b"): compiled} if compiled else {}
    target = _write_target(
        tmp_path / "t", _boundary_nodes((_test_id("b"), node)), {_test_id("b"): "pass"}, code
    )
    assert boundary_failures(boundary_build, "orders", target) == failures


def test_full_gate_mode_makes_skipped_metric_test_block_orders(
    boundary_build: Path, tmp_path: Path
) -> None:
    """metrics_test_severity=error면 건너뛴 rpt 대사 Test가 경계 Test가 되어 전체 차단으로 간다."""
    name = "rpt_customer_order_activity_matches_fct_order"
    models = ("rpt_customer_order_activity_daily", "fct_order", "dim_customer")
    nodes = _boundary_nodes(
        (_test_id(name), _test(name, *models, severity="ERROR")),
        (_model_id(models[0]), _model(models[0])),
    )
    target = _write_target(tmp_path / "t", nodes, {_test_id(name): "skipped"})

    assert boundary_failures(boundary_build, "orders", target) == (_test_id(name),)


@pytest.mark.parametrize(
    "node",
    [
        pytest.param(_test("b", "fct_order", "dim_customer", severity="WARN"), id="warn-severity"),
        pytest.param(_test("b", "fct_order"), id="no-shared-model"),
        pytest.param(_test("b", "fct_subscription_payment", "dim_customer"), id="other-domain"),
        pytest.param(_test("b", "dim_customer"), id="shared-only"),
    ],
)
def test_boundary_ignores_non_boundary_tests(
    boundary_build: Path, tmp_path: Path, node: dict
) -> None:
    """보류 도메인과 공유 Model을 함께 참조하는 error Test만 다시 본다."""
    target = _write_target(
        tmp_path / "t", _boundary_nodes((_test_id("b"), node)), {_test_id("b"): "skipped"}
    )
    assert boundary_failures(boundary_build, "orders", target) == ()
