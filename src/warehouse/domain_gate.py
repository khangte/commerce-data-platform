"""dbt 실패를 도메인에 귀속하고, 그 도메인만 직전 게시 값으로 되돌린 뒤 경계를 다시 검증한다."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import duckdb

# 도메인은 dbt Group(dbt/models/marts/groups.yml)이다. 여기 없는 Group은 되돌릴 Key가 없어 전체 차단이다.
AFFECTED_KEY_DOMAIN = {"orders": "order", "subscriptions": "subscription_payment"}
SHARED_GROUP = "shared"

_NON_MART_PREFIXES = ("stg_", "int_")
_EXCLUDED = ""


@dataclass(frozen=True)
class DomainTable:
    """Manifest의 Mart Group으로 만든 도메인별 Model 이름과 공유 Model 이름이다."""

    domains: dict[str, frozenset[str]]
    shared: frozenset[str]


def is_mart(node: dict) -> bool:
    """staging·intermediate가 아닌 Model을 Mart로 본다."""
    return node["resource_type"] == "model" and not node["name"].startswith(_NON_MART_PREFIXES)


def domain_table(nodes: dict) -> DomainTable:
    """Mart Model의 config.group으로 도메인 표를 만든다.

    Group이 없거나 AFFECTED_KEY_DOMAIN에 없는 Group의 Mart는 어느 쪽에도 넣지 않는다. 그 Model이
    실패·건너뜀에 걸리면 귀속되지 않아 전체 차단이 된다. 선언 누락이 덜 막는 쪽으로 가지 않게 한다.
    """
    members: dict[str, set[str]] = {domain: set() for domain in AFFECTED_KEY_DOMAIN}
    shared: set[str] = set()
    for node in nodes.values():
        if not is_mart(node):
            continue
        group = node["config"].get("group")
        if group == SHARED_GROUP:
            shared.add(node["name"])
        elif group in members:
            members[group].add(node["name"])
    return DomainTable({domain: frozenset(names) for domain, names in members.items()}, frozenset(shared))


def attribute_failure(target_dir: Path) -> str | None:
    """실패 원인을 도메인 하나로 귀속하고, 귀속할 수 없으면 None(전체 차단)을 반환한다.

    skipped는 원인이 아니라 결과라서 Mart 밖 Node는 제외한다. 건너뛴 intermediate를 다른 도메인이
    쓰면 그 Mart Model도 skipped가 되어 여기서 걸린다. 산출물을 해석할 수 없으면 막는 쪽을 택한다.
    """
    try:
        results = _read_json(target_dir / "run_results.json")["results"]
        nodes = _read_json(target_dir / "manifest.json")["nodes"]
        table = domain_table(nodes)
        owners = {
            _owner(result["status"], nodes[result["unique_id"]], table)
            for result in results
            if result["status"] in ("error", "fail", "skipped")
        }
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return None
    owners.discard(_EXCLUDED)
    return owners.pop() if len(owners) == 1 else None


def revert_domain(
    build_path: Path, published_path: Path, domain: str, target_dir: Path, invocation_id: str
) -> None:
    """도메인 Relation과 이번 실행의 affected_keys를 한 Transaction에서 직전 게시 상태로 되돌린다.

    Published 파일이나 Relation이 없으면 duckdb 예외가 나고, Commit 전이므로 Build는 그대로다.
    """
    nodes = _read_json(target_dir / "manifest.json")["nodes"]
    models = {node["name"]: node for node in nodes.values() if node["resource_type"] == "model"}
    with duckdb.connect(str(build_path)) as connection:
        escaped_path = str(published_path).replace("'", "''")
        connection.execute(f"ATTACH '{escaped_path}' AS pub (READ_ONLY)")
        connection.execute("BEGIN")
        for name in sorted(domain_table(nodes).domains[domain]):
            relation = _relation(models[name])
            connection.execute(f"CREATE OR REPLACE TABLE {relation} AS SELECT * FROM pub.{relation}")
        connection.execute(
            "DELETE FROM control.affected_keys WHERE invocation_id = ? AND affected_domain = ?",
            [invocation_id, AFFECTED_KEY_DOMAIN[domain]],
        )
        connection.execute("COMMIT")
        connection.execute("DETACH pub")


def boundary_failures(build_path: Path, domain: str, target_dir: Path) -> tuple[str, ...]:
    """보류 도메인과 공유 Model을 함께 참조하는 error Test 중 실패한 Node ID를 반환한다.

    되돌린 값은 게시 당시 통과했으므로 새로 깨질 수 있는 것은 공유 Dimension과의 경계뿐이다.
    dbt가 건너뛴 relationships Test도 검증하려고 SQL을 Manifest에서 만든다.
    """
    nodes = _read_json(target_dir / "manifest.json")["nodes"]
    table = domain_table(nodes)
    compiled = {
        result["unique_id"]: result.get("compiled_code")
        for result in _read_json(target_dir / "run_results.json")["results"]
    }
    failures = []
    with duckdb.connect(str(build_path), read_only=True) as connection:
        # dbt Profile과 같은 Session 시간대로 맞춰 시각 비교 Test의 결과를 동일하게 유지한다.
        connection.execute("SET TimeZone = 'UTC'")
        for unique_id, node in nodes.items():
            if not _is_boundary_test(node, domain, table):
                continue
            sql = _relationship_sql(node, nodes)
            if sql is None and compiled.get(unique_id):
                sql = f"SELECT count(*) FROM ({compiled[unique_id]}) dbt_internal_test"
            if sql is None or connection.execute(sql).fetchone()[0] > 0:
                failures.append(unique_id)
    return tuple(failures)


def _owner(status: str, node: dict, table: DomainTable) -> str | None:
    """실패·건너뜀 Node 하나의 소유 도메인, 제외 표식, 또는 차단(None)을 반환한다."""
    if node["resource_type"] == "test":
        dependencies = node["depends_on"]["nodes"]
        models = {dep.split(".")[-1] for dep in dependencies if dep.startswith("model.")}
        if status == "skipped" and all(name.startswith(_NON_MART_PREFIXES) for name in models):
            return _EXCLUDED
        return _single_domain(models, table) if len(models) == len(dependencies) else None
    if node["resource_type"] == "model" and status == "skipped":
        if node["name"].startswith(_NON_MART_PREFIXES):
            return _EXCLUDED
        return _single_domain({node["name"]}, table)
    return None


def _single_domain(models: set[str], table: DomainTable) -> str | None:
    """Model 집합이 한 도메인과 공유 Model로만 이루어졌을 때 그 도메인을 반환한다."""
    owners = [domain for domain, members in table.domains.items() if models & members]
    if len(owners) == 1 and models <= table.domains[owners[0]] | table.shared:
        return owners[0]
    return None


def _is_boundary_test(node: dict, domain: str, table: DomainTable) -> bool:
    """보류 도메인과 공유 Model을 모두 참조하는 error 등급 Test인지 판단한다."""
    if node["resource_type"] != "test" or node["config"]["severity"].lower() != "error":
        return False
    models = {dep.split(".")[-1] for dep in node["depends_on"]["nodes"] if dep.startswith("model.")}
    return bool(models & table.domains[domain]) and bool(models & table.shared)


def _relationship_sql(node: dict, nodes: dict) -> str | None:
    """relationships Test의 위반 건수 SQL을 만들고, 만들 수 없으면 None을 반환한다."""
    metadata = node.get("test_metadata") or {}
    if metadata.get("name") != "relationships" or node["config"].get("where"):
        return None
    child_id = node.get("attached_node")
    parents = [dep for dep in node["depends_on"]["nodes"] if dep != child_id]
    if child_id is None or len(parents) != 1:
        return None
    kwargs = metadata["kwargs"]
    # NOT IN은 부모에 NULL이 있으면 위반을 놓치므로 NOT EXISTS를 쓴다.
    return (
        f'SELECT count(*) FROM {_relation(nodes[child_id])} child'
        f' WHERE child."{kwargs["column_name"]}" IS NOT NULL AND NOT EXISTS ('
        f'SELECT 1 FROM {_relation(nodes[parents[0]])} parent'
        f' WHERE parent."{kwargs["field"]}" = child."{kwargs["column_name"]}")'
    )


def _relation(node: dict) -> str:
    """Manifest Node의 Schema와 Alias로 따옴표 친 Relation 이름을 만든다."""
    return f'"{node["schema"]}"."{node["alias"]}"'


def _read_json(path: Path) -> dict:
    """dbt 산출물 JSON 파일을 읽는다."""
    return json.loads(path.read_text())
