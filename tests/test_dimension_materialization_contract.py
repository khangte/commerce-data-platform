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
