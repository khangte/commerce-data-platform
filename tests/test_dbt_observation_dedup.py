"""관측 중복 제거의 선택 순서와 서로 다른 이력 보존을 검증한다."""

from pathlib import Path

import duckdb
import pytest
from jinja2 import Environment, StrictUndefined

DBT_DIR = Path(__file__).resolve().parents[1] / "dbt"


@pytest.mark.parametrize("key_column", ["customer_id", "subscription_id"])
def test_dedup_keeps_latest_ingestion_then_batch_and_distinct_history(key_column: str) -> None:
    """수집 시각·배치 순서로 중복을 선택하고 키·시각·속성이 다른 관측은 보존한다."""
    macros = Environment(undefined=StrictUndefined).from_string(
        (DBT_DIR / "macros/dedupe_observations.sql").read_text(encoding="utf-8")
    ).module
    with duckdb.connect() as connection:
        connection.execute(f"""
            CREATE TABLE observations (
                {key_column} VARCHAR, updated_at TIMESTAMP, attribute_hash VARCHAR,
                _ingested_at TIMESTAMP, _batch_id VARCHAR, payload VARCHAR
            )
        """)
        connection.execute("""
            INSERT INTO observations VALUES
                ('a', '2026-10-01', 'h1', '2026-10-02', 'z', 'older-ingestion'),
                ('a', '2026-10-01', 'h1', '2026-10-03', 'a', 'older-batch'),
                ('a', '2026-10-01', 'h1', '2026-10-03', 'b', 'winner'),
                ('a', '2026-10-01', 'h2', '2026-10-02', 'a', 'other-hash'),
                ('a', '2026-10-02', 'h1', '2026-10-03', 'a', 'other-time'),
                ('b', '2026-10-01', 'h1', '2026-10-02', 'a', 'other-key')
        """)
        connection.execute(
            "CREATE TABLE deduplicated AS "
            + macros.dedupe_observations("observations", key_column)
        )
        assert connection.execute(
            "SELECT payload FROM deduplicated ORDER BY payload"
        ).fetchall() == [("other-hash",), ("other-key",), ("other-time",), ("winner",)]
        assert connection.execute("SELECT * FROM deduplicated WHERE false").description == (
            connection.execute("SELECT * FROM observations WHERE false").description
        )
