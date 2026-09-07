from __future__ import annotations

from datetime import UTC, datetime

import pytest

from src.seed.contracts import TABLE_CONTRACTS, combined_checksum, validate_input_directory
from src.seed.loader import parse_seeded_at


def test_parse_seeded_at_normalizes_to_utc() -> None:
    assert parse_seeded_at("2026-09-03T09:00:00+09:00") == datetime(2026, 9, 3, tzinfo=UTC)


@pytest.mark.parametrize("value", ("2026-09-03T00:00:00", "not-a-timestamp"))
def test_parse_seeded_at_rejects_invalid_or_naive_values(value: str) -> None:
    with pytest.raises(ValueError):
        parse_seeded_at(value)


def test_header_validation_and_checksum_are_deterministic(tmp_path) -> None:
    for contract in TABLE_CONTRACTS:
        (tmp_path / contract.file_name).write_text(
            ",".join(contract.raw_header) + "\n", encoding="utf-8"
        )

    first = validate_input_directory(tmp_path)
    second = validate_input_directory(tmp_path)

    assert first == second
    assert combined_checksum(first) == combined_checksum(second)


def test_header_validation_rejects_source_schema_drift(tmp_path) -> None:
    for contract in TABLE_CONTRACTS:
        header = contract.raw_header
        if contract.table_name == "customers":
            header = (*header, "unexpected_column")
        (tmp_path / contract.file_name).write_text(
            ",".join(header) + "\n", encoding="utf-8"
        )

    with pytest.raises(ValueError, match="Unexpected header"):
        validate_input_directory(tmp_path)
