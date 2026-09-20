"""검증 Rule Registry가 PRD §11 순서와 validation.py 사용 Code를 따르는지 검증한다."""

from __future__ import annotations

import re
from pathlib import Path

from src.ingestion import rules

VALIDATION_SOURCE = Path(__file__).resolve().parents[2] / "src/ingestion/validation.py"


def test_registry_follows_prd_section_11_order() -> None:
    """Registry 순서와 order 값이 PRD §11 검증 순서와 같다."""
    assert [rule.code for rule in rules.VALIDATION_RULES] == [
        "SCHEMA_MISMATCH", "REQUIRED_NULL", "TYPE_MISMATCH", "KEY_NULL", "BATCH_DUPLICATE",
        "STATUS_DOMAIN_INVALID", "NUMERIC_RANGE_INVALID", "BROKEN_REFERENCE", "CURSOR_OUT_OF_RANGE",
    ]
    assert [rule.order for rule in rules.VALIDATION_RULES] == list(range(1, 10))


def test_batch_rules_are_schema_and_cursor_range_only() -> None:
    """Batch 오류는 Schema 불일치와 Cursor 범위 이탈 두 가지뿐이다."""
    batch_codes = {rule.code for rule in rules.VALIDATION_RULES if rule.scope == rules.BATCH}
    assert batch_codes == {"SCHEMA_MISMATCH", "CURSOR_OUT_OF_RANGE"}
    assert rules.ROW_ERROR_CODES == {rule.code for rule in rules.VALIDATION_RULES if rule.scope == rules.ROW}


def test_validation_module_has_no_error_code_literals() -> None:
    """validation.py는 오류 Code 문자열을 직접 쓰지 않고 Registry만 참조한다."""
    assert re.findall(r'"[A-Z][A-Z_]{3,}"', VALIDATION_SOURCE.read_text(encoding="utf-8")) == []
