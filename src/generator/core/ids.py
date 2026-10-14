"""Generator의 Business ID와 비교용 Hash를 결정적으로 만든다."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime

GENERATOR_ID_NAMESPACE = uuid.UUID("32db4e67-1e8c-59b8-87ed-d69b0518a2e5")


def deterministic_uuid(entity_name: str, *components: object) -> uuid.UUID:
    """Entity 이름과 정규화된 입력으로 UUIDv5 Business ID를 만든다."""
    if not entity_name:
        raise ValueError("entity_name must not be empty")
    return uuid.uuid5(
        GENERATOR_ID_NAMESPACE,
        _canonical_json({"entity_name": entity_name, "components": components}),
    )


def logical_hash(value: object) -> str:
    """순서와 표현이 같은 논리 내용을 SHA-256 Hex 값으로 변환한다."""
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _canonical_json(value: object) -> str:
    """Hash와 UUIDv5 이름에 사용할 안정적인 JSON 표현을 반환한다."""
    return json.dumps(value, default=_json_default, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _json_default(value: object) -> str:
    """JSON 기본 형식에 없는 결정적 값의 표준 문자열 표현을 반환한다."""
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetime values used for deterministic IDs must include a UTC offset")
        return value.astimezone(UTC).isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    raise TypeError(f"Unsupported deterministic value: {type(value).__name__}")
