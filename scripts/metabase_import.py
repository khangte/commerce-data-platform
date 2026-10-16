"""저장된 Metabase 카드 쿼리를 비교하고 명시적 요청 때만 기존 카드에 반영한다."""

from __future__ import annotations

import argparse
import difflib
import json
import os
import sys
from collections.abc import Sequence
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXPORT_DIR = PROJECT_ROOT / "metabase/export"
DEFAULT_METABASE_URL = "http://localhost:3000"


def load_api_key() -> str:
    """환경 변수 또는 프로젝트 루트 .env에서 API Key를 읽는다."""
    key = os.environ.get("METABASE_API_KEY", "").strip()
    if key:
        return key
    env_path = PROJECT_ROOT / ".env"
    if env_path.is_file():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("METABASE_API_KEY="):
                key = line.partition("=")[2].strip()
                if key:
                    return key
                break
    raise RuntimeError("METABASE_API_KEY가 필요합니다.")


def request_card(url: str, api_key: str, payload: dict | None = None) -> dict:
    """카드를 조회하거나 쿼리만 갱신하고 인증 정보를 오류 출력에서 숨긴다."""
    request = Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload else None,
        headers={"X-API-Key": api_key, "Content-Type": "application/json"},
        method="PUT" if payload is not None else "GET",
    )
    try:
        with urlopen(request, timeout=15) as response:
            return json.load(response)
    except HTTPError as error:
        raise RuntimeError(f"Metabase 카드 요청 실패: HTTP {error.code}") from None
    except URLError:
        raise RuntimeError("Metabase 서버에 연결할 수 없습니다.") from None


def validate_card(card: dict, card_id: int) -> int:
    """요청 카드 식별자와 쿼리·메타데이터의 데이터베이스 식별자를 검증한다."""
    if card.get("id") != card_id:
        raise ValueError(f"카드 {card_id}: 카드 id 불일치")
    database_id = card.get("database_id")
    query_database = card["dataset_query"].get("database")
    if type(database_id) is not int or database_id <= 0 or database_id != query_database:
        raise ValueError(f"카드 {card_id}: database id 불일치 또는 누락")
    return database_id


def query_diff(card_id: int, live_query: dict, export_query: dict) -> str:
    """실제로 반영할 dataset_query의 차이를 통합 diff로 만든다."""
    live_lines = json.dumps(live_query, ensure_ascii=False, sort_keys=True, indent=2).splitlines()
    export_lines = json.dumps(export_query, ensure_ascii=False, sort_keys=True, indent=2).splitlines()
    return "\n".join(difflib.unified_diff(
        live_lines, export_lines,
        fromfile=f"live/card-{card_id}/dataset_query",
        tofile=f"export/card-{card_id}/dataset_query",
        lineterm="",
    ))


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """카드 식별자 목록과 기본 비활성인 쓰기 옵션을 해석한다."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("card_ids", type=int, nargs="+", help="기존 카드의 양의 정수 id")
    parser.add_argument("--apply", action="store_true", help="차이를 확인한 뒤 기존 카드 쿼리 반영")
    args = parser.parse_args(argv)
    if any(card_id <= 0 for card_id in args.card_ids):
        parser.error("카드 id는 양의 정수여야 합니다.")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    """모든 카드의 식별자와 차이를 확인한 뒤 요청한 경우에만 쿼리를 반영한다."""
    args = parse_args(argv)
    try:
        api_key = load_api_key()
        base = os.environ.get("METABASE_URL", DEFAULT_METABASE_URL).rstrip("/")
        changes = []
        for card_id in dict.fromkeys(args.card_ids):
            exported = json.loads((EXPORT_DIR / f"card-{card_id}.json").read_text(encoding="utf-8"))
            export_database = validate_card(exported, card_id)
            url = f"{base}/api/card/{card_id}"
            live = request_card(url, api_key)
            if validate_card(live, card_id) != export_database:
                raise ValueError(f"카드 {card_id}: 운영·Export database id 불일치")
            diff = query_diff(card_id, live["dataset_query"], exported["dataset_query"])
            if diff:
                print(diff)
                changes.append((card_id, url, {"dataset_query": exported["dataset_query"]}))
            else:
                print(f"카드 {card_id}: dataset_query 변경 없음")
        if args.apply:
            for card_id, url, payload in changes:
                request_card(url, api_key, payload)
                print(f"카드 {card_id}: dataset_query 반영 완료")
        else:
            print(f"모의 실행: 변경 대상 {len(changes)}개, PUT 요청 없음")
        return 0
    except (RuntimeError, OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        print(f"카드 가져오기 중단: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
