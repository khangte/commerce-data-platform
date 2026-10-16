"""Collection의 Metabase 카드와 Dashboard 배포 설정을 저장한다."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.serving.metabase_settings import host_metabase_url, require_metabase_setting

EXPORT_DIR = PROJECT_ROOT / "metabase/export"
SNAPSHOT_COLLECTION_IDS = (5, 6)
CARD_MODELS = frozenset({"card", "metric", "dataset"})
CARD_FIELDS = ("id", "name", "description", "type", "display", "collection_id", "database_id",
               "dataset_query", "parameters", "parameter_mappings", "visualization_settings")
DASHBOARD_FIELDS = ("id", "name", "description", "collection_id", "parameters", "width", "tabs", "dashcards")
DASHCARD_FIELDS = ("id", "card_id", "dashboard_tab_id", "row", "col", "size_x", "size_y",
                   "parameter_mappings", "visualization_settings", "inline_parameters")


def get_json(url: str, api_key: str) -> dict:
    """인증 키를 헤더로만 보내 JSON을 조회한다."""
    request = Request(url, headers={"X-API-Key": api_key}, method="GET")
    try:
        with urlopen(request, timeout=15) as response:
            return json.load(response)
    except HTTPError as error:
        raise RuntimeError(f"Metabase 요청 실패: HTTP {error.code}") from None
    except URLError:
        raise RuntimeError("Metabase 서버에 연결할 수 없습니다.") from None


def collection_targets(base: str, api_key: str, collection_ids: Sequence[int]) -> list[tuple[str, int]]:
    """지정 Collection에서 직접 속한 지원 모델만 중복 없이 모은다."""
    targets = set()
    for collection_id in collection_ids:
        response = get_json(
            f"{base}/api/collection/{collection_id}/items?models=card&models=metric&models=dataset&models=dashboard",
            api_key,
        )
        for item in response["data"]:
            model = item["model"]
            if model in CARD_MODELS:
                targets.add(("card", item["id"]))
            elif model == "dashboard":
                targets.add((model, item["id"]))
    return sorted(targets)


def trim_card(card: dict) -> dict:
    """카드의 배포 필드만 남긴다."""
    return {key: card[key] for key in CARD_FIELDS if key in card}


def trim_dashboard(dashboard: dict) -> dict:
    """Dashboard와 배치 카드의 배포 필드만 남긴다."""
    result = {key: dashboard[key] for key in DASHBOARD_FIELDS if key in dashboard}
    if "dashcards" in result:
        result["dashcards"] = [
            {key: card[key] for key in DASHCARD_FIELDS if key in card}
            for card in sorted(result["dashcards"], key=lambda card: card["id"])
        ]
    return result


def has_uncommitted_changes(path: Path) -> bool:
    """출력 경로의 추적 파일 변경과 미추적 파일을 확인한다."""
    try:
        result = subprocess.run(
            ["git", "-C", str(PROJECT_ROOT), "status", "--porcelain", "--", str(path)],
            capture_output=True, text=True, check=True,
        )
    except subprocess.CalledProcessError:
        raise RuntimeError("git 상태를 확인할 수 없습니다.") from None
    return bool(result.stdout.strip())


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """출력 경로와 강제 덮어쓰기 옵션을 해석한다."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=EXPORT_DIR)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """변경 보호와 전체 조회를 마친 뒤 Snapshot을 교체한다."""
    args = parse_args(argv)
    try:
        api_key = require_metabase_setting("METABASE_API_KEY", PROJECT_ROOT / ".env")
        base = host_metabase_url()
        if not args.force and has_uncommitted_changes(args.output_dir):
            print(f"{args.output_dir}에 커밋하지 않은 변경이 있습니다. 확인 후 --force로 다시 실행한다.", file=sys.stderr)
            return 1
        targets = collection_targets(base, api_key, SNAPSHOT_COLLECTION_IDS)
        snapshots = {}
        for model, target_id in targets:
            obj = get_json(f"{base}/api/{model}/{target_id}", api_key)
            trimmed = trim_card(obj) if model == "card" else trim_dashboard(obj)
            snapshots[f"{model}-{obj['id']}.json"] = trimmed
        args.output_dir.mkdir(parents=True, exist_ok=True)
        for name, obj in snapshots.items():
            (args.output_dir / name).write_text(
                json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8",
            )
        deleted = 0
        for pattern in ("card-*.json", "dashboard-*.json"):
            for path in args.output_dir.glob(pattern):
                if path.name not in snapshots:
                    path.unlink()
                    deleted += 1
        cards = sum(model == "card" for model, _ in targets)
        dashboards = sum(model == "dashboard" for model, _ in targets)
        print(f"Snapshot: 카드 {cards}개, Dashboard {dashboards}개, 삭제 {deleted}개")
        return 0
    except (RuntimeError, OSError, ValueError) as error:
        print(f"Snapshot 중단: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
