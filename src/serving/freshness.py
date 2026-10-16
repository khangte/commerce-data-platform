"""Metabase의 실제 서빙 실행과 최신 게시 실행을 비교한다."""

from dataclasses import dataclass

from src.common.database import PostgresSettings
from src.serving.metabase_repoint import read_live_serving_manifest
from src.warehouse.publish_metadata import get_latest_publish_run


@dataclass(frozen=True)
class ServingFreshness:
    """실제 연결의 내보내기·게시 실행과 최신 게시 실행을 담는다."""

    database_id: int
    live_export_id: str
    live_publish_run_id: str
    latest_publish_run_id: str | None

    @property
    def fresh(self) -> bool:
        """최신 게시 실행이 있고 실제 연결 실행과 같을 때 참이다."""
        return (
            self.latest_publish_run_id is not None
            and self.live_publish_run_id == self.latest_publish_run_id
        )


def check_serving_freshness(
    settings: PostgresSettings, metabase_url: str, api_key: str, database_id: int
) -> ServingFreshness:
    """Metabase를 거쳐 읽은 매니페스트를 최신 PUBLISHED 기록과 대조한다."""
    latest = get_latest_publish_run(settings)
    export_id, publish_run_id = read_live_serving_manifest(metabase_url, api_key, database_id)
    return ServingFreshness(
        database_id=database_id,
        live_export_id=export_id,
        live_publish_run_id=publish_run_id,
        latest_publish_run_id=None if latest is None else str(latest.publish_run_id),
    )
