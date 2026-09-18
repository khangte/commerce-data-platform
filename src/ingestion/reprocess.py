"""명시 시각으로 Watermark를 되감아 Re-extract 입력 경계를 연다."""

from __future__ import annotations

import argparse
import json
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from src.common.database import PostgresSettings
from src.ingestion.extract import cursor_before_timestamp
from src.ingestion.lease import acquire_table_lease, release_table_lease
from src.ingestion.metadata import CursorPosition, get_or_create_watermark, rewind_watermark
from src.ingestion.tables import TABLE_CONFIGS, table_config


@dataclass(frozen=True)
class RewindOutcome:
    """한 Source Table의 되감기 전후 Cursor와 Version이다."""

    source_table: str
    pipeline_name: str
    cursor_before: CursorPosition
    cursor_after: CursorPosition
    version_before: int
    version_after: int

    def as_json(self) -> dict[str, object]:
        """CLI 표준 출력에 실을 JSON 표현을 돌려준다."""
        return {
            "source_table": self.source_table,
            "pipeline_name": self.pipeline_name,
            "cursor_before": self.cursor_before.as_metadata_json(),
            "cursor_after": self.cursor_after.as_metadata_json(),
            "version_before": self.version_before,
            "version_after": self.version_after,
        }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """되감기 대상 Table과 UTC 경계 시각을 CLI 인자로 읽는다."""
    parser = argparse.ArgumentParser(
        description="Rewind ingestion watermarks so a bounded re-extract can run."
    )
    parser.add_argument(
        "--tables",
        required=True,
        nargs="+",
        choices=sorted(TABLE_CONFIGS),
        help="Source tables to rewind.",
    )
    parser.add_argument(
        "--reprocess-from",
        required=True,
        help="UTC timestamp in ISO-8601 format. Rows at or after it are re-extracted.",
    )
    parser.add_argument(
        "--pipeline-name",
        default=None,
        help="Pipeline name override. Defaults to <table>_bronze.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve the rewind target without writing metadata.",
    )
    return parser.parse_args(argv)


def rewind_tables(
    settings: PostgresSettings,
    *,
    source_tables: Sequence[str],
    boundary: datetime,
    pipeline_name: str | None = None,
    dry_run: bool = False,
) -> tuple[RewindOutcome, ...]:
    """Table마다 Lease를 잡고 경계 직전 Cursor로 Watermark를 되감는다."""
    outcomes: list[RewindOutcome] = []
    for source_table in source_tables:
        config = table_config(source_table)
        resolved_pipeline = pipeline_name or f"{source_table}_bronze"
        target = cursor_before_timestamp(settings, config, boundary)
        current = get_or_create_watermark(settings, resolved_pipeline, source_table)
        if dry_run:
            outcomes.append(
                RewindOutcome(
                    source_table=source_table,
                    pipeline_name=resolved_pipeline,
                    cursor_before=current.cursor,
                    cursor_after=target,
                    version_before=current.version,
                    version_after=current.version,
                )
            )
            continue
        lease = acquire_table_lease(
            settings,
            pipeline_name=resolved_pipeline,
            source_table=source_table,
            owner_id=uuid.uuid4(),
        )
        try:
            rewound = rewind_watermark(
                settings,
                pipeline_name=resolved_pipeline,
                source_table=source_table,
                owner_id=lease.owner_id,
                expected_version=lease.watermark_version,
                cursor=target,
            )
        finally:
            release_table_lease(settings, lease)
        outcomes.append(
            RewindOutcome(
                source_table=source_table,
                pipeline_name=resolved_pipeline,
                cursor_before=current.cursor,
                cursor_after=rewound.cursor,
                version_before=current.version,
                version_after=rewound.version,
            )
        )
    return tuple(outcomes)


def main(argv: list[str] | None = None) -> int:
    """되감기를 실행하고 Table별 결과를 JSON으로 보고한다."""
    args = parse_args(argv)
    boundary = datetime.fromisoformat(args.reprocess_from)
    settings = PostgresSettings.from_environment()
    outcomes = rewind_tables(
        settings,
        source_tables=args.tables,
        boundary=boundary,
        pipeline_name=args.pipeline_name,
        dry_run=args.dry_run,
    )
    print(json.dumps([outcome.as_json() for outcome in outcomes], ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
