"""Phase 2 Generator 실행과 입력 검증을 제공하는 CLI다."""

from __future__ import annotations

import argparse
import json

from src.common.database import PostgresSettings
from src.generator.config import EXECUTABLE_ANOMALY_PROFILES, GENERATOR_VERSION, GeneratorConfig
from src.generator.ids import logical_hash
from src.generator.metadata import ensure_generator_metadata
from src.generator.service import resolve_source_snapshot_id, run_generator


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Generator Foundation CLI 인자를 읽는다."""
    parser = argparse.ArgumentParser(
        description="Run the deterministic generator or validate its inputs without source mutation."
    )
    parser.add_argument(
        "--source-snapshot-id",
        help="Stable identifier of the seed snapshot. Defaults to the latest successful seed.",
    )
    parser.add_argument("--seed", type=int, required=True, help="Deterministic random seed.")
    parser.add_argument("--logical-date", required=True, help="UTC logical date in ISO-8601 format.")
    parser.add_argument("--orders", type=int, required=True, help="Number of orders to generate.")
    parser.add_argument(
        "--anomaly-profile",
        default="default",
        choices=sorted(EXECUTABLE_ANOMALY_PROFILES),
        help="Deterministic profile executable by this CLI.",
    )
    parser.add_argument(
        "--generator-version",
        default=GENERATOR_VERSION,
        help="Generator implementation contract version.",
    )
    parser.add_argument(
        "--initialize-metadata",
        action="store_true",
        help="Create the generator_runs metadata table before execution or validation.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate inputs and optionally initialize metadata without mutating commerce_source.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """입력을 검증하거나 결정적 Generator 실행 결과를 출력한다."""
    args = parse_args(argv)
    settings = PostgresSettings.from_environment()
    source_snapshot_id = args.source_snapshot_id or resolve_source_snapshot_id(settings)
    config = GeneratorConfig.from_values(
        source_snapshot_id=source_snapshot_id,
        random_seed=args.seed,
        logical_date=args.logical_date,
        order_count=args.orders,
        anomaly_profile=args.anomaly_profile,
        generator_version=args.generator_version,
    )
    if args.initialize_metadata:
        ensure_generator_metadata(settings)
    if args.validate_only:
        print(
            json.dumps(
                {
                    "deterministic_input_hash": logical_hash(config.deterministic_inputs()),
                    "generator_config": config.deterministic_inputs(),
                    "metadata_initialized": args.initialize_metadata,
                    "validated_only": True,
                },
                sort_keys=True,
            )
        )
        return
    result = run_generator(config, settings)
    print(
        json.dumps(
            {
                "generator_run_id": str(result.generator_run_id),
                "result_counts": result.result_counts,
                "logical_content_hash": result.logical_content_hash,
                "reused_successful_run": result.reused_successful_run,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
