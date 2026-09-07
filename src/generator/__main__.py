"""Phase 2 Generator 실행 입력을 검증하는 CLI다."""

from __future__ import annotations

import argparse
import json

from src.common.database import PostgresSettings
from src.generator.config import GENERATOR_VERSION, SUPPORTED_ANOMALY_PROFILES, GeneratorConfig
from src.generator.ids import logical_hash
from src.generator.metadata import ensure_generator_metadata


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Generator Foundation CLI 인자를 읽는다."""
    parser = argparse.ArgumentParser(
        description="Validate deterministic generator inputs without mutating the source."
    )
    parser.add_argument("--source-snapshot-id", required=True, help="Stable identifier of the seed snapshot.")
    parser.add_argument("--seed", type=int, required=True, help="Deterministic random seed.")
    parser.add_argument("--logical-date", required=True, help="UTC logical date in ISO-8601 format.")
    parser.add_argument("--orders", type=int, required=True, help="Number of orders to generate.")
    parser.add_argument(
        "--anomaly-profile",
        default="default",
        choices=sorted(SUPPORTED_ANOMALY_PROFILES),
        help="Deterministic service scenario profile.",
    )
    parser.add_argument(
        "--generator-version",
        default=GENERATOR_VERSION,
        help="Generator implementation contract version.",
    )
    parser.add_argument(
        "--initialize-metadata",
        action="store_true",
        help="Create the generator_runs metadata table without mutating commerce_source.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """입력을 검증하고 선택적으로 Generator Metadata Schema를 준비한다."""
    args = parse_args(argv)
    config = GeneratorConfig.from_values(
        source_snapshot_id=args.source_snapshot_id,
        random_seed=args.seed,
        logical_date=args.logical_date,
        order_count=args.orders,
        anomaly_profile=args.anomaly_profile,
        generator_version=args.generator_version,
    )
    if args.initialize_metadata:
        ensure_generator_metadata(PostgresSettings.from_environment())
    print(
        json.dumps(
            {
                "deterministic_input_hash": logical_hash(config.deterministic_inputs()),
                "generator_config": config.deterministic_inputs(),
                "metadata_initialized": args.initialize_metadata,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
