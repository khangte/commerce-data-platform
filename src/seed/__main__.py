"""Command-line entry point for loading the Olist baseline snapshot."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.common.database import PROJECT_ROOT, PostgresSettings
from src.seed.loader import parse_seeded_at, run_seed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load a raw-compatible Olist seed snapshot.")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "raw" / "olist",
        help="Directory containing the six required Olist CSV files.",
    )
    parser.add_argument(
        "--seeded-at",
        required=True,
        help="UTC baseline snapshot timestamp, for example 2026-09-03T00:00:00Z.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    result = run_seed(
        input_dir=args.input_dir.resolve(),
        seeded_at=parse_seeded_at(args.seeded_at),
        settings=PostgresSettings.from_environment(),
    )
    print(
        json.dumps(
            {
                "seed_run_id": str(result.seed_run_id),
                "raw_checksum": result.raw_checksum,
                "seeded_at": result.seeded_at.isoformat(),
                "table_row_counts": result.table_row_counts,
                "table_content_hashes": result.table_content_hashes,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
