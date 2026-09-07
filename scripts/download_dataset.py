"""Download the Olist source dataset into the project's local raw-data directory."""

from __future__ import annotations

import argparse
from pathlib import Path

import kagglehub

DATASET_HANDLE = "olistbr/brazilian-ecommerce"


def project_root() -> Path:
    """Return the repository root without relying on the caller's working directory."""
    return Path(__file__).resolve().parents[1]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download the Olist dataset used by the Commerce Analytics Data Platform.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=project_root() / "data" / "raw" / "olist",
        help="Directory for raw CSV files (default: %(default)s)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    output_dir = args.output_dir.resolve()

    output_dir.mkdir(parents=True, exist_ok=True)

    path = kagglehub.dataset_download(
        DATASET_HANDLE,
        output_dir=str(output_dir),
    )

    print(f"Dataset downloaded to: {path}")


if __name__ == "__main__":
    main()
