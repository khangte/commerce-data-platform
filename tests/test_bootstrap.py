"""Tests for the repository's Phase 0 bootstrap contract."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_download_module():
    spec = importlib.util.spec_from_file_location(
        "download_dataset", PROJECT_ROOT / "scripts" / "download_dataset.py"
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_phase_zero_repository_layout_exists() -> None:
    expected_paths = (
        "airflow/dags",
        "src/common",
        "src/generator",
        "src/ingestion",
        "src/seed",
        "dbt/models",
        "dbt/tests",
        "dbt/macros",
        "sql/source",
        "sql/metadata",
        "sql/validation",
        "data/raw/olist",
        "data/generated",
        "data/warehouse",
        "data/samples",
        "tests/seed",
        "tests/generator",
        "tests/ingestion",
        "tests/integration",
    )

    missing = [path for path in expected_paths if not (PROJECT_ROOT / path).is_dir()]
    assert not missing, f"Missing Phase 0 directories: {', '.join(missing)}"


def test_environment_template_has_required_keys_without_real_secrets() -> None:
    values = {}
    for line in (PROJECT_ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#"):
            key, value = line.split("=", maxsplit=1)
            values[key] = value

    assert {
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_PORT",
        "COMMERCE_SOURCE_DB",
        "AIRFLOW_METADATA_DB",
        "PIPELINE_METADATA_DB",
        "SEAWEEDFS_ACCESS_KEY",
        "SEAWEEDFS_SECRET_KEY",
        "SEAWEEDFS_BUCKET",
        "INGESTION_PAGE_SIZE",
    }.issubset(values)
    assert values["POSTGRES_PASSWORD"] == "change_me"
    assert values["SEAWEEDFS_SECRET_KEY"] == "change_me"


def test_download_dataset_cli_has_stable_default_output_directory() -> None:
    module = load_download_module()

    args = module.parse_args([])

    assert module.DATASET_HANDLE == "olistbr/brazilian-ecommerce"
    assert args.output_dir == PROJECT_ROOT / "data" / "raw" / "olist"


def test_download_dataset_cli_help_is_available() -> None:
    result = subprocess.run(
        [sys.executable, "scripts/download_dataset.py", "--help"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )

    assert "--output-dir" in result.stdout


def test_init_script_is_present_and_syntax_valid() -> None:
    init_script = PROJECT_ROOT / "scripts" / "init.sh"

    assert init_script.is_file()
    subprocess.run(["bash", "-n", str(init_script)], check=True)
