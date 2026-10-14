"""수집 종료 뒤 Run의 Local Parquet 정리를 확인한다."""

import uuid
from pathlib import Path

from src.ingestion.service import _remove_local_artifacts
from src.ingestion.tables import table_config


def test_remove_local_artifacts_deletes_only_the_run_files(tmp_path: Path) -> None:
    """같은 Run의 Bronze·Quarantine 파일만 지우고 다른 Run 파일과 빈 경로는 건드리지 않는다."""
    config = table_config("orders")
    run_id, other_id = uuid.uuid4(), uuid.uuid4()
    table_dir = tmp_path / config.source_table
    table_dir.mkdir()
    mine = [table_dir / f"{run_id}.parquet", table_dir / f"{run_id}.quarantine.parquet"]
    other = table_dir / f"{other_id}.parquet"
    for path in (*mine, other):
        path.write_bytes(b"x")
    _remove_local_artifacts(tmp_path, config, run_id)
    assert not any(path.exists() for path in mine)
    assert other.exists()
    _remove_local_artifacts(tmp_path, config, run_id)
