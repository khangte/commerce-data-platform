"""Generator CLI 인자 계약을 검증한다."""

from __future__ import annotations

from src.generator.__main__ import parse_args


def test_cli_progress_time_scale_defaults_to_one() -> None:
    """배율 인자를 생략하면 1이다."""
    args = parse_args(["--seed", "0", "--logical-date", "2026-09-04T00:00:00Z", "--orders", "1"])
    assert args.progress_time_scale == 1


def test_cli_accepts_progress_time_scale() -> None:
    """DAG 실행을 CLI로 재현할 수 있게 배율을 받는다."""
    args = parse_args(
        ["--seed", "0", "--logical-date", "2026-09-04T00:00:00Z", "--orders", "1", "--progress-time-scale", "144"]
    )
    assert args.progress_time_scale == 144
