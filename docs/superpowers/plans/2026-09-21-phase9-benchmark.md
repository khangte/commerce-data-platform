# Phase 9 Benchmark Implementation Plan

> **For agentic workers:** Implement this plan one task at a time, in order. Steps use checkbox (`- [ ]`) syntax for tracking. After each batch, report the result to architect. Do not use subagent or worktree execution.

**Goal:** Prove measured differences — not absolute runtimes — between Baseline and improved runs on fixed data, environment and queries. Ship a reusable benchmark harness, experiments A–D, S/M/L scale runs, one bottleneck improvement, and the evidence documents.

**Architecture:**
- The harness is a runnable CLI, not test infrastructure. It lives in `src/benchmark/` and runs as `uv run python -m src.benchmark`.
- Canonical row hashing moves from `src/warehouse/mart_hash.py` into `src/common/row_hash.py` so mart hashes and benchmark result hashes share one definition.
- Raw results go to `data/benchmarks/{benchmark_id}/runs.jsonl` (gitignored). Curated evidence goes to `docs/benchmarks/` and **is committed** — lead approved this as an explicit exception to the docs commit rule on 2026-09-21.
- Every comparison is gated on `result_hash` equality. A mismatch invalidates the timing numbers for that comparison.
- No new runtime dependency. Resource sampling uses `time.monotonic`, `resource.getrusage` and `/proc/self/io`.
- No change to the Bronze Parquet contract. Experiment B builds a benchmark-only CSV mirror from committed Parquet.

**Tech Stack:** Python 3.12, DuckDB 1.5.5, dbt-duckdb 1.11 / dbt-core 1.12.3, psycopg 3, SeaweedFS S3 (boto3), pyarrow, pytest, uv, ruff (line length 100).

**Spec:** `docs/superpowers/specs/2026-09-21-phase9-benchmark-design.md` (PRD: `PRD_v1.14.md` §20, FR-19, FR-20, AC-17)

## Global Constraints

- Put a short docstring directly below every new class and function. Write it in Korean unless it must be English (code, identifier, SQL, command).
- Write non-ASCII strings as literal UTF-8. Never use `\uXXXX` escapes.
- Run `uv run ruff check .` before each commit and keep it green.
- Never print, cat, or commit `.env`. Read credentials only through `PostgresSettings.from_environment()` and `SeaweedFSSettings.from_environment()`.
- **Do not add any new package to `pyproject.toml` dependencies.** Resource sampling is stdlib only.
- **Do not change the Bronze Parquet write path** (`src/ingestion/bronze.py`) or any reliability contract (manifest verification, watermark CAS, lease, publish gate) for benchmark convenience. If a seam is missing, stop and ask architect.
- Benchmark code must never mutate committed metadata or watermarks outside the rewind path that `src/ingestion/reprocess.py` already provides.
- Record failures honestly. A scale that does not fit in 8GB RAM is recorded with its observed failure, never omitted or estimated.
- Never mix cold and warm runs into one median.
- Benchmark tests carry `pytestmark = [pytest.mark.benchmark, ...]` and skip unless their flags are set. Harness unit tests stay flag-free and fast.
- Commit messages: use `/caveman:caveman-commit`. End each message with `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>`.
- Commit code, SQL, tests, scripts and README. **Do not commit anything under `docs/`** except `docs/architect-review/` and `docs/benchmarks/`. `docs/benchmarks/` is a lead-approved exception (2026-09-21) because Phase 9 evidence is the deliverable. `docs/phases/` and `docs/superpowers/` stay uncommitted as before.
- Run shell commands from the project root `/home/kang/projects/commerce-data-platform`.

## File Structure

| Path | Kind | Responsibility |
|---|---|---|
| `src/common/row_hash.py` | Create | Canonical row JSON + streaming SHA-256 |
| `src/warehouse/mart_hash.py` | Modify | Import the shared hashing helpers instead of its private copies |
| `src/benchmark/__init__.py` | Create | Package marker |
| `src/benchmark/config.py` | Create | `ScaleProfile`, `BenchmarkScenario`, `RunConfig`, `scenario_config_hash` |
| `src/benchmark/metadata.py` | Create | Git/Python/lock/image/host environment metadata |
| `src/benchmark/measure.py` | Create | Wall time, CPU, max RSS, I/O bytes, row counts |
| `src/benchmark/result_hash.py` | Create | Logical hash of an arbitrary DuckDB query result |
| `src/benchmark/cache.py` | Create | Cold/warm procedures and `cache_reset_method` |
| `src/benchmark/store.py` | Create | Raw JSONL writer, median aggregation, comparison rendering |
| `src/benchmark/runner.py` | Create | Repeat runs, split cold/warm, enforce the hash gate |
| `src/benchmark/experiments/__init__.py` | Create | Scenario registry |
| `src/benchmark/experiments/extract.py` | Create | Experiment A |
| `src/benchmark/experiments/file_format.py` | Create | Experiment B |
| `src/benchmark/experiments/scan.py` | Create | Experiment C |
| `src/benchmark/experiments/cache_effect.py` | Create | Experiment D |
| `src/benchmark/__main__.py` | Create | CLI |
| `tests/benchmark/*.py` | Create | Harness unit tests + scale smoke |
| `pyproject.toml` | Modify | Register the `benchmark` marker |
| `.gitignore` | Modify | Ignore `data/benchmarks/` |
| `README.md` | Modify | How to run benchmarks and reset caches |
| `docs/benchmarks/*.md` | Create | Environment guide, experiment results, limits (committed) |
| `docs/phases/phase-09-benchmark.md`, `docs/phases/ROADMAP.md` | Modify | Task checkoffs (not committed) |

---

## Batch 1 — Harness foundation (P9-01 … P9-06)

### Task 1: Shared canonical row hashing

**Files:**
- Create: `src/common/row_hash.py`
- Modify: `src/warehouse/mart_hash.py`
- Test: `tests/test_mart_hash.py` (must still pass unchanged), `tests/benchmark/test_row_hash.py`

**Interfaces:**
- Produces: `canonical_row_json(row: dict[str, object]) -> str` — the exact behaviour currently in `mart_hash._canonical_row_json`, including its `_json_default` value coercion.
- Produces: `hash_cursor_rows(cursor, *, batch_size: int = 10_000) -> tuple[str, int]` returning `(sha256_hexdigest, row_count)` over an already-ordered DuckDB cursor.

**Steps:**
- [ ] Move `_canonical_row_json` and `_json_default` from `src/warehouse/mart_hash.py` into `src/common/row_hash.py` without changing their value coercion. Byte-for-byte identical output is required.
- [ ] Rewrite `mart_logical_hash` to call `hash_cursor_rows`. Keep `mart_logical_hash`, `mart_logical_hashes`, `mart_row_counts`, `target_for`, `mismatched_relations`, `describe_mart_difference` signatures unchanged.
- [ ] Add `tests/benchmark/__init__.py` and a unit test asserting a known row dict hashes to a stable digest and that Decimal/UUID/date/datetime coercion is unchanged.

**Verify:**
- [ ] `uv run ruff check .`
- [ ] `uv run pytest tests/test_mart_hash.py tests/benchmark -q`
- [ ] Existing mart hashes are unchanged: run `uv run pytest tests/test_dimension_materialization_contract.py tests/test_fact_incremental_contract.py -q`

---

### Task 2: Scenario and run configuration (P9-01)

**Files:**
- Create: `src/benchmark/__init__.py`, `src/benchmark/config.py`
- Test: `tests/benchmark/test_config.py`

**Interfaces:**
- Produces: `ScaleProfile` frozen dataclass — `name: Literal["S","M","L"]`, `order_count: int`, `random_seed: int`. Registry `SCALE_PROFILES` with S=100_000, M=1_000_000, L=5_000_000 orders and one fixed seed shared by all three.
- Produces: `BenchmarkScenario` frozen dataclass — `scenario: str`, `experiment: Literal["A","B","C","D"]`, `arms: tuple[str, ...]`, `repeats: int = 5`, `cold: bool`, `description: str`.
- Produces: `RunConfig` frozen dataclass — resolved `scenario`, `scale`, `benchmark_id`, `repeats`, `is_cold_run`, plus the free-form `parameters: Mapping[str, object]` used by experiment-specific fields.
- Produces: `new_benchmark_id(scenario: str, scale: str, now: datetime) -> str` formatted `{scenario}-{scale}-{YYYYMMDDTHHMMSSZ}`.
- Produces: `scenario_config_hash(scenario: BenchmarkScenario, config: RunConfig) -> str` — SHA-256 over the canonical JSON of the resolved configuration.

**Steps:**
- [ ] Validate `repeats >= 5` and reject a scale name outside `SCALE_PROFILES`.
- [ ] Keep the registry declarative in Python. Do not add a YAML/JSON config loader — no scenario is user-supplied.
- [ ] `scenario_config_hash` must ignore `benchmark_id` and timestamps so two runs of the same scenario definition hash the same.

**Verify:**
- [ ] `uv run pytest tests/benchmark/test_config.py -q`

---

### Task 3: Environment metadata (P9-06)

**Files:**
- Create: `src/benchmark/metadata.py`
- Test: `tests/benchmark/test_metadata.py`

**Interfaces:**
- Produces: `EnvironmentMetadata` frozen dataclass with `git_commit`, `python_version`, `dependency_lock_hash`, `docker_image_versions: dict[str, str]`, `host_wsl_spec: dict[str, str]`.
- Produces: `collect_environment() -> EnvironmentMetadata`.

**Steps:**
- [ ] `git_commit`: `git rev-parse HEAD`, plus a `-dirty` suffix when `git status --porcelain` is non-empty. A dirty tree is recorded, not rejected.
- [ ] `dependency_lock_hash`: SHA-256 of `uv.lock` bytes.
- [ ] `docker_image_versions`: parse `image:` values from `compose.yaml`. If `docker compose images` runs, prefer its resolved digests; on failure fall back to the file values and record which source was used.
- [ ] `host_wsl_spec`: CPU model and core count from `/proc/cpuinfo`, `MemTotal` from `/proc/meminfo`, kernel from `platform.release()`. Every field that cannot be read is `null`, never guessed.
- [ ] No subprocess call may raise out of `collect_environment`. Record the failure text in the field instead.

**Verify:**
- [ ] `uv run pytest tests/benchmark/test_metadata.py -q`
- [ ] `uv run python -c "from src.benchmark.metadata import collect_environment; print(collect_environment())"`

---

### Task 4: Resource measurement (P9-02)

**Files:**
- Create: `src/benchmark/measure.py`
- Test: `tests/benchmark/test_measure.py`

**Interfaces:**
- Produces: `Measurement` frozen dataclass — `duration_seconds: float`, `cpu_user_seconds`, `cpu_system_seconds`, `max_rss_bytes`, `read_bytes: int | None`, `write_bytes: int | None`.
- Produces: `measure(*, include_children: bool = False) -> ContextManager[MeasurementCollector]`; `MeasurementCollector.result()` returns the `Measurement` after exit.
- Produces: `RowCounts` frozen dataclass — `rows_scanned`, `rows_changed`, `input_bytes`, `output_bytes`; each field is optional and defaults to `None`.

**Steps:**
- [ ] Wall time from `time.monotonic()`. CPU and max RSS from `resource.getrusage(RUSAGE_SELF)`, adding `RUSAGE_CHILDREN` only when `include_children=True`.
- [ ] I/O deltas from `/proc/self/io`. When the file is unreadable, set both byte fields to `None` — do not substitute zero.
- [ ] `max_rss_bytes` converts Linux `ru_maxrss` kilobytes to bytes.
- [ ] Nesting `measure()` is not supported; raise if a collector is re-entered.

**Verify:**
- [ ] `uv run pytest tests/benchmark/test_measure.py -q` — asserts a busy loop reports non-zero duration and CPU, and that a missing `/proc/self/io` yields `None`.

---

### Task 5: Result hash and accuracy gate (P9-04)

**Files:**
- Create: `src/benchmark/result_hash.py`
- Test: `tests/benchmark/test_result_hash.py`

**Interfaces:**
- Produces: `query_result_hash(connection: duckdb.DuckDBPyConnection, sql: str, params: Sequence[object] = ()) -> tuple[str, int]` returning `(hash, row_count)` via `hash_cursor_rows`.
- Produces: `ResultHashMismatch(Exception)` carrying the two arm names and their hashes.
- Produces: `assert_arms_match(hashes: Mapping[str, str]) -> None` raising `ResultHashMismatch` when the arms disagree.

**Steps:**
- [ ] Require the caller's SQL to carry a deterministic `ORDER BY`. Raise `ValueError` when `order by` is absent from the statement — an unordered result hash is meaningless.
- [ ] Reuse `src/common/row_hash.py`. Do not re-implement canonical JSON.

**Verify:**
- [ ] `uv run pytest tests/benchmark/test_result_hash.py -q` — same rows in a different physical order hash equal; a changed value hashes differently; a missing `ORDER BY` raises.

---

### Task 6: Cold/warm procedure (P9-05)

**Files:**
- Create: `src/benchmark/cache.py`
- Modify: `README.md`
- Test: `tests/benchmark/test_cache.py`

**Interfaces:**
- Produces: `CacheResetResult` frozen dataclass — `method: Literal["drop_caches","process_restart_only"]`, `detail: str`.
- Produces: `reset_caches(*, services: Sequence[str] = ()) -> CacheResetResult`.
- Produces: `warm_up(callable) -> None` running one discarded warm-up pass.

**Steps:**
- [ ] Cold path: run `sync`, then write `3` to `/proc/sys/vm/drop_caches`. On `PermissionError` or `OSError`, fall back to `process_restart_only` and record the exact reason in `detail`. Never claim `drop_caches` when it did not happen.
- [ ] Fallback path: `docker compose restart {services}` when services are named, plus a note that the OS page cache was not dropped.
- [ ] Document both procedures and the WSL2 permission caveat in `README.md`.

**Verify:**
- [ ] `uv run pytest tests/benchmark/test_cache.py -q` — a patched unwritable `drop_caches` yields `process_restart_only` with a non-empty reason.

---

### Task 7: Raw store, median and comparison rendering (P9-03)

**Files:**
- Create: `src/benchmark/store.py`
- Modify: `.gitignore`, `pyproject.toml`
- Test: `tests/benchmark/test_store.py`

**Interfaces:**
- Produces: `BenchmarkRun` frozen dataclass carrying every PRD §20 field plus `cache_reset_method`, `change_rate`, `cursor_range`, `scenario_config_hash`, and `status: Literal["VALID","INVALID"]`.
- Produces: `append_run(run: BenchmarkRun) -> Path` writing one JSON object per line to `data/benchmarks/{benchmark_id}/runs.jsonl`.
- Produces: `load_runs(benchmark_id: str) -> tuple[BenchmarkRun, ...]`.
- Produces: `median_duration(runs, *, is_cold_run: bool) -> float | None` — filters to `status == "VALID"` and the requested cache state.
- Produces: `render_comparison(baseline, improved) -> str` emitting the Phase 9 result-document block: raw 5 values, median, result hash, percent change.

**Steps:**
- [ ] Median, not mean. For an even count use the mean of the two middle values and say so in the docstring.
- [ ] `median_duration` returns `None` rather than a number when fewer than 5 valid runs exist for that cache state.
- [ ] Percent change is recomputed from the raw values at render time. Never store a derived percentage.
- [ ] Add `data/benchmarks/` to `.gitignore` and register the `benchmark` marker in `pyproject.toml`.

**Verify:**
- [ ] `uv run pytest tests/benchmark/test_store.py -q`
- [ ] `uv run ruff check .`

---

### Task 8: Runner and CLI

**Files:**
- Create: `src/benchmark/runner.py`, `src/benchmark/__main__.py`, `src/benchmark/experiments/__init__.py`
- Test: `tests/benchmark/test_runner.py`

**Interfaces:**
- Produces: `ArmResult` — `arm: str`, `measurement: Measurement`, `counts: RowCounts`, `result_hash: str`.
- Produces: `run_experiment(scenario: BenchmarkScenario, config: RunConfig) -> tuple[BenchmarkRun, ...]` executing `repeats` iterations of every arm, resetting caches before each cold iteration, and appending each run.
- Produces: `EXPERIMENTS: dict[str, Callable[[RunConfig], Mapping[str, ArmResult]]]` in `experiments/__init__.py`.
- Produces: CLI `uv run python -m src.benchmark run --scenario <name> --scale {S,M,L} [--cold|--warm] [--repeats 5]` and `uv run python -m src.benchmark report --benchmark-id <id>`.

**Steps:**
- [ ] After every iteration call `assert_arms_match`. On `ResultHashMismatch`, mark all runs of that iteration `status="INVALID"`, keep the raw values, and continue to the next iteration; exit non-zero at the end.
- [ ] Attach `collect_environment()` and `scenario_config_hash` to every run.
- [ ] `report` prints the rendered comparison for both cold and warm populations separately.
- [ ] Keep `run_experiment` free of experiment-specific logic. Everything experiment-specific lives behind the `EXPERIMENTS` registry.

**Verify:**
- [ ] `uv run pytest tests/benchmark/test_runner.py -q` — a fake experiment with mismatching hashes produces `INVALID` runs and a non-zero exit; a matching one produces 5 valid runs per arm.
- [ ] `uv run python -m src.benchmark --help`

---

## Batch 2 — Experiments A–D (P9-07 … P9-18)

### Task 9: Experiment A — Full vs Incremental Extract (P9-07 … P9-09)

**Files:**
- Create: `src/benchmark/experiments/extract.py`
- Test: `tests/benchmark/test_experiment_extract.py`

**Interfaces:**
- Produces: `run_extract_experiment(config: RunConfig) -> Mapping[str, ArmResult]` with arms `full` and `incremental`.
- Produces: `bronze_logical_hash(catalog_path: Path, table: str) -> tuple[str, int]` hashing the committed Bronze rows of one table in PK order.

**Steps:**
- [ ] Build state T0, then apply a fixed change rate from `config.parameters["change_rate"]` with the existing generator, producing state T1.
- [ ] Arm `incremental`: ingest at T0 outside the measured window, then measure only the delta ingestion that reaches T1.
- [ ] Arm `full`: rewind watermarks with `src/ingestion/reprocess.py`, then measure a single ingestion pass that reaches T1 from empty cursors.
- [ ] Measure ingestion only. Generator time, rewind time and fixture setup stay outside `measure()`. State this exclusion in the experiment docstring.
- [ ] Record `rows_scanned`, `rows_changed`, `input_bytes`, `output_bytes`, `change_rate` and `cursor_range` on both arms.
- [ ] Result hash for both arms is `bronze_logical_hash` at T1, computed through `src/common/row_hash.py`.
- [ ] Do not add a new rewind or full-refresh path to `src/ingestion/`. If the existing CLI cannot express the full arm, **stop and report to architect**.

**Verify:**
- [ ] `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/benchmark/test_experiment_extract.py -q` at S scale with a small order count
- [ ] Both arms report the same `result_hash`

---

### Task 10: Experiment B — CSV vs Parquet (P9-10 … P9-12)

**Files:**
- Create: `src/benchmark/experiments/file_format.py`
- Test: `tests/benchmark/test_experiment_file_format.py`

**Interfaces:**
- Produces: `export_csv_mirror(catalog_path: Path, table: str, destination: Path) -> Path` writing a benchmark-only CSV from committed Bronze Parquet.
- Produces: `run_file_format_experiment(config: RunConfig) -> Mapping[str, ArmResult]` with arms `csv` and `parquet`.

**Steps:**
- [ ] Export with DuckDB `COPY (SELECT ... ORDER BY ...) TO '<path>' (FORMAT CSV, HEADER)`. Write only under `data/benchmarks/`. The CSV is never read by any pipeline code.
- [ ] Both arms run the identical projection and `ORDER BY` over the identical row range, so the result hashes match by construction.
- [ ] Record `input_bytes` as the on-disk size of the read files, and `output_bytes` as the result payload size.
- [ ] CSV export happens outside the measured window.
- [ ] Do not modify `src/ingestion/bronze.py`.

**Verify:**
- [ ] `RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/benchmark/test_experiment_file_format.py -q`
- [ ] Both arms report the same `result_hash`, and file sizes differ

---

### Task 11: Experiment C — Full Scan vs Filtered Scan (P9-13 … P9-15)

**Files:**
- Create: `src/benchmark/experiments/scan.py`
- Test: `tests/benchmark/test_experiment_scan.py`

**Interfaces:**
- Produces: `run_scan_experiment(config: RunConfig) -> Mapping[str, ArmResult]` with arms `full_scan` and `filtered_scan`.
- Produces: `profile_scan(connection, sql, params) -> tuple[int, int]` returning `(rows_scanned, bytes_scanned)` from DuckDB profiling output.

**Steps:**
- [ ] Both arms must produce the identical final aggregate. The filtered arm adds predicate pushdown and column projection only; it must not change grain, filter semantics or the output row set.
- [ ] Enable profiling with `PRAGMA enable_profiling='json'` and `PRAGMA profiling_output`, parse scanned rows and bytes, then disable profiling before the measured run so profiling overhead never lands in the duration. Run profiling in a separate pass.
- [ ] If DuckDB 1.5.5 does not expose bytes scanned, record `rows_scanned` and leave `input_bytes` from file sizes, and note the limitation in the result document. Do not fabricate the value.

**Verify:**
- [ ] `uv run pytest tests/benchmark/test_experiment_scan.py -q`
- [ ] Both arms report the same `result_hash`, and the filtered arm reports fewer scanned rows

---

### Task 12: Experiment D — Cold vs Warm (P9-16 … P9-18)

**Files:**
- Create: `src/benchmark/experiments/cache_effect.py`
- Test: `tests/benchmark/test_experiment_cache_effect.py`

**Interfaces:**
- Produces: `run_cache_experiment(config: RunConfig) -> Mapping[str, ArmResult]` reusing the Experiment C filtered-scan scenario as its single workload.

**Steps:**
- [ ] Run 5 cold iterations and 5 warm iterations of the same workload as separate `benchmark_id` populations.
- [ ] Cold iterations call `reset_caches()` before each run and record the returned `cache_reset_method` on every run row.
- [ ] Warm iterations run one discarded warm-up, then 5 measured runs with no cache reset.
- [ ] `store.median_duration` must be called once per cache state. Never aggregate the two together.
- [ ] The cache result is interpreted on its own. Do not fold it into any A/B/C improvement claim.

**Verify:**
- [ ] `uv run pytest tests/benchmark/test_experiment_cache_effect.py -q`
- [ ] `uv run python -m src.benchmark report --benchmark-id <cold id>` and the warm id render as two separate populations

---

## Batch 3 — Scale runs (P9-19 … P9-21)

### Task 13: S scale harness validation (P9-19)

**Files:**
- Create: `docs/benchmarks/00-environment.md` (committed)

**Steps:**
- [ ] Generate the S dataset (100K orders) with the fixed seed and record its dataset identity: `random_seed`, `order_count`, seed hash, source row counts.
- [ ] Run experiments A, B, C, D at S scale, 5 repeats each.
- [ ] Confirm every comparison reports matching `result_hash` and `status="VALID"`.
- [ ] Measure the harness overhead: run one no-op scenario and record its duration as the measurement floor.
- [ ] Write `docs/benchmarks/00-environment.md`: reproduction commands, environment metadata, cache-reset procedure, dataset identity, harness overhead.

**Verify:**
- [ ] `data/benchmarks/` contains 5 raw runs per arm per experiment
- [ ] No `INVALID` run remains

---

### Task 14: M scale baseline (P9-20)

**Files:**
- Create: `docs/benchmarks/01-experiment-a-extract.md`, `02-experiment-b-file-format.md`, `03-experiment-c-scan.md`, `04-experiment-d-cache.md` (committed)

**Steps:**
- [ ] Generate the M dataset (1M orders) with the same seed and record its identity.
- [ ] Run experiments A, B, C, D at M scale, 5 repeats each.
- [ ] Write one document per experiment in the Phase 9 result format: hypothesis, measured scope and exclusions, environment and dataset, exact commands, raw 5 values, median, result hash, observed bottleneck, limitations.
- [ ] Report the M-scale wall-clock cost of dataset generation to architect before starting Task 15.

**Verify:**
- [ ] Every document carries raw 5 values and a median recomputable from them
- [ ] Every comparison carries identical result hashes

---

### Task 15: L scale run or resource-limit evidence (P9-21)

**Files:**
- Create: `docs/benchmarks/05-scale-limits.md` (committed)

**Steps:**
- [ ] Attempt the L dataset (5M orders) and the same experiment set.
- [ ] On success, record the results exactly as in Task 14.
- [ ] On failure, record the failure condition verbatim: the failing stage, the exact error, peak RSS, available memory, disk usage and elapsed time at failure. Do not estimate what the result would have been.
- [ ] Lead approved on 2026-09-21: if L fails on 8GB RAM, close Phase 9 on the M-scale results plus the recorded resource-limit evidence. Do not retry L with a weakened contract or a reduced dataset presented as L.

**Verify:**
- [ ] `docs/benchmarks/05-scale-limits.md` states clearly whether L succeeded or hit a resource limit, with observations

---

## Batch 4 — Improvement loop and closeout (P9-22 … P9-24)

### Task 16: Bottleneck selection and one improvement (P9-22, P9-23)

**Files:**
- Create: `docs/benchmarks/06-improvement.md` (committed)
- Modify: whichever single source file the chosen improvement touches

**Steps:**
- [ ] Pick exactly one bottleneck from the M-scale evidence. Justify it with duration contribution and scanned rows/bytes, not intuition.
- [ ] **Report the chosen bottleneck and the intended change to architect before editing any source file.** Wait for approval.
- [ ] Change one variable only. Do not bundle unrelated edits.
- [ ] Re-measure with the identical dataset, environment and scenario, 5 repeats.
- [ ] Confirm the result hash is unchanged. If it changed, the improvement is rejected — record it and stop.
- [ ] Write `docs/benchmarks/06-improvement.md` with baseline raw/median, improved raw/median, percent change recomputed from raw values, result hash equality, and the trade-off the change introduces.
- [ ] Run the full existing suites to prove no contract regressed.

**Verify:**
- [ ] `uv run ruff check .`
- [ ] `uv run pytest -q`
- [ ] `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 RUN_DBT_PUBLISH_INTEGRATION=1 uv run pytest tests/reliability -q`
- [ ] Baseline and improved result hashes are identical

---

### Task 17: Documentation and phase closeout (P9-24)

**Files:**
- Modify: `README.md`, `docs/phases/phase-09-benchmark.md`, `docs/phases/ROADMAP.md` (docs not committed)

**Steps:**
- [ ] `README.md`: add a "Benchmark 실행하기" section with the scale generation commands, `python -m src.benchmark run/report` usage, the cold-run permission caveat, and where raw results land.
- [ ] Check off `P9-01` … `P9-24` in `docs/phases/phase-09-benchmark.md`, each with its measured evidence (command + file path), in the same style Phase 8 used.
- [ ] Fill the Phase 9 Definition of Done with evidence, including the AC-17 line.
- [ ] Mark Phase 9 complete in `docs/phases/ROADMAP.md`.
- [ ] Report completion to architect with the list of evidence documents.

**Verify:**
- [ ] `docs/phases/phase-09-benchmark.md` has zero remaining `- [ ]`
- [ ] Every DoD line cites a command or a file, not a claim

---

## Lead decisions (2026-09-21)

- L scale may close as resource-limit evidence; M scale carries the Definition of Done.
- `docs/benchmarks/` is committed as an approved exception to the docs commit rule.

## Open questions for architect

1. Experiment A's full arm depends on `src/ingestion/reprocess.py` expressing a rewind to empty cursors for all nine tables in one pass. Confirm at Task 9 before writing any new code path.
2. If DuckDB 1.5.5 profiling does not report bytes scanned, Task 11 records rows only. Confirm that is acceptable evidence for `P9-15`.
3. Task 16's improvement target needs architect approval before any `src/` edit.
