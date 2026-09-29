# Phase 9. Benchmark

> 상태: Done  
> Milestone: 3 — Portfolio Evidence  
> 선행 Phase: [Phase 8. Reliability Scenarios](phase-08-reliability.md)  
> 기준 문서: [ROADMAP](ROADMAP.md), [PRD v1.18](../../PRD_v1.18.md)

## 목표

절대 처리시간을 성공 조건으로 삼지 않고, 고정된 데이터·환경·Query에서 Baseline과 개선 전후의 차이를 정량적으로 증명한다. Raw 측정값, Median, Result Hash, 환경 Metadata를 함께 보존한다.

## 원칙

- 100K Orders에서 측정 Framework를 먼저 검증한 뒤 1M과 5M으로 확장한다.
- 각 주요 실험은 같은 조건으로 5회 실행하고 Raw 값을 모두 보존한다.
- 평균보다 Median을 대표값으로 사용한다.
- 성능 개선 전후 결과의 Logical Hash가 같아야 한다.
- Cold/Warm Run을 구분하며 섞어서 비교하지 않는다.
- 한 번에 하나의 주요 변수를 변경한다.
- 8GB RAM에서 5M이 불가능하면 실패 조건과 관측치를 숨기지 않고 기록한다.

## 선행 조건

이 Phase는 Grain 계약을 필요로 하는 실험과 그렇지 않은 실험으로 나뉜다. 골격 Task는 Phase 6 완료 전에 착수할 수 있다.

### 골격 Task 선행 조건 (9-1)

- Phase 8의 골격 Scenario가 안정적으로 통과한다.
- Dataset을 결정적으로 생성하고 Scale을 식별할 수 있다.
- Full/Incremental 결과의 Logical Hash를 계산할 수 있다.
- Host/WSL/Docker Resource 정보를 기록할 수 있다.

### 적용 Task 선행 조건 (9-2)

- Phase 6, 7, 8의 모든 Task가 완료된다.
- Mart까지 포함한 E2E Pipeline이 안정적으로 통과한다.

## Scale

| Scale | Orders | 목적                                  |
| ----- | -----: | ------------------------------------- |
| S     |   100K | Harness와 측정 오버헤드 검증          |
| M     |     1M | 기본 Portfolio Baseline               |
| L     |     5M | Page Write/Memory/파일 크기 한계 관측 |

## 9-1. 골격: Grain 계약 없이 진행 가능

Harness와 Ingestion·Storage 계층 실험은 Mart 구성과 독립적이다. 측정 대상이 Extract, 파일 형식, Scan 전략이라서 어떤 Dimension과 Fact가 있는지 몰라도 수행할 수 있다.

### 9-1-1. Benchmark Harness

- [x] `P9-01` Benchmark Scenario/Run ID와 Config Schema 정의 — `src/benchmark/config.py`, `src/benchmark/runner.py`; `uv run python -m src.benchmark run --scenario harness_overhead --scale S` → `harness_overhead-S-20260921T104453Z` (`docs/benchmarks/00-environment.md` §6).
- [x] `P9-02` Wall Time, CPU/Memory, I/O, Row Count 수집 — `src/benchmark/measure.py`, `src/benchmark/store.py`; `docs/benchmarks/02-experiment-b-file-format.md`의 M/L Duration·입출력 Byte와 `docs/benchmarks/03-experiment-c-scan.md`의 Scan Row/Byte 실측.
- [x] `P9-03` Raw Result 저장 형식과 Median 계산 구현 — `src/benchmark/store.py`, `src/benchmark/runner.py`; `data/benchmarks/{benchmark_id}/runs.jsonl` 및 `uv run python -m src.benchmark report --benchmark-id <benchmark_id>`, Raw 5회·Median은 `docs/benchmarks/00-environment.md` §5에 예시 기록.
- [x] `P9-04` Result Hash와 정확성 Gate 연결 — `src/benchmark/result_hash.py`, `src/benchmark/runner.py`; `uv run python -m src.benchmark run --scenario scan --scale M --fixture-old-rows 5000000 --fixture-current-rows 1000000` → 두 Arm Hash 동일(`docs/benchmarks/03-experiment-c-scan.md` §M Scale).
- [x] `P9-05` Cold/Warm Run 구분과 Cache Reset 절차 문서화 — `src/benchmark/cache.py`; `uv run python -m src.benchmark run --scenario cache_effect --scale M --cold --fixture-old-rows 5000000 --fixture-current-rows 1000000` 및 같은 명령의 `--warm`, 절차·한계는 `docs/benchmarks/00-environment.md` §3 및 `docs/benchmarks/04-experiment-d-cache-effect.md`.
- [x] `P9-06` Dependency Lock/Image/Dataset 식별 정보 기록 — `src/benchmark/metadata.py`; `uv run python -m src.benchmark report --benchmark-id harness_overhead-S-20260921T104453Z`, 환경·Lock·Image·Seed는 `docs/benchmarks/00-environment.md` §2·§4.

Run Metadata 최소 필드:

```text
benchmark_id
scenario
dataset_scale
run_number
is_cold_run
duration_seconds
rows_scanned
rows_changed
input_bytes
output_bytes
result_hash
git_commit
python_version
dependency_lock_hash
docker_image_versions
host_wsl_spec
random_seed
query_or_command
```

### 9-1-2. Experiment A — Full vs Incremental Extract

- [x] `P9-07` 동일 최종 결과를 만드는 Full Extract Baseline — `uv run python -m src.benchmark run --scenario extract --scale S` → Full median 282.089758063초, Anchor/Hash는 `docs/benchmarks/01-experiment-a-extract.md`.
- [x] `P9-08` 변경률이 고정된 Incremental Extract 측정 — 같은 명령 → Incremental median 5.847424009초, `change_rate=0.013475091293743515`, 같은 Anchor/Hash(`docs/benchmarks/01-experiment-a-extract.md`).
- [x] `P9-09` Rows Scanned/Changed, Bytes, Duration 비교 — `uv run python -m src.benchmark report --benchmark-id extract-S-20260921T113012Z`; Table별 t0/delta/t1과 Full/Incremental 48.2배 비교를 `docs/benchmarks/00-environment.md` §4·`01-experiment-a-extract.md`에 기록.

변경률과 Cursor 범위를 결과에 기록한다. 결과 Hash가 다르면 성능 수치를 채택하지 않는다.

### 9-1-3. Experiment B — CSV vs Parquet

- [x] `P9-10` 같은 Column/Row 범위의 CSV Read 측정 — `uv run python -m src.benchmark run --scenario file_format --scale M` → csv Raw 5회·median 19.910847433초(`docs/benchmarks/02-experiment-b-file-format.md` §M Scale).
- [x] `P9-11` 같은 결과를 만드는 Parquet Read 측정 — 같은 명령 → parquet Raw 5회·median 19.992092046초 및 csv와 동일 Hash(`docs/benchmarks/02-experiment-b-file-format.md` §M Scale).
- [x] `P9-12` 파일 크기, Scan Bytes, Duration 비교 — `uv run python -m src.benchmark report --benchmark-id file_format-M-20260921T134121Z`; M/L CSV·Parquet On-disk Byte와 Duration을 `docs/benchmarks/02-experiment-b-file-format.md`에 기록.

### 9-1-4. Experiment C — Full Scan vs Filtered Scan

- [x] `P9-13` 전체 Dataset Scan Baseline — `uv run python -m src.benchmark run --scenario scan --scale M --fixture-old-rows 5000000 --fixture-current-rows 1000000` → `full_scan` 6,000,000 Row, median 0.033107826초(`docs/benchmarks/03-experiment-c-scan.md` §M Scale).
- [x] `P9-14` 동일 분석 결과 범위의 Predicate/Column Projection 적용 — 같은 명령 → Pushdown `filtered_scan` 1,000,000 Row, 동일 Result Hash(`docs/benchmarks/03-experiment-c-scan.md` §M Scale).
- [x] `P9-15` Scan Rows/Bytes와 Duration 비교 — `uv run python -m src.benchmark report --benchmark-id scan-M-20260921T134454Z`; 6,000,000/98,132 대 1,000,000/19,322 Row/Byte 및 -22.2%를 `docs/benchmarks/03-experiment-c-scan.md`에 기록.

### 9-1-5. Experiment D — Cold vs Warm

- [x] `P9-16` Cold Run 절차로 5회 측정 — `uv run python -m src.benchmark run --scenario cache_effect --scale M --cold --fixture-old-rows 5000000 --fixture-current-rows 1000000` → 5/5 VALID, median 0.029063759초(`docs/benchmarks/04-experiment-d-cache-effect.md`).
- [x] `P9-17` Warm Run 절차로 5회 측정 — `uv run python -m src.benchmark run --scenario cache_effect --scale M --warm --fixture-old-rows 5000000 --fixture-current-rows 1000000` → 5/5 VALID, median 0.025608052초, Cold와 Hash 동일(`docs/benchmarks/04-experiment-d-cache-effect.md`).
- [x] `P9-18` Cache 효과를 별도 결과로 해석 — `PYTHONPATH=. uv run python scripts/d_control_experiments.py`; 교차 실행 대조에서 분리가 무너져 Cache 효과를 분해하지 못했다는 판정을 `docs/benchmarks/04-experiment-d-cache-effect.md` §대조군 실측에 기록.

## 9-2. 적용: Phase 6 완료 후 진행

Mart까지 포함한 전체 Pipeline을 측정 대상으로 삼는 실험이다. Bottleneck 선정과 개선 대상에 Warehouse 계층이 포함된다.

### 9-2-1. Scale 확장과 개선 Loop

- [x] `P9-19` S Scale에서 Harness 검증 — `uv run python -m src.benchmark run --scenario harness_overhead --scale S` → median `4.95e-05`초; A~D와 Overhead 모두 5/5 VALID(`docs/benchmarks/00-environment.md` §5·§6).
- [x] `P9-20` M Scale 전체 주요 실험 수행 — `uv run python -m src.benchmark run --scenario file_format --scale M` 및 위 P9-13·P9-16·P9-17 명령 → B/C/D Raw 5회·Median·Hash는 `docs/benchmarks/02-experiment-b-file-format.md`~`04-experiment-d-cache-effect.md`; A는 비용·누적 원천 제약으로 S 정본을 유지한 범위 결정(`01-experiment-a-extract.md` §M Scale 미실행).
- [x] `P9-21` L Scale 실행 또는 자원 한계 Evidence 기록 — `uv run python -m src.benchmark run --scenario file_format --scale L`; Scan/Cache는 `--fixture-old-rows 25000000 --fixture-current-rows 5000000`으로 실행 → B/C/D/Overhead 5/5 VALID, 메모리·Swap 관측과 A 제외 근거는 `docs/benchmarks/05-scale-limits.md`.
- [x] `P9-22` 가장 큰 Bottleneck 하나 선정 — `docs/architect-review/038_phase9-task16-bottleneck-selection.md`: M Scale `file_format`에서 `fetchmany` Tuple 변환 10.251초와 Hash Loop 10.253초를 실측해 병목을 선정하고, `to_arrow_reader` 대조 0.295초 및 architect 사전 승인을 기록했다.
- [x] `P9-23` 개선 적용 후 동일 조건 재측정 — `docs/benchmarks/06-improvement.md`: 같은 M Fixture·실행 창에서 전후 각각 csv/parquet 5회를 수행해 median 15.9%/17.6% 감소와 두 Arm의 동일 Result Hash를 기록했다.
- [x] `P9-24` Baseline/개선 결과와 Trade-off 문서화 — `docs/benchmarks/06-improvement.md`: 같은 M Fixture에서 Before/After 각각 csv/parquet 5회, median -15.9%/-17.6%, 20 Run 동일 Hash, Arrow Batch 메모리·간접성 및 Run Record 한계를 기록.

```text
Baseline
    ↓
Bottleneck 관측
    ↓
하나의 변경 적용
    ↓
같은 Dataset/환경/Scenario로 5회 재측정
    ↓
Median과 Result Hash 비교
```

## 실험 통제 Matrix

### 9-3. 사후 조치: 운영 Bronze 혼입 제거와 다음 실행 격리

- [x] `P9-25` 044 판정에 따라 운영 Source·Metadata·Bronze·Catalog를 Seed 기준으로 재기준화하고,
  Extract·File Format이 전용 DB·Bucket 없이는 실행되지 않게 했다. 재기준화 전 Inventory,
  백업, `bench-*` 428건 집계와 재기준화 후 `bench-*` 0건은
  [044 판정 실행 기록](../architect-review/044_benchmark-bronze-isolation-ruling.md#5-실행-기록)에 남겼다.

각 비교에서 다음 값이 같아야 한다.

| 통제 변수                       | 기록 위치              |
| ------------------------------- | ---------------------- |
| Dataset Scale/Checksum          | Benchmark Run Metadata |
| Random Seed                     | Benchmark Run Metadata |
| Git Commit                      | Benchmark Run Metadata |
| Python/Dependency/Image Version | 환경 Metadata          |
| Host/WSL/Docker Resource        | 환경 Metadata          |
| Query/Scenario                  | Scenario 정의          |
| Cold/Warm 조건                  | Run별 Flag와 절차      |
| 출력 정확성                     | `result_hash`          |

## 결과 문서 형식

```text
가설
측정 대상과 제외 범위
환경과 Dataset
실행 명령
Raw 5회 결과
Median
Result Hash
관측된 Bottleneck
변경 사항
개선 후 Raw 5회/Median
차이와 해석
한계
```

개선율은 원본 Raw 값에서 재계산 가능해야 하며, 예시는 다음 형태로 표현한다.

```text
Baseline Median 18.2s
Improved Median 7.4s
Duration 59% 감소
Result Hash 동일
```

## 범위 밖

- 근거 없는 목표 처리시간 선언
- 서로 다른 결과를 만드는 Query의 속도 비교
- 단일 실행값만을 대표 결과로 사용
- Cache 상태가 다른 결과를 같은 모집단으로 집계
- Benchmark를 위해 신뢰성 계약을 약화하는 변경
- `src/common/row_hash.py`의 `hash_cursor_rows` Arrow 경로 전환(`docs/architect-review/040_backlog-row-hash-arrow-candidate.md`)

## 요구사항 추적

| 구분 | 연결 항목                              | 증거                                 |
| ---- | -------------------------------------- | ------------------------------------ |
| PRD  | Section 20 비기능 요구사항과 Benchmark | 환경/Raw/Median 문서                 |
| FR   | FR-19 1M+ Scale                        | M/L Scale 결과                       |
| FR   | FR-20 Benchmark                        | 실험 A~D                             |
| AC   | AC-17 Benchmark                        | Raw 5회, Median, Hash, 환경 Metadata |

## 산출물

- 재사용 가능한 Benchmark Harness
- S/M/L Dataset 정의와 Checksum
- 실험 A~D의 Raw Result
- Median과 Result Hash를 포함한 결과 문서
- Bottleneck 분석과 최소 1개 개선 전후 비교
- `docs/benchmarks/`의 환경/재현 가이드

## 파일·폴더별 변경 요약

| 경로 | 변경 내용 |
| --- | --- |
| `src/benchmark/` | Scenario/Config, 측정·Metadata·Raw JSONL 저장, Cache 초기화와 A~D·Overhead 실험을 구현했고, `settings.py`에서 Extract·File Format의 전용 DB·Bucket 격리 Guard를 추가했다. |
| `tests/benchmark/` | Harness·측정·Hash·각 실험의 단위/통합 검증과 Arrow Batch Hash 등가성 검증, Benchmark 격리 값 누락·운영 값 재사용 거부 Test를 추가했다. |
| `scripts/d_control_experiments.py`, `scripts/profile_file_format_read.py` | Cache 순서 교락 대조와 File Format 병목 구간 분해를 재현 가능한 진단으로 추가했다. |
| `docs/benchmarks/` | 환경, A~D Raw 5회·Median·Hash, L Scale 자원 관측, 개선 전후·트레이드오프·재현 명령과 044 이후 저장소 격리 전제를 기록했다. |
| `docs/architect-review/` | 실험 방법·Cache/Extract 교정·Task 16 병목 선정과 개선 검수의 승인 근거를 기록했다. |

## Definition of Done

- [x] 모든 `P9-*` Task가 완료됐다 — 이 문서의 `P9-01`~`P9-24` 전부 `[x]`이며, 각 항목은 실행 명령과 `docs/benchmarks/` 또는 `docs/architect-review/` 증거 경로를 함께 기록한다.
- [x] 주요 실험마다 Raw 5회 결과가 있다 — `docs/benchmarks/01-experiment-a-extract.md`~`04-experiment-d-cache-effect.md`, `06-improvement.md`의 모든 Arm Raw 5회와 `data/benchmarks/{benchmark_id}/runs.jsonl`.
- [x] 대표값으로 Median을 계산했다 — `uv run python -m src.benchmark report --benchmark-id <benchmark_id>`; 각 결과 문서가 Raw 배열에서 Median을 기록(예: `docs/benchmarks/03-experiment-c-scan.md` §M Scale).
- [x] 비교 전후 Result Hash가 같다 — `docs/benchmarks/06-improvement.md`: Before/After 두 Benchmark ID 20 Run 전부 `66d27f16...`이며 모두 VALID.
- [x] 환경과 Dataset Metadata가 누락 없이 기록됐다 — `docs/benchmarks/00-environment.md` §2·§4의 Host/Lock/Image/Seed 및 `src/benchmark/metadata.py`의 Run Record 수집.
- [x] M Scale이 완료되고 L Scale은 성공 또는 자원 한계가 증명됐다 — `docs/benchmarks/05-scale-limits.md` §4: B/C/D/Overhead L Run 성공, A는 누적 원천과 20시간 이상 외삽 비용으로 실행하지 않은 범위를 §1·§2에 명시.
- [x] AC-17이 통과한다 — `uv run python -m src.benchmark report --benchmark-id file_format-M-20260922T000636Z`; 환경 Metadata·Raw 5회·Median·동일 Result Hash를 `docs/benchmarks/00-environment.md`, `06-improvement.md`에 함께 보존.

## Portfolio Evidence

- Raw Result와 Median 계산이 연결된 표/그래프
- Full/Incremental Scan Rows와 Duration 비교
- CSV/Parquet Bytes와 Duration 비교
- Cold/Warm 차이
- Bottleneck 관측에서 개선 결정으로 이어지는 기록
- 정확성 Hash가 유지된 개선 결과

## 권장 Commit

```text
perf: add reproducible benchmark evidence
```

## 다음 Phase 인계

Phase 10은 Benchmark가 검증한 Mart를 소비 대상으로 사용한다. BI 연결을 위해 Mart Grain이나 Metric 의미를 변경하지 않는다.
