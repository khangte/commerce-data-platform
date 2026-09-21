# Phase 9 Benchmark — Design

> 작성: 2026-09-21 · architect
> 기준 문서: `docs/phases/phase-09-benchmark.md`, `docs/phases/ROADMAP.md` §Milestone 3, `PRD_v1.14.md` §20 / FR-19 / FR-20 / AC-17
> 선행 상태: Phase 0–8 완료 (`ea928e6 feat(reliability): add phase8 fault-injection suite and fix contract gaps`)

## 1. 목표

고정된 Dataset·환경·Query에서 Baseline과 개선 전후의 **차이**를 정량 증명한다. 절대 처리시간은 성공 조건이 아니다.
모든 비교는 Raw 5회 값, Median, `result_hash`, 환경 Metadata를 함께 남긴다.

## 2. 선행 상태 점검 (2026-09-21 기준)

| Phase 9 선행 조건 | 상태 | 근거 |
| --- | --- | --- |
| Phase 8 Scenario 안정 통과 | 충족 | `tests/reliability` 18 passed, `phase-08-reliability.md` DoD 전 항목 `[x]` |
| 결정적 Dataset 생성·Scale 식별 | 충족 | `src/generator/config.py`의 `random_seed`/`order_count`, `deterministic_inputs()` |
| Full/Incremental Logical Hash 계산 | 충족 | `src/warehouse/mart_hash.py` `mart_logical_hashes`, `tests/integration/test_incremental_full_refresh_hash_integration.py` |
| Host/WSL/Docker Resource 기록 | **미구현** | 해당 수집 코드 없음. Task로 신설 |
| Phase 6/7/8 전 Task 완료 (9-2 선행) | 충족 | phase-05~08 문서 체크박스 미완 0건 |

결론: 9-1 골격과 9-2 적용을 한 번에 진행할 수 있다. 신규 구현이 필요한 것은 Benchmark Harness 자체뿐이다.

## 3. 배치 결정

### 3.1 Harness는 `src/benchmark/`에 둔다

Phase 8 Harness는 pytest 전용 보조물이라 `tests/`에 뒀다. Phase 9 Harness는 Phase 9 산출물 목록의 "재사용 가능한
Benchmark Harness"이자 직접 실행하는 CLI다. 따라서 `src/benchmark/` 패키지로 만들고 `uv run python -m src.benchmark`로 실행한다.
`tests/benchmark/`에는 Harness 자체의 단위 테스트만 둔다.

### 3.2 모듈 경계

| 모듈 | 책임 |
| --- | --- |
| `src/common/row_hash.py` | Canonical Row JSON + 누적 SHA-256. `mart_hash.py`의 기존 사설 구현을 여기로 올리고 재사용 |
| `src/benchmark/config.py` | `ScaleProfile`(S/M/L), `BenchmarkScenario`, `RunConfig`, `scenario_config_hash` |
| `src/benchmark/metadata.py` | git commit, python version, `uv.lock` hash, Docker Image Version, Host/WSL Spec |
| `src/benchmark/measure.py` | Wall Time, CPU(user/sys), Max RSS, Read/Write Bytes, Row Count |
| `src/benchmark/result_hash.py` | 임의 DuckDB Query 결과의 Logical Hash |
| `src/benchmark/cache.py` | Cold/Warm 절차와 `cache_reset_method` 기록 |
| `src/benchmark/store.py` | Raw JSONL 적재, Median 집계, 비교표 렌더 |
| `src/benchmark/runner.py` | N회 반복, Cold/Warm 분리, Hash Gate |
| `src/benchmark/experiments/` | 실험 A·B·C·D 구현 |
| `src/benchmark/__main__.py` | CLI |

### 3.3 저장 위치

- Raw 결과: `data/benchmarks/{benchmark_id}/runs.jsonl` — Git 미추적(`.gitignore` 추가). Phase 8 `data/reliability/`와 같은 규칙.
- 정리된 증적 문서: `docs/benchmarks/` — 작성하되 **커밋하지 않는다**(팀 규칙: `docs/` 중 `docs/architect-review/`만 커밋).
- Harness 코드·테스트·README는 커밋한다.

## 4. 측정 계약

### 4.1 Run Metadata

PRD §20 필드를 그대로 쓰고 다음 4개만 추가한다. 그 외 필드는 늘리지 않는다.

```text
PRD §20 필드 전체
+ cache_reset_method     # drop_caches | process_restart_only
+ change_rate            # 실험 A에서만. 그 외 null
+ cursor_range           # 실험 A에서만. 그 외 null
+ scenario_config_hash   # 같은 Scenario 정의로 측정했음을 증명
```

`benchmark_id`는 `{scenario}-{scale}-{utc_compact}` 형식이고 한 Scenario의 5회 Run이 같은 `benchmark_id`를 공유한다.
`run_number`는 1..5다.

### 4.2 자원 수집 방법 (신규 의존성 없음)

- Wall Time: `time.monotonic()`
- CPU/Max RSS: `resource.getrusage(RUSAGE_SELF)` 및 자식 Process를 쓰는 Scenario는 `RUSAGE_CHILDREN` 델타
- I/O Bytes: `/proc/self/io`의 `read_bytes`/`write_bytes` 델타. 읽기 실패 시 `null`로 남기고 숨기지 않는다
- `psutil` 등 신규 패키지를 추가하지 않는다

### 4.3 정확성 Gate

비교하는 두 Arm의 `result_hash`가 다르면 **그 비교의 성능 수치를 채택하지 않는다.** Runner가 `ResultHashMismatch`를
올리고 해당 `benchmark_id`를 `INVALID`로 기록한다. 수치는 Raw에 남기되 Median과 비교표에서 제외한다.

### 4.4 Cold/Warm

- Cold 절차: `sync` 후 `/proc/sys/vm/drop_caches`에 `3` 기록. 권한이 없으면 실패를 숨기지 않고
  `cache_reset_method="process_restart_only"`로 내려간다(새 Process + 새 DuckDB Connection + 대상 Compose 서비스 재시작).
- Cold Run과 Warm Run은 **절대 같은 모집단으로 집계하지 않는다.** `store.py`가 `is_cold_run`으로 분리 집계한다.
- 대표값은 평균이 아니라 Median이다.

## 5. 실험 설계

### 5.1 실험 A — Full vs Incremental Extract

동일한 최종 Bronze 상태 T1을 만드는 두 경로를 측정한다.

```text
Arm Full:        Watermark Rewind → T1 상태 전량 수집
Arm Incremental: T0 상태 수집 → 고정 변경률 델타 적용 → 델타만 수집
Gate:            두 Arm의 T1 Bronze Logical Hash 동일
```

- 측정 창은 **수집(ingestion)만** 포함한다. Generator 실행 시간은 제외하고 그 사실을 문서에 명시한다.
- Rewind는 기존 `src/ingestion/reprocess.py`를 쓴다. 새 Rewind 경로를 만들지 않는다.
- `change_rate`와 `cursor_range`를 Run Metadata에 기록한다.

### 5.2 실험 B — CSV vs Parquet

Bronze는 Parquet 전용 계약이다(`src/ingestion/bronze.py`). **이 계약을 바꾸지 않는다.**
Benchmark 전용 CSV Mirror를 Committed Parquet에서 `COPY ... TO ... (FORMAT CSV)`로 만들고, 같은 Column/Row 범위를
같은 Query로 양쪽에서 읽어 비교한다. CSV는 `data/benchmarks/` 아래에만 존재하고 Pipeline 어디에서도 읽지 않는다.

측정: File Bytes, Scan Bytes, Duration. Gate: 두 Query 결과의 `result_hash` 동일.

### 5.3 실험 C — Full Scan vs Filtered Scan

같은 분석 결과를 만드는 두 Query를 비교한다.

```text
Baseline: 전체 Column/Row Scan 후 집계
Filtered: Predicate Pushdown + Column Projection 후 같은 집계
Gate:     최종 집계 결과 result_hash 동일
```

Scan Rows/Bytes는 DuckDB `PRAGMA enable_profiling='json'` 출력에서 읽는다. 서로 다른 결과를 만드는 Query를 비교하지 않는다.

### 5.4 실험 D — Cold vs Warm

실험 C의 Filtered Scan Scenario 하나를 고정해 Cold 5회, Warm 5회 측정한다. Cache 효과는 A/B/C의 개선 효과와
섞지 않고 **별도 결과로만** 해석한다.

## 6. Scale과 한계

| Scale | Orders | 목적 |
| --- | ---: | --- |
| S | 100K | Harness와 측정 오버헤드 검증 |
| M | 1M | 기본 Portfolio Baseline |
| L | 5M | Page Write/Memory/파일 크기 한계 관측 |

Dataset 식별은 `random_seed` + `order_count` + Seed Hash + Source Row Count Hash로 한다.
8GB RAM에서 L이 실패하면 **실패 조건과 관측치를 그대로 기록한다.** 성공한 것처럼 쓰지 않는다.

## 7. 개선 Loop (9-2)

```text
M Scale Baseline
  → 가장 큰 Bottleneck 1개 선정 (근거: Duration 기여도 + Scan Bytes)
  → 변수 1개만 변경
  → 같은 Dataset/환경/Scenario로 5회 재측정
  → Median 비교 + result_hash 동일 확인
  → Trade-off 문서화
```

한 번에 하나의 변수만 바꾼다. Benchmark를 위해 신뢰성 계약(Manifest 검증, Watermark CAS, Lease, Publish Gate)을
약화하는 변경은 채택하지 않는다.

## 8. 범위 밖

- 목표 처리시간 선언
- 서로 다른 결과를 만드는 Query의 속도 비교
- 단일 실행값을 대표값으로 사용
- Cold/Warm 혼합 집계
- Bronze Parquet 계약 변경
- 신규 런타임 의존성 추가

## 9. 미해결 확인 사항 (lead 확인 필요)

1. L Scale 5M Orders 생성은 실행 시간이 길다. 실패 시 "자원 한계 Evidence"로 종료해도 되는지(Phase 9 문서는 허용).
2. `docs/benchmarks/` 증적 문서는 팀 규칙상 커밋하지 않는다. Portfolio 제출 시 별도 처리가 필요한지.
