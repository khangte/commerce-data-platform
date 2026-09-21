# 028. Phase 9 배치2 실험 방법 판정 (실험 A~D)

> 판정일: 2026-09-21 · architect
> 대상 커밋: `291b1f2`, `aaa2ce9`, `2dd706a`, `b7d6113`
> 기준: `docs/superpowers/plans/2026-09-21-phase9-benchmark.md` Task 9~12, `docs/superpowers/specs/2026-09-21-phase9-benchmark-design.md` §5

## 판정 요약

| Task | 실험 | 판정 | 사유 |
| --- | --- | --- | --- |
| 9 | A Full vs Incremental | 승인 | 실제 Ingestion만 측정, 기존 `reprocess.rewind_tables` 재사용, `src/ingestion/` 무변경 |
| 10 | B CSV vs Parquet | 승인 | Bronze 쓰기 경로 무변경, CSV Export가 측정 창 밖, 두 Arm Hash 일치 |
| 11 | C Full vs Filtered Scan | 조건부 승인 | 방법 이탈은 인정하나 Fixture 크기·재사용을 고쳐야 함 |
| 12 | D Cold vs Warm | **반려** | Fixture 생성이 측정 창 안에 있고 Cache Reset 뒤에 실행돼 Cold 측정이 성립하지 않음 |

공통 확인: `src/ingestion/` 변경 0건, 신규 런타임 의존성 0건, `ruff` clean, benchmark 테스트 41 passed / 2 skipped.

## 1. Task 11 — 설계 이탈 판정: 조건부 승인

### 이탈 내용

계획서는 Filtered Arm이 SQL에 Predicate Pushdown과 Column Projection을 추가하고 Baseline Arm은 전체 Column/Row를
Scan하도록 했다. 구현은 두 Arm이 **완전히 같은 SQL**을 실행하고, Full Scan Arm만 DuckDB
`disabled_optimizers='filter_pushdown,row_group_pruner,unused_columns,column_lifetime'`로 Pushdown 규칙을 끈다.

### 판단

이탈을 승인한다. 근거:

- Phase 9의 절대 규칙은 "서로 다른 결과를 만드는 Query를 비교하지 않는다"이다. 같은 SQL을 쓰면 결과 동일성이
  구성상 보장되고, 계획서 방식보다 이 규칙을 더 강하게 만족한다.
- "한 번에 하나의 변수만 변경한다"도 더 정확히 지켜진다. 변수는 Optimizer 규칙 하나다.

다만 **측정 대상의 의미가 바뀌었다.** 이 실험은 이제 "Query를 어떻게 쓰느냐"가 아니라 "Pushdown 규칙이 켜졌느냐"를
측정한다. 결과 문서가 이 둘을 같은 것처럼 서술하면 근거가 과장된다.

### 조건

1. 결과 문서(`docs/benchmarks/03-experiment-c-scan.md`)의 가설·측정 대상 절에 두 Arm이 같은 SQL이고 차이는 DuckDB
   Optimizer 규칙 4개뿐이라는 사실과, 끈 규칙 이름을 그대로 적는다.
2. 같은 문서에 이 실험이 Bronze 실데이터가 아니라 로컬 합성 Parquet Fixture를 쓴다는 사실을 적는다.
3. `old_rows = current_rows * 20` 고정 배수를 제거한다. M Scale에서 21M Row, L Scale에서 105M Row가 되어
   Scale 표(S=100K/M=1M/L=5M Orders)의 의미와 어긋난다. Fixture Row 수는 `RunConfig.parameters`의 명시 값으로 받는다.
4. Fixture를 반복 회차마다 다시 만들지 않는다. 아래 2절과 같은 수정이다.

## 2. Task 12 — 반려

### 결함

`run_cache_experiment`가 `measure()` 안에서 `run_filtered_scan_workload()`를 호출하고, 이 함수는 호출될 때마다
`tempfile.TemporaryDirectory`에 Parquet Fixture를 새로 쓴다. 두 가지가 동시에 깨진다.

1. **측정 창 오염** — 측정된 `duration_seconds`가 Query 시간이 아니라 Fixture 쓰기 시간 + Query 시간이다.
   Fixture 쓰기가 지배적이라 Cache 효과가 묻힌다.
2. **Cold 조건 무효화** — `run_experiment`는 회차 시작 전에 `reset_caches()`를 호출한다. 그런데 Fixture는 그
   Reset **이후에** 새로 기록된다. 방금 쓴 파일은 Page Cache에 올라가 있으므로 Cold Run이 실제로는 Warm 데이터를
   읽는다. 이 상태에서는 Cold와 Warm의 차이가 관측되지 않으며, 관측되더라도 Cache 효과로 해석할 수 없다.

Phase 9 원칙 "Cold/Warm을 구분하며 섞어서 비교하지 않는다"를 형식적으로는 분리했으나 실질적으로 위반한다.

### 부수 결함

- `RowCounts.rows_scanned=config.scale.order_count`는 측정값이 아니라 가정값이다. 실측하거나 `None`으로 둔다.

### 수정 지시

1. Fixture를 `data/benchmarks/fixtures/{scale}/`에 **한 번만** 만들고 반복 회차가 재사용한다. 이미 있으면 다시 쓰지 않는다.
2. Fixture 생성은 `measure()` 밖이면서 `reset_caches()` **앞**이어야 한다. 순서는 Fixture 준비 → Cache Reset → 측정이다.
3. `measure()` 안에는 Query 실행만 남긴다.
4. `rows_scanned`는 실측하거나 `None`으로 둔다. 가정값을 기록하지 않는다.
5. 수정 후 Cold Median과 Warm Median이 실제로 갈리는지 확인하고, 갈리지 않으면 그 관측을 숨기지 말고 그대로 보고한다.
   `cache_reset_method`가 `process_restart_only`면 Page Cache가 실제로 비워지지 않았다는 뜻이므로 차이가 작을 수 있고,
   그 경우 그 사실이 결과 해석의 일부다.

## 3. 배치3 착수 조건

Task 12 수정과 Task 11 조건 1~4를 반영한 뒤 배치3(Scale S/M/L)에 착수한다. Task 9, 10은 재작업 없이 확정한다.
