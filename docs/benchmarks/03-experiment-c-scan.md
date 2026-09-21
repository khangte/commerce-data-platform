# 03. 실험 C — Full Scan vs Filtered Scan

> Scenario: `scan` · Scale: S(100 Orders) · Repeats: 5 · Cold: 아니오(Warm)
> Benchmark ID: `scan-S-20260921T044215Z`

## 가설·측정 대상

**계획서와 다르게 측정한다.** 계획서는 Filtered Arm에 Predicate/Projection
Pushdown이 들어간 SQL을, Baseline Arm에 전체 Column/Row를 읽는 SQL을 따로 써서
비교하도록 했다. 실제 구현은 **두 Arm이 완전히 같은 SQL**을 실행하고, `full_scan`
Arm에서만 DuckDB Optimizer 규칙 4개를 꺼서 Pushdown이 없던 상태를 재현한다.

```sql
SET disabled_optimizers='filter_pushdown,row_group_pruner,unused_columns,column_lifetime';
```

즉 이 실험이 실제로 재는 것은 "Query를 어떻게 쓰느냐"가 아니라 **"이 Optimizer
규칙 4개가 켜졌느냐 꺼졌느냐"**다. `filtered_scan` = 기본 설정(Pushdown 켜짐),
`full_scan` = 위 4개 규칙을 끈 상태(Pushdown 꺼짐). architect 조건부 승인
(`docs/architect-review/028_phase9-batch2-experiment-method.md` §1)에 따라 이
이탈을 채택했다: 같은 SQL을 쓰면 두 Arm의 결과 동일성이 구성상 보장되고,
"한 번에 변수 하나만 바꾼다"는 원칙도 계획서 방식보다 더 엄격히 지킨다.

## Fixture

Bronze 실데이터를 쓰지 않는다. `old`/`current` 두 Bucket으로 나뉜 **로컬 합성
Parquet Fixture**를 직접 만들어 쓰며, `WHERE bucket = 'current'`로 최신 Bucket만
골라내는 집계 Query를 두 Arm이 똑같이 실행한다.

- 경로: `data/benchmarks/fixtures/{scale}/{old,current}.parquet` (gitignored)
- Row 수는 Scale의 `order_count`가 아니라 `RunConfig.parameters`에 명시한
  값을 그대로 쓴다(`fixture_old_rows`, `fixture_current_rows`). 이번 실행은
  `old_rows=2000, current_rows=100`.
- Fixture는 Scale별로 **한 번만** 만들고 이후 회차·다른 실험(실험 D)이 재사용한다.
  이미 있으면 다시 쓰지 않는다(`ensure_scan_fixture`).

## 결과

```
baseline(full_scan):     raw=[0.01105, 0.01152, 0.03283, 0.01292, 0.00997]
                          median=0.01152 result_hash=702773e0...b3742319
improved(filtered_scan): raw=[0.01027, 0.01335, 0.02622, 0.01062, 0.01047]
                          median=0.01062 result_hash=702773e0...b3742319
change: -7.8% (baseline median -> improved median)
```

두 Arm의 `result_hash`가 일치한다 — 같은 SQL이므로 당연한 결과이며, 이 실험의
채택 근거이기도 하다.

Scan 단계에서 실제로 읽은 Row 수(`PRAGMA enable_profiling`의 `TABLE_SCAN`
Operator `operator_cardinality` 합, 측정 구간 밖 별도 Pass에서 확인):

| Arm | rows_scanned |
| --- | --- |
| `filtered_scan` (Pushdown 켜짐) | 100 |
| `full_scan` (Pushdown 꺼짐) | 2,100 |

Filtered Arm은 Fixture 전체(2,100 Row) 중 `current` Bucket(100 Row)만 Scan한다.
Duration 차이(-7.8%)는 이번 S Scale·Local Parquet 조건에서는 크지 않다 —
Fixture가 작아 I/O 자체가 가볍기 때문으로 보이며, M/L Scale에서 재확인이
필요하다(배치3).

## 재현

```bash
uv run pytest tests/benchmark/test_experiment_scan.py -q
uv run python -m src.benchmark run --scenario scan --scale S
uv run python -m src.benchmark report --benchmark-id <benchmark_id>
```
