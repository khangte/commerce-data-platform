# 03. 실험 C — Full Scan vs Filtered Scan

버전 2. Task 14(M Scale Baseline) 결과 추가.

> S Scale — Benchmark ID: `scan-S-20260921T044215Z` (Repeats 5, Cold: 아니오)
> M Scale — Benchmark ID: `scan-M-20260921T134454Z` (Repeats 5, Cold: 아니오)

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
([[028_phase9-batch2-experiment-method]] §1)에 따라 이 이탈을 채택했다: 같은
SQL을 쓰면 두 Arm의 결과 동일성이 구성상 보장되고, "한 번에 변수 하나만
바꾼다"는 원칙도 계획서 방식보다 더 엄격히 지킨다.

## Fixture

Bronze 실데이터를 쓰지 않는다. `old`/`current` 두 Bucket으로 나뉜 **로컬 합성
Parquet Fixture**를 직접 만들어 쓰며, `WHERE bucket = 'current'`로 최신 Bucket만
골라내는 집계 Query를 두 Arm이 똑같이 실행한다.

- 경로: `data/benchmarks/fixtures/{scale}/{old,current}.parquet` (gitignored)
- Row 수는 Scale의 `order_count`가 아니라 `RunConfig.parameters`에 명시한
  값을 그대로 쓴다(`fixture_old_rows`, `fixture_current_rows`) — 028 §1이
  `old_rows = current_rows * 20` 같은 고정 배수를 Scale 표(S=100K/M=1M/L=5M
  주문 건수)와 어긋난다는 이유로 제외했으므로, 각 Scale의 값은 호출자가
  직접 고른 상수다.
- Fixture는 Scale별로 **한 번만** 만들고 이후 회차·다른 실험(실험 D)이 재사용한다.
  이미 있으면 다시 쓰지 않는다(`ensure_scan_fixture`).

| Scale | fixture_old_rows | fixture_current_rows | old.parquet 실측 | current.parquet 실측 |
| --- | --- | --- | --- | --- |
| S | 2,000 | 100 | (미기록, S 실행 시점) | (미기록, S 실행 시점) |
| M | 5,000,000 | 1,000,000 | 39.6 MB (41,528,819 bytes 추정치와 동일 계열) | 7.9 MB |

M Scale 값은 S 그대로 키우면(2,000/100 → 논리상 20,000/1,000 정도) 실질
Byte 수가 여전히 너무 작아 실험 D의 Cache 효과가 노이즈에 묻힐 가능성이 커서,
사전에 Parquet 행당 Byte 수를 직접 측정(`pyarrow.parquet.write_table` 실측,
약 8.3 byte/row)한 뒤 결정한 값이다.

## 결과 — S Scale

```
baseline(full_scan):     raw=[0.01105, 0.01152, 0.03283, 0.01292, 0.00997]
                          median=0.01152 result_hash=702773e0...b3742319
improved(filtered_scan): raw=[0.01027, 0.01335, 0.02622, 0.01062, 0.01047]
                          median=0.01062 result_hash=702773e0...b3742319
change: -7.8% (baseline median -> improved median)
```

| Arm | rows_scanned | input_bytes |
| --- | --- | --- |
| `filtered_scan` (Pushdown 켜짐) | 100 | (미기록) |
| `full_scan` (Pushdown 꺼짐) | 2,100 | (미기록) |

Filtered Arm은 Fixture 전체(2,100 Row) 중 `current` Bucket(100 Row)만 Scan한다.
Duration 차이(-7.8%)는 S Scale·Local Parquet 조건에서는 크지 않았다.

## 결과 — M Scale

```
baseline(full_scan):     raw=[0.030655661, 0.046328353, 0.033107826, 0.031184125, 0.034329235]
                          median=0.033107826 result_hash=ba79ec6c...8388dd2a1f711
improved(filtered_scan): raw=[0.023903870, 0.025761190, 0.028436982, 0.022709242, 0.026985934]
                          median=0.025761190 result_hash=ba79ec6c...8388dd2a1f711
change: -22.2% (baseline median -> improved median)
```

5/5 `VALID`, 두 Arm `result_hash` 일치.

| Arm | rows_scanned | input_bytes |
| --- | --- | --- |
| `filtered_scan` (Pushdown 켜짐) | 1,000,000 | 19,322 |
| `full_scan` (Pushdown 꺼짐) | 6,000,000 | 98,132 |

`filtered_scan`의 `rows_scanned`(1,000,000)는 `fixture_current_rows`와 정확히
일치한다 — Row Group Pruning이 `old.parquet`(전부 `bucket='old'`)을 완전히
건너뛰고 `current.parquet`만 읽었다는 뜻이다. `full_scan`은 Pushdown 없이
두 파일 전체(5,000,000 + 1,000,000 = 6,000,000 Row)를 읽는다.

S Scale(-7.8%)보다 M Scale(-22.2%)에서 Pushdown Arm의 상대적 이득이 더 크게
측정됐다 — Fixture가 커질수록 건너뛰는 Row 비중(83%, `old.parquet` 5,000,000
Row)이 절대 시간에서 차지하는 비중도 커진 것으로 보인다.

## 재현

```bash
uv run pytest tests/benchmark/test_experiment_scan.py -q
uv run python -m src.benchmark run --scenario scan --scale S \
  --fixture-old-rows 2000 --fixture-current-rows 100
uv run python -m src.benchmark run --scenario scan --scale M \
  --fixture-old-rows 5000000 --fixture-current-rows 1000000
uv run python -m src.benchmark report --benchmark-id <benchmark_id>
```

Fixture가 이미 Scale 디렉터리에 있으면 위 Row 수 인자는 무시되고 기존 파일을
그대로 쓴다(`ensure_scan_fixture`).
