# 033 — Phase 9 실험A T0 시각과 경계 시각 충돌 판정

- 일자: 2026-09-21
- 판정자: architect
- 대상: `src/benchmark/experiments/extract.py` `_anchor_times`, `_ensure_watermark_at`
- 선행 판정: [030](030_phase9-extract-experiment-repeat-protocol.md), [031](031_phase9-extract-watermark-and-change-rate.md), [032](032_phase9-shared-source-accumulation.md)

## 보고된 증상

`extract-S-20260921T071933Z`. Setup 1,793초 정상 완료. 5회 반복 전부 예외 없이 완주.
그러나 5회 전부 `INVALID` — 두 Arm의 `result_hash`가 구조적으로 다르다
(`incremental=aebe1d4f369b`, `full=406d004902a9`, 각각 5회 내내 고정).
Row 수는 정상: `orders t0=620460 delta=10000 t1=630460`.

## 진단 확인

developer의 근본 원인 분석은 맞다. 코드로 확인했다.

```python
# src/benchmark/experiments/extract.py:72, 78-81
t_boundary, t1 = _anchor_times(config.benchmark_id)
run_generator(
    _generator_config(source_snapshot_id, config.scale.random_seed, t_boundary, order_count_t0),
    postgres,
)
```

T0 Generator의 `logical_date`가 `t_boundary` 자체다. Generator는 논리 시각을 `updated_at`에 찍으므로
**이번 실행 T0 행 전부가 정확히 `updated_at == t_boundary`** 가 된다.

```sql
-- src/ingestion/extract.py:153  cursor_before_timestamp
WHERE {cursor_timestamp_column} < %s
```

`cursor_before_timestamp`는 **엄격 부등호**다. 따라서 `target`은 이번 T0 전체를 건너뛰고 과거 실행 행을
가리킨다. Setup 적재가 세워 둔 `current.cursor`(= 이번 T0의 마지막 행)와 항상 다르다.
`_ensure_watermark_at`의 동등 비교가 매 회차 실패하고, 실제 되감기가 일어나, 이번 T0 전체가
Incremental Catalog에 두 번 들어간다. Full Arm은 한 번에 전량을 읽어 중복이 없다. Hash가 갈린다.

Row 수 지표가 정상으로 보인 이유도 여기 있다. `change_stats`는 Postgres를 직접 세므로 적재 중복과 무관하다.

## 판정

| 항목                                                                            | 판정        |
| ------------------------------------------------------------------------------- | ----------- |
| 제안: 캡처한 정확 Cursor로 `rewind_watermark` + `acquire_table_lease` 직접 호출 | 반려        |
| `_anchor_times`를 3개 시각(t0 < t_boundary < t1)으로 분리                       | 승인 (정본) |

## 1. 제안 반려

제안은 동작한다. 그러나 문제의 원인을 고치지 않고 우회한다.

`rewind_tables`의 "경계 이상 전부 재처리"는 잘못된 계약이 아니다. 이 실험이 원하는 의미와 **정확히 같다** —
Incremental Arm이 읽어야 할 것이 바로 "경계 이후 Delta 전부"다. 두 의미가 어긋난 건 계약 때문이 아니라
**T0 행을 경계와 같은 순간에 찍었기 때문**이다. 보고의 "T0 행이 정확히 boundary 시각에 찍히는 한 두 의미가
절대 안 맞는다"는 맞는 말이고, 그렇다면 고칠 곳은 되감기 호출 방식이 아니라 T0를 찍는 시각이다.

제안대로 가면 비용이 붙는다.

1. Lease 획득·해제와 `expected_version` 전달을 Benchmark가 직접 떠안는다. `rewind_tables`가 `try/finally`로
   보장하던 해제 책임이 실험 코드로 옮겨 온다. 측정 코드가 실패하면 Lease가 남는다.
2. "Full Arm은 기존 되감기만 재사용하고 새 경로를 만들지 않는다"는 전제가 깨진다. 이 전제는 계획의
   "신뢰성 계약을 Benchmark 편의로 건드리지 않는다" 제약과 같은 뿌리다.
3. Setup에서 캡처한 Cursor를 회차 간에 실어 나르는 상태가 하나 늘어난다. 지금 실험A가 반복적으로
   실패해 온 원인이 전부 "측정 밖 상태가 회차에 새는 것"이었다.

## 2. 정본 — 시각을 3개로 분리한다

`_anchor_times`가 `t0`, `t_boundary`, `t1` 세 시각을 돌려준다. T0 Generator 호출의 `logical_date`를
`t_boundary`가 아니라 `t0`로 준다. 경계와 Delta는 그대로.

```
t0        = <benchmark_id 시각>
t_boundary = t0 + 30분
t1        = t0 + 1시간
```

- T0 Generator: `logical_date=t0`
- 경계: `t_boundary` (변경 없음)
- Delta Generator: `logical_date=t1` (변경 없음)

이러면 `cursor_before_timestamp(t_boundary)`가 `updated_at < t_boundary`인 마지막 Composite Cursor를
찾는데, 그게 바로 이번 T0의 마지막 행이다. Setup 적재 직후 Watermark도 같은 행이다
(`_fetch_upper_bound`와 `cursor_before_timestamp`가 같은 `_order_by`, 같은 Composite Key를 쓴다).
두 값이 일치하므로 `_ensure_watermark_at`이 회차마다 **진짜 무동작**이 되고 중복 적재가 사라진다.

Full Arm은 변경 없다. 전 구간 앞 경계로 되감아 전량을 읽는다.

### 성립 조건

`(t0, t_boundary)` 구간에 행이 없어야 한다. Generator만 Source를 쓰고 논리 시각에만 찍으므로 성립한다.
`t0`는 `benchmark_id` 시각이라 기존 누적 행(최대 `2026-09-21 07:10:43`)보다 항상 뒤다.
T0 호출이 건드리지 않는 테이블은 `target`과 Setup Watermark가 둘 다 과거 최대 Cursor로 같아져 역시 일치한다.

## 3. 변경 범위

`src/benchmark/experiments/extract.py` 한 파일. `_anchor_times` 반환값과 T0 `run_generator` 인자.
운영 코드 무변경. `_ensure_watermark_at` 로직 무변경 — 031의 판정은 그대로 옳고, 비교 대상이
어긋나 있던 것뿐이다.

## 4. 재실행 전 확인

되감기가 실제로 안 일어나는지 코드가 아니라 실행으로 확인한다. Incremental Arm의
`_ensure_watermark_at`이 회차마다 무동작 경로를 탔는지 로그로 남기고, 5회 전부 무동작이어야 한다.
한 번이라도 되감기가 실행되면 성립 조건이 깨진 것이므로 멈추고 보고한다.

## 후속

- developer: 2절 반영, 4절 확인 로그 추가, S Scale 실험A 재실행. Task 13 마무리 보고.
