# 040. 백로그 — `hash_cursor_rows` Arrow 경로 전환 후보

- 일자: 2026-09-22
- 상태: **백로그(미착수)**. Phase 9 범위 밖으로 유지한다. lead 지시로 기록만 남긴다.
- 근거: [[038_phase9-task16-bottleneck-selection]] §5,
  [[039_phase9-task16-improvement-review]] §5, `docs/benchmarks/06-improvement.md`

## 1. 내용

`src/common/row_hash.py:51`의 `hash_cursor_rows`가 Task 16에서 고친
`_hash_rows_with_payload_size`와 같은 `cursor.fetchmany(batch_size)` Tuple
Loop다. Task 16 실측은 이 패턴이 M Scale(1,000,000 Row) 기준 Row 적재에
약 10.25초를 쓰고, 그중 약 3초가 `to_arrow_reader` Columnar 경로로 회수
가능함을 보였다(csv -15.9%, parquet -17.6%).

## 2. 착수 시점

**Phase 9 종료 후.** 지금 하면 Task 16의 변수 1개가 깨진다.

## 3. 영향 범위 — 벤치마크가 아니라 정합성 계약이다

| 호출 지점 | 성격 |
| --- | --- |
| `src/warehouse/mart_hash.py:53` | Mart Hash. 정합성 계약 경로 |
| `src/benchmark/result_hash.py:21` | Benchmark 결과 Hash |
| `src/benchmark/experiments/extract.py:461` | 실험 A 결과 Hash |

Mart Hash가 걸려 있어 Task 16과 위험이 다르다. Hash 값이 한 Bit라도 바뀌면
기존에 저장된 Mart Hash와 어긋나고, 그것은 성능 회귀가 아니라 정합성 사고로
보인다.

## 4. 착수 전 반드시 증명할 것

`canonical_row_json`은 `Decimal`을 `format(value, "f")`, `date`를
`isoformat()`, `UUID`를 `str()`로 쓰고, Offset 없는 `datetime`은
`ValueError("Mart timestamp must include a UTC offset")`로 거절한다
(`src/common/row_hash.py:33-48`). 즉 Hash는 **Cursor가 돌려주는 Python
타입에 의존한다.**

`fetchmany` Tuple과 Arrow `to_pylist()`가 같은 Python 타입을 주는지는
Mart Column 타입별로 따로 확인해야 한다 — 최소 `DECIMAL`(Scale 보존),
`DATE`, `TIMESTAMP`(naive), `TIMESTAMPTZ`(Offset 보존), `UUID`, `NULL`.
Task 16의 Hash 동일성 확인은 `sellers` 한 테이블 Schema 안에서만 성립한
결과이므로 이 경로의 근거로 쓸 수 없다.

착수 조건:

1. 위 타입 전부에 대해 두 경로의 Hash 동일성을 Test로 고정한다.
2. 실제 Mart 1개 이상에서 전환 전후 Mart Hash가 같음을 실측한다.
3. Phase 9와 같은 방식으로 Before/After를 같은 실행 창에서 측정한다.
4. 하나라도 어긋나면 전환하지 않는다. 성능보다 Hash 안정성이 먼저다.

## 5. 기대 이득

Mart Hash는 Row 수에 선형이므로 Task 16과 같은 비율(약 15~17%)을 기대할
근거는 있으나, 이 경로를 실측한 데이터는 아직 없다. 착수 시 먼저 측정한다.
