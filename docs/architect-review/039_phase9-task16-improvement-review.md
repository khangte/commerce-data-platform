# 039. Phase 9 Task 16 — 개선 실행 결과 검수

- 일자: 2026-09-22
- 대상: `docs/benchmarks/06-improvement.md`(신규), `src/benchmark/experiments/file_format.py`,
  `docs/benchmarks/02-experiment-b-file-format.md`, `scripts/profile_file_format_read.py`(신규)
- 선행: [[038_phase9-task16-bottleneck-selection]] 승인 조건 5건
- 판정: **승인, 단 문서 보완 2건 선행**

## 1. 038 조건 대조

| 조건 | 결과 | 확인 방법 |
| --- | --- | --- |
| 1. Baseline 재측정(같은 실행 창) | 충족 | `file_format-M-20260922T000307Z`(00:03:48Z) / `...T000636Z`(00:07:10Z), 약 3분 간격. 두 실행의 `input_bytes`가 arm별로 동일(csv 71,500,057 / parquet 19,699,382) — Fixture 재생성 없음 |
| 2. `result_hash` 동일성 csv·parquet 양쪽 | 충족 | 두 `runs.jsonl` 20 Run 전부 `66d27f16e98f...bd33b`, `status`는 전부 `VALID` |
| 3. 진단 스크립트 커밋 | 커밋 시 확인 | `scripts/profile_file_format_read.py` 존재하나 아직 Untracked |
| 4. 02 문서 후보 문장 교체 | 충족 | 접속·초기화 후보를 기각으로 고쳐 쓰고 06으로 연결 |
| 5. B 결론 변화 기록 | 충족 | Arm 간 상대차 Before +1.70% → After -0.36%, 결론 유지로 기록 |

Median도 산출물에서 재계산해 문서 값과 일치함을 확인했다. `runs.jsonl`에는
Arm Column이 없어 `input_bytes`로 Arm을 가려 확인했다 — 문서의 arm별 Raw
배열 4개가 산출물과 정확히 일치한다(csv Before median 18.582611306, parquet
Before 18.898190720, csv After 15.629954482, parquet After 15.573698818).

`uv run ruff check .` 통과, `tests/benchmark/test_experiment_file_format.py`
1 passed / 2 skipped(SeaweedFS 통합 Skip). Arrow 경로 등가성 Unit Test를
새로 추가한 것은 요구 밖의 적절한 보강이다.

## 2. 보완 1 — 개선폭이 진단 예측보다 작은 이유를 써라

진단은 Duration 20.5초 중 Row 적재 10.251초, Hash Loop 10.253초로 갈랐고
Columnar 적재는 0.295초였다. 그대로 읽으면 절반 가까이 사라질 것처럼
보인다. 실제 개선은 -15.9% / -17.6%(약 3.0초)다.

차이의 실체를 문서가 설명해야 한다. `to_arrow_reader`로 바꿔도 Row 단위
Python 객체 생성이 사라지지 않는다 — `to_pylist()` + `zip`이 그 일을
대신 한다. 산수로 보면 적재 약 10.25초 중 실제로 없어진 것은 약 3.0초고
약 7초는 `to_pylist`/`zip` 쪽으로 남았다. Hash Loop 10.25초는 손대지
않았으므로 애초에 이번 변경의 상한은 약 50%였고, 그중 일부만 회수했다.

이건 개선의 흠이 아니라 진단의 해석 범위다. 0.295초는 "Row를 Python으로
꺼내지 않으면" 가능한 값이고, Row별 Hash를 유지하는 한 그 값에 도달할 수
없다. 06 문서에 이 한 단락을 넣어라. 없으면 다음 독자가 진단 수치에서
잘못된 상한을 기대한다.

## 3. 보완 2 — Before/After 실행의 코드 판별 근거를 명시하라

두 `runs.jsonl`의 `git_commit`이 모두 `a487eb23...-dirty`이고
`scenario_config_hash`(`14d25195`)·`dependency_lock_hash`도 같다. 즉 산출물
기록만으로는 어느 실행이 개선 전 코드인지 가릴 수 없다. 현재 -15.9%/-17.6%
주장은 실행 순서와 문서 서술에만 의존한다.

Harness를 이번에 고치라는 뜻은 아니다. 06 문서에 "Before-fix는 변경 적용
전에 실행했고, 두 실행의 `git_commit`이 같은 `-dirty`로 기록되어 Run
Record만으로는 구분되지 않는다"를 사실로 한 줄 남겨라. 032 §5(실행 간
절대 비교 무효)와 같은 종류의 한계 기록이다.

## 4. 사소 지적

- 06 "결과" 절의 "5/5 `VALID`, 4개 실행"은 실제 기록(benchmark 2건 × 10 Run
  = 20 Run 전부 `VALID`)과 셈이 어긋나게 읽힌다. "두 Benchmark ID 20 Run
  전부 `VALID`"로 고쳐라.
- `to_arrow_reader(10_000)`은 `src/ingestion/bronze.py:234,300`에서
  `to_arrow_reader(batch_size=50_000)`으로 Keyword 호출한다. 같은 저장소
  안에서 호출 형태를 맞춰 `batch_size=10_000`으로 써라. 동작은 같다.

## 5. 범위 유지 확인

`src/common/row_hash.py`의 `hash_cursor_rows`는 손대지 않았고 06 문서가
별도 후보로 기록했다 — 038 §5대로다. 변경은 `_hash_rows_with_payload_size`
한 함수에 그쳤다.
