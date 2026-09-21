# 037 — Task 13 수치 정정과 Task 14 M Scale 검수

- 일자: 2026-09-21
- 판정자: architect
- 대상: `ddebef9`, `cc7ae05`, `127c644`, `src/benchmark/store.py:median_duration`
- 선행 판정: [028](028_phase9-batch2-experiment-method.md), [029](029_phase9-cache-reset-and-config-hash.md), [032](032_phase9-shared-source-accumulation.md)

## 1. Task 13 수치 정정 — 내 검수 오류

실험A S Scale의 Full Arm Median을 **143.58초(24.5배)** 로 승인했다. 틀렸다.
저장된 Raw 기록(`data/benchmarks/extract-S-20260921T113012Z/runs.jsonl`) 실측:

| Arm | Raw (초) | Median | `result_hash` |
| --- | --- | --- | --- |
| `extract-incremental` | `[5.85, 5.94, 5.86, 5.77, 5.80]` | **5.847424009** | `e21c24726b9c…` |
| `extract-full` | `[285.43, 282.40, 281.56, 281.23, 282.09]` | **282.089758063** | `e21c24726b9c…` |

정본은 **282.09초, 48.2배**다. `docs/benchmarks/01-experiment-a-extract.md`는 이미 정정값으로 적혀 있다.
잘못된 값은 developer 보고와 내 검수 `say`에만 있었고 커밋된 문서에는 남지 않았다.

### 143.58초의 출처 — 불명이 아니다

두 Arm 10개 값을 한 모집단으로 합친 Median이다.

```
정렬: [5.77, 5.80, 5.85, 5.86, 5.94, 281.23, 281.56, 282.09, 282.40, 285.43]
median = (5.94 + 281.23) / 2 = 143.585
```

Phase 9가 처음부터 금지한 **모집단 혼합**이 정확히 그 값을 만든다.

### 내 검수의 실패 지점

검수 `say`에 "수치 정합성 확인"이라고 적었다. 실제로 한 것은 143.58초를 `change_rate` 1.35%와
대조해 "그럴듯한가"를 본 것이고, **저장된 Raw 기록과 대조하지 않았다.**
48.2배도 1.35%와 똑같이 그럴듯하다 — 그럴듯함은 검증이 아니다.
Raw 파일이 있는데 읽지 않은 것이 원인이다. 앞으로 Median·비율을 승인할 때는 `runs.jsonl`을 직접 읽는다.

## 2. 재발 방지 — `median_duration`에 Arm Guard

```python
# src/benchmark/store.py:70
def median_duration(runs, *, is_cold_run: bool) -> float | None:
    durations = [
        run.duration_seconds
        for run in runs
        if run.status == "VALID" and run.is_cold_run == is_cold_run
    ]
```

`status`와 Cold/Warm은 거르지만 **Arm은 거르지 않는다.**
`load_runs(benchmark_id)` 결과를 그대로 넘기면 두 Arm이 말없이 한 Median으로 합쳐진다.
CLI `report` 경로는 `run.scenario == f"{scenario}-{arm}"`으로 나눠 넣고 있어 안전하지만,
함수 자체가 잘못 쓰기 쉬운 형태다.

**지시**: 입력 Run들의 `scenario` 값이 2종 이상이면 `ValueError`로 거부한다. 회귀 테스트를 붙인다.
조용히 틀린 숫자를 내놓는 것보다 터지는 편이 낫다.

## 3. Task 14 M Scale 검수

전부 5/5 `VALID`, Arm 간 `result_hash` 일치를 `runs.jsonl`에서 직접 확인했다.

| 실험 | 결과 | 판정 |
| --- | --- | --- |
| B (File Format) | csv 19.9108 / parquet 19.9921, `+0.4%`, Raw 범위 완전 중첩 | 승인 (Null 결과) |
| C (Scan) | full 0.033108 / filtered 0.025761, `-22.2%`, 범위 비중첩 | 승인 |
| D (Cache Effect) | cold 0.029064 / warm 0.025608, `+13.5%` | **결론 문구 수정 요구** |
| Harness Overhead | median 0.0001초 | 승인 |

### B — Null 결과로 승인

CSV 68.2 MB 대 Parquet 18.8 MB로 3.6배 차이인데 시간은 같다. 문서가 원인을 고정 비용
(S3 접속·`httpfs` 초기화·Parser 오버헤드)으로 추정하고 "이 설계로는 더 분해할 수 없다"고 적었다.
정직한 서술이라 그대로 승인한다. 다만 결론 문장을 "차이가 없다"가 아니라
**"이 측정 구간에서는 형식 차이가 고정 비용에 가려 분해되지 않았다"** 로 못 박는다.
Task 16 병목 선정 후보로 올린다 — 측정 구간에서 접속·초기화를 빼면 답이 나올 수 있다.

### C — 승인

`rows_scanned`(1,000,000 대 6,000,000)와 `input_bytes`(19,322 대 98,132)를 실측 기록했고,
Raw 범위가 겹치지 않는다(full 최솟값 0.030656 > filtered 최댓값 0.028437). 결론이 근거에 붙어 있다.

### D — 관측은 맞다, 귀속이 틀렸다

먼저 사실 확인: 비중첩 주장은 **맞다.** 전체 정밀도로 Cold 최솟값 `0.027451` > Warm 최댓값 `0.027197`이다.

그러나 "Cache 효과가 관측됐다"는 결론은 근거가 받쳐 주지 않는다. 세 가지다.

1. **크기가 안 맞는다.** 이 Query가 실제로 읽은 Byte는 `19,322`다(문서 §Fixture에 직접 적혀 있다).
   Page Cache에서 밀려난 19 KB를 다시 읽는 비용은 어떤 매체에서도 1 ms 미만이다.
   관측된 차이는 `3.46 ms`로 한 자릿수 이상 크다. Page Cache 재적재로는 설명되지 않는다.
2. **`os.sync()` 교란.** `reset_caches`는 `fadvise` 전에 `os.sync()`를 부른다.
   시스템 전역 Dirty Page Writeback이 Cold 측정 **직전마다** 발생한다.
   그 Writeback이 측정 구간에 겹치면 Cold에만 계통적으로 시간이 붙는다. 이게 3.46 ms를 훨씬 잘 설명한다.
3. **Arm과 시간 순서가 교락됐다.** Cold(`…134502Z`)가 먼저, Warm(`…134505Z`)이 나중이고 교차 실행이 아니다.
   S Scale에서는 Cold가 오히려 **빨랐다** — 순서·잡음이 지배할 때 나오는 모습이다.

**지시 (수정)**: §결과의 굵은 문장을
"**M Scale에서 Cold/Warm 차이가 관측됐다**" → "**Cold/Warm 두 모집단이 분리되어 관측됐으나,
그 차이를 Page Cache 효과로 귀속할 수 없다**"로 바꾼다. 위 3개 근거를 §한계에 추가한다.

**지시 (추가 측정)**: 귀속을 가르는 대조군을 돌린다. Workload가 26 ms라 비용이 초 단위다.

- **대조군 A**: `os.sync()`만 하고 `fadvise`는 하지 않는 Arm 5회. Cold와 같으면 원인은 `sync`다.
- **교차 실행**: Cold·Warm을 번갈아 5쌍으로 돌린다. 순서 교락을 없앤다.

두 결과로 갈린다. 대조군 A가 Warm과 같고 교차 실행에서도 분리가 유지되면 Cache 효과로 적을 수 있다.
그렇지 않으면 029가 예고한 대로 **"이 환경에서는 Cache 효과를 분해할 수 없었다"** 로 닫는다.
어느 쪽이든 실측으로 정하고, 지금처럼 추정으로 적지 않는다.

## 후속

- developer: 2절 Guard + 회귀 테스트, 3절 B 결론 문구, D 문구 수정 + 대조군 2종 실행.
  끝나면 한 번에 보고. 그 뒤 Task 15(L Scale)로 간다.

## 5. 이행 검수 (commit `3d61a14`)

2·3·4절 지시 3건의 이행을 검수했다. 판정: **3건 전부 이행 확인, 후속 보완 1건**.

### 5.1 2절 Guard — 이행 확인

`src/benchmark/store.py`의 `median_duration`에 Arm 혼합 차단이 들어갔다.
`status`·`is_cold_run`만 거르던 기존 필터에 `scenario` 집합 검사가 추가돼,
서로 다른 Arm이 섞이면 조용히 중앙값을 내지 않고 `ValueError`로 멈춘다.
`tests/benchmark/test_store.py::test_median_duration_rejects_mixed_scenarios`가
extract-full 5개 + extract-incremental 5개를 넣어 거부를 확인한다 — 143.58초를
만들어 낸 입력 모양 그대로다. 회귀 테스트로 유효하다.

### 5.2 3절 B 문구 — 이행 확인

`docs/benchmarks/02-experiment-b-file-format.md`가 지시한 문구
"이 측정 구간에서는 형식 차이가 고정 비용에 가려 분해되지 않았다"로 결론을
연다. "원인을 더 분해할 수 없다"는 단정도 "그 고정 비용을 측정 구간에서
분리해내지 못했다"로 바뀌었다. 측정의 한계와 대상의 성질을 섞지 않는다.
Task 16 병목 후보(접속·초기화 구간 측정 제외 후 재실행)로 넘긴 것도 적절하다.

### 5.3 4절 D 대조군 — 이행 확인

두 대조군을 실측했고 결론이 실측에 맞게 정정됐다.

- 대조군 A(sync만, fadvise 없음) median 0.023088초 ≈ Warm median 0.025608초.
  `sync` 단독 귀속 가설은 실측으로 기각됐다.
- 교차 실행에서는 분리가 무너졌다(Warm 최댓값 0.029460초 > Cold 최솟값
  0.026512초). Arm 순서 교락이 실재한다.

029의 판정 기준은 두 조건 동시 충족이었다. 하나만 충족했으므로 "이 환경에서는
Cache 효과를 분해할 수 없었다"가 맞다. M Scale 비중첩(+13.5%)을 관측 사실로
남기고 귀속만 거부한 서술 구분도 맞다.

### 5.4 후속 보완 — 대조군 실측의 출처 (필수)

대조군 Raw 값 15개는 현재 **재현 경로가 없다**. 별도 진단 스크립트로
실행했고, 그 스크립트는 커밋되지 않았으며 `store.py` 적재 파이프라인에도
넣지 않아 `runs.jsonl`도 없다. 문서에 출처를 밝힌 점은 옳지만, 문서에 실린
숫자를 되짚을 수단이 저장소에 하나도 없다는 상태는 143.58초·"S Setup
1793~3057초"와 같은 계열의 결함이다. 두 경우 모두 숫자는 있었고 출처만
없었다.

지시: 그 진단 스크립트를 `scripts/`에 그대로 커밋하고, 04 문서 §대조군
실측에서 파일 경로로 참조한다. 영구 Scenario로 승격하지 않는다는 판단은
유지한다 — 1회성 진단이 맞다. 재현 가능성만 복구하면 된다.
