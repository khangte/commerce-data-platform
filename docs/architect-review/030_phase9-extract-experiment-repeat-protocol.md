# 030. Phase 9 실험 A 반복 실행 프로토콜 판정

> 판정일: 2026-09-21 · architect
> 증상: `BatchIdentityConflictError: Committed batch range differs from the current watermark` (실험 A 5회 반복 2회차)
> 선행 판정: [028](028_phase9-batch2-experiment-method.md), [029](029_phase9-cache-reset-and-config-hash.md)

## 1. 원인 판정

developer의 Root Cause 분석이 맞다. 다만 충돌은 증상이고, 설계 오류는 그 앞에 있다.

**실험 A가 측정 반복 안에서 원천을 변형한다.** `run_extract_experiment`가 회차마다 `run_generator()`를 호출해
Postgres Row를 누적시킨다. 그 결과 두 가지가 동시에 깨진다.

1. `BatchIdentity(dag_id, logical_date)`가 회차마다 같은데 Source Range가 달라져 Bronze 재사용 판정이 거부한다.
2. 더 근본적으로, **회차마다 측정 조건이 달라진다.** T0 Order 수가 회차마다 커지므로 5회는 같은 조건의 반복이 아니다.

1번만 고치면 2번이 남는다. 그래서 제시된 대안 1(`run_number`를 `dag_id`에 섞기)은 단독으로는 반려한다.
대안 2(T0 선적재를 반복 밖으로)도 단독으로는 반려한다 — Batch가 이미 Commit되어 있으면 2회차부터 재사용으로
즉시 반환되어 실제 작업을 측정하지 못한다.

## 2. 채택 설계: 원천은 한 번만 만들고, 회차마다 제어 상태만 되돌린다

핵심은 **Generator를 반복 루프에서 완전히 빼는 것**이다. Full Arm과 Incremental Arm의 차이는 원천 상태의
차이가 아니라 **Watermark 시작 위치의 차이**다. 같은 T1 원천 위에서 Watermark를 어디에 두느냐로 두 Arm이 갈린다.

### 프로토콜

**설정(5회 반복 전, 딱 한 번)**

1. Seed + Generator로 원천을 최종 상태 T1까지 결정적으로 만든다. 생성 Row가 경계 시각 `t_boundary`를 사이에 두고
   앞뒤로 나뉘게 한다. `t_boundary` 이전 = T0 모집단, 이후 = Delta.
2. Delta Row 수를 실측해 `change_rate`로 기록한다. 추정하지 않는다.
3. **이후 5회 반복 동안 `run_generator()`를 다시 호출하지 않는다.** 원천은 T1에서 동결된다.

**회차마다(`measure()` 밖)**

4. Arm별 Watermark를 되감는다. 기존 `rewind_tables`를 그대로 쓴다.
   - Full Arm: `boundary`를 모든 Row보다 이른 고정 시각으로 준다 → Watermark가 최초 위치로 간다 → 전량 수집.
   - Incremental Arm: `boundary=t_boundary` → Watermark가 T0 경계로 간다 → Delta만 수집.
   - `rewind_tables`의 `pipeline_name`은 Arm별 Benchmark 전용 이름으로 **반드시 명시한다.** 기본값
     `<table>_bronze`를 쓰면 운영 Watermark를 건드린다.
5. `dag_id`에 Arm과 회차 번호를 섞어 회차마다 다른 `BatchIdentity`를 만든다. 멱등 재사용 경로가 작동하면
   실제 작업이 측정되지 않으므로, 회차마다 새 Identity여야 한다.

**측정(`measure()` 안)**

6. `ingest_table` 호출만 넣는다. Generator, Rewind, Fixture 준비, Catalog 작성은 전부 밖이다.

**검증**

7. 두 Arm의 T1 Bronze Logical Hash가 같아야 한다. 나아가 **5회 반복 전부에서 같아야 한다.** 한 회차라도 다르면
   그 비교는 `INVALID`다.
8. Arm별 `cursor_range`(되감기 전 → 수집 후)를 Run Metadata에 기록한다.

### 이 설계가 해결하는 것

- 원천이 동결되므로 5회가 진짜 같은 조건의 반복이 된다.
- 회차마다 Identity가 새로우므로 재사용 경로로 빠지지 않고 실제 수집 비용을 측정한다.
- 되감기 비용은 `measure()` 밖이므로 측정에 섞이지 않는다. 대안 3이 걱정한 "정리 비용 혼입"이 생기지 않는다.
- `src/ingestion/`에 새 경로를 만들 필요가 없다. `rewind_tables`의 기존 `boundary` 인자가 두 Arm을 모두 표현한다.

## 3. `runner.py` 구조 변경 판정

`run_number`가 실험 함수에 전달되지 않는 제약은 실재하며 고쳐야 한다. 단 `experiment_fn(config, run_number)`로
Signature를 넓히지 않는다. 실험 5개를 모두 고쳐야 하고, 대부분은 회차를 쓰지 않는다.

**채택:** `RunConfig`에 `run_number: int = 0` 필드를 더하고, `run_experiment`가 회차마다
`dataclasses.replace(config, run_number=i)`로 바꿔 넘긴다. 호출 규약은 `experiment_fn(config)` 그대로 유지된다.
`run_number`는 실행 배선 값이므로 `_NON_DEFINING_PARAMETERS`와 같은 이유로 `scenario_config_hash`에서 제외한다.

## 4. 부수 사항

- 회차마다 새 Identity로 수집하므로 Bronze Object가 반복 수만큼 쌓인다. 실험 종료 후 정리하고, 정리는
  측정 창 밖에서 한다. 누적량을 결과 문서 한계 절에 적는다.
- 실험 A는 실행 시간이 길다. M Scale 진입 전에 S Scale에서 이 프로토콜이 5회 전부 통과하는지 먼저 확인한다.
