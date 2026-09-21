# 035 — Phase 9 실험A 시각 Anchor 산출 방식 판정

- 일자: 2026-09-21
- 판정자: architect
- 대상: `src/benchmark/experiments/extract.py` `_anchor_times`
- 선행 판정: [032](032_phase9-shared-source-accumulation.md), [033](033_phase9-extract-t0-anchor-collision.md), [034](034_phase9-extract-watermark-noop-gate.md)

> Phase 9는 사용자 지시로 중단 상태다. 재개 시점의 출발점 기록이며 재실행 지시가 아니다.

## 보고된 증상

`extract-XS-20260921T092941Z`. Watermark 사후 조건 Gate는 5회 전부 통과. 그러나 `result_hash`는 5회 전부
불일치(`Full=dd286f3e993f`, `Incremental=c4091cd51cb7`, 회차 내내 고정).

`run1 orders cursor_range` 시작 Cursor가 `2026-09-21T10:05:49` — 이번 실행의 t0(09:29:41)도
t_boundary(09:59:41)도 아니고, **직전 XS 검증(`extract-XS-20260921T090549Z`)의 t1**이다.

## 진단 확인

developer 분석이 맞다. 그리고 이번 것이 가장 중요한 발견이다.

`_anchor_times`가 `t1 = t0 + 1시간`을 만들고, Delta Generator가 그 시각을 `updated_at`에 찍는다.
032가 확정했듯 원천은 영구 누적이다. 따라서 **각 실행이 자기 시작 시각보다 1시간 앞선 "가짜 미래" 행을
원천에 영구히 남긴다.** 다음 실행이 그 1시간 안에 시작하면 자기 T0가 남겨진 미래 행보다 과거가 된다.

그러면 Setup의 `t0_incremental` 적재(빈 Watermark에서 상한 없이 읽음)가 이전 실행의 미래 행까지
쓸어 담고, Watermark가 이번 T0가 아니라 이전 실행 t1에 가서 앉는다. Incremental Arm의 Row 집합이
Full Arm과 달라지고 Hash가 갈린다.

033 §2가 "t0는 기존 누적 행 최대 시각보다 항상 뒤"를 성립 조건으로 적었는데, 이 조건은 `benchmark_id`의
벽시계 시각에서 Anchor를 뽑는 한 **보장되지 않는 가정**이었다. 재시도가 잦을수록 오염이 쌓인다는
지적도 맞다. 재시도를 멈추고 물어본 판단이 옳았다.

## 판정

| 항목 | 판정 |
| --- | --- |
| 대안 (1): Ingestion에 읽기 상한 옵션 추가 | 반려 |
| 대안 (2): 재시도 간격 1시간 강제 | 반려 |
| 대안 (3): Benchmark 전용 원천 스키마 격리 | 반려 |
| 대안 (4): Anchor를 원천 실측 최대 시각에서 산출 | 승인 (정본) |

### (1) 반려

운영 Ingestion에 Benchmark 전용 상한 옵션을 넣는 것이다. 계획의 "Benchmark 편의로 Ingestion 경로나
신뢰성 계약을 바꾸지 않는다" 제약에 정면으로 걸린다. 게다가 원인을 안 없앤다 — 가짜 미래 행은 계속 쌓이고,
상한으로 가릴 뿐이다.

### (2) 반려

회피책이다. 1시간 대기는 XS 사전 검증의 목적(빠른 반복 확인)을 없앤다. 원인도 그대로 남는다.

### (3) 반려

`table_config`가 `public` Source Table을 가리키고, Generator·Ingestion·Warehouse가 전부 그 이름에 묶여 있다.
Phase 9 범위를 넘는 구조 변경이고, 032에서 초기화를 반려한 근거(공유 상태를 Benchmark 편의로 건드리지 않는다)가
그대로 적용된다.

## 정본 — Anchor를 벽시계가 아니라 원천에서 뽑는다

`_anchor_times`가 `benchmark_id`의 벽시계 시각을 쓰는 것을 그만둔다.
9개 Source Table의 Cursor Timestamp 최대값을 실측하고, 그 뒤에 Anchor를 세운다.

```
max_existing = MAX(table_config(t).cursor_timestamp_column) over 9개 Source Table   # 없으면 고정 기준 시각
t0           = max_existing + 1분
t_boundary   = t0 + 1분
t1           = t_boundary + 1분
```

- `t0 > max_existing`이 **가정이 아니라 계산으로 보장된다.** 033 §2의 성립 조건이 구조적으로 참이 된다.
- 한 실행의 `t1`이 다음 실행의 `max_existing`이 되므로 Anchor가 단조 증가한다. 충돌이 재발할 수 없다.
- 간격을 1시간에서 1분으로 줄인다. 실행마다 원천 시각이 1시간씩 미래로 밀리는 것을 막는다.
  간격의 크기는 실험에 아무 의미가 없다 — 필요한 건 `t0 < t_boundary < t1` 순서뿐이다.
- 대기가 없다. XS 사전 검증을 연속으로 돌려도 된다.
- 기존에 쌓인 오염 행은 정리하지 않는다. 다음 실행의 `t0`가 그보다 뒤로 계산되므로 저절로 해소된다.

### 사전 조건 검사 추가

Anchor 산출 직후, 9개 테이블에 `cursor_timestamp_column >= t0`인 행이 없음을 확인한다.
하나라도 있으면 `RuntimeError`로 멈춘다. Setup 51분을 태우기 전에 실패하게 만든다.

### Anchor 기록

Anchor는 이제 `benchmark_id`가 아니라 원천 상태에서 나온다. 따라서 `t0`/`t_boundary`/`t1` 세 값을
Run Metadata에 남기고 결과 문서에 싣는다. 032가 정한 "실측값만 쓴다"와 같은 원칙이다.
`t_boundary`는 이미 `config.parameters`로 회차에 전달되고 있다 — 반복 간 일관성은 그대로 유지된다.

## 이 수정으로 Hash가 맞는 이유

- Setup `t0_incremental`: 빈 Watermark에서 전량을 읽고, 최신 행이 이번 T0이므로 Watermark가 T0 끝에 선다.
- 회차별 Incremental: `cursor_before_timestamp(t_boundary)` = T0 끝. 그 이후를 읽으니 Delta만 읽는다.
- Full: 전 구간 앞으로 되감아 전량(과거 누적 + T0 + Delta)을 읽는다.
- Incremental Catalog = `t0_incremental`(과거 누적 + T0) + Delta = Full의 집합과 같다.

## 후속 (재개 시)

- developer: 정본 반영, 사전 조건 검사 추가, Anchor 3값 기록. XS 사전 검증 → 통과 시 S Scale 실험A 재실행 → Task 13 마무리 보고.
- 재개 시점은 lead 지시를 따른다.
