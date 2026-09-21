# 034 — Phase 9 실험A 무동작 Gate 정정

- 일자: 2026-09-21
- 판정자: architect
- 대상: 033 §4 확인 조건, `src/benchmark/experiments/extract.py` `_assert_incremental_watermark_noop`
- 선행 판정: [031](031_phase9-extract-watermark-and-change-rate.md), [033](033_phase9-extract-t0-anchor-collision.md)

> Phase 9는 사용자 지시로 중단 상태다. 이 문서는 재개 시점의 출발점을 남기는 판정이며,
> 재실행 지시가 아니다.

## 보고된 증상

`extract-XS-20260921T090549Z`. run1은 9개 테이블 전부 무동작. run2 `customers`에서 실제 되감기 발생
(`cursor_before=t1(10:05:49)` → `cursor_after=t0(09:05:49)`), 033 §4 지시대로 `RuntimeError`로 정지.

## 진단 확인

developer 분석이 맞다. 측정 대상 Incremental Ingest는 읽기 상한이 없다. `t_boundary`는 DAG Run의
`logical_date` 메타데이터로만 쓰인다. 따라서 run1의 측정 Ingest가 Watermark를 t1까지 전진시키고,
run2 진입 시점에는 되감기가 **필요한 상태가 된다.**

## 판정 — 033 §4가 틀렸다

| 항목                                    | 판정                     |
| --------------------------------------- | ------------------------ |
| 033 §4 "5회 전부 무동작" 조건           | **철회.** 내 판정 오류다 |
| 대안 (a): `pipeline_name` 회차별 분리   | 반려                     |
| 대안 (b): "run1만 무동작 확인"으로 완화 | 반려                     |
| 사후 조건(Cursor 동등) 검사로 교체      | 승인 (정본)              |

033 §4는 "되감기가 일어나지 않는 것"을 불변식으로 적었다. 이건 틀렸다.
031이 `_ensure_watermark_at`을 만들 때 세운 불변식은 처음부터 **"되감기 실행"이 아니라 "목표 지점 보장"** 이었다
(`extract.py:193` docstring에도 그렇게 적혀 있다). 되감기가 일어나는지 여부는 회차마다 달라지는
정상 변동이고, run1 무동작은 T0 선적재 때문에 생기는 특수 경우일 뿐이다.
매 회차 되감기가 실제로 일어나는 것은 결함이 아니라 **요구사항이다** — 5회 반복이 같은 출발 조건에서
시작하려면 run2부터는 되감아야 한다.

## 1. 대안 (a) 반려 — 즉시 실패한다

`pipeline_name`을 회차별로 분리하면 매 회차 `get_or_create_watermark`가 Cursor `None`인 새 Row를 만든다.

```python
# src/ingestion/metadata.py:508-514
def _cursor_is_earlier(candidate: CursorPosition, current: CursorPosition) -> bool:
    if current.timestamp is None:
        return False
```

`current.timestamp is None`이면 항상 `False`를 돌려주므로 `rewind_watermark`가
`WatermarkRewindError("Rewind target must be earlier than the current cursor")`로 즉시 터진다.
동작하지 않는 방향이다.

설령 이 Guard가 없었다 해도 반려다. 빈 Watermark에서 시작하는 Incremental Ingest는 Cursor 이후 전부,
즉 **테이블 전량**을 읽는다. Incremental Arm이 Full Arm과 같아져 실험 자체가 무의미해진다.
보고의 "Full Arm처럼 빈 상태에서 새로 시작하는 셈이라 설계 의도와 달라질 수 있다"는 우려가 정확하다.
임의로 바꾸지 않고 물어본 판단이 옳았다.

## 2. 대안 (b) 반려 — 검사가 필요한 구간을 비운다

run1은 T0 선적재 덕분에 구조적으로 안전한 회차다. 드리프트가 생길 수 있는 곳은 run2~run5다.
그 구간의 검사를 빼면 Gate가 아무것도 막지 못한다.

## 3. 정본 — 사후 조건을 검사한다

되감기가 일어났는지가 아니라, **`_ensure_watermark_at` 호출 후 Watermark가 목표 지점에 있는지**를 본다.

```
for source_table in 9개 테이블:
    _ensure_watermark_at(postgres, pipeline, source_table, target_boundary)
    current = get_or_create_watermark(postgres, pipeline, source_table)
    target  = cursor_before_timestamp(postgres, table_config(source_table), target_boundary)
    current.cursor != target  → RuntimeError (멈추고 보고)
```

- 두 Arm 전부, 5회 전부 적용한다. Full Arm은 자기 경계를 `target_boundary`로 넣는다.
- 되감기 발생 여부는 실패 조건이 아니라 **기록 대상**이다. 회차별 `RewindOutcome`
  (`cursor_before`/`cursor_after`)을 남겨 두고, 결과 문서에 "run1 무동작, run2~5 되감기"로 적는다.
- 검사는 측정 밖에서 한다 (030 유지).

이 검사가 통과하면 5회 반복의 출발 조건이 같음이 보장되고, 그게 실험이 실제로 필요로 하는 불변식이다.

## 4. 남는 확인

`_ensure_watermark_at` 후 두 Arm의 출발 Cursor가 같아도, Incremental Ingest에 읽기 상한이 없다는 점은
그대로다. 반복 중 원천은 T1에서 동결되므로(030) 현재 설계에서는 문제가 없다.
Setup 종료 후 Source를 건드리는 코드가 생기면 이 전제가 깨진다 — 실험A에 그런 경로를 넣지 않는다.

## 후속 (재개 시)

- developer: 3절로 `_assert_incremental_watermark_noop`을 교체하고 이름도 사후 조건 검사에 맞게 바꾼다.
  XS 사전 검증 재개 → 통과 시 S Scale 실험A 재실행 → Task 13 마무리 보고.
- 재개 시점은 lead 지시를 따른다.
