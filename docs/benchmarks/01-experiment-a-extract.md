# 01. 실험 A — Extract(Full vs Incremental)

버전 1. Task 14(M Scale Baseline) 범위 결정 반영. **M Scale은 실행하지 않았다** —
아래 "M Scale 미실행" 절 참조. S Scale 결과를 정본으로 싣는다.

> Scenario: `extract` · Scale: S(100,000 주문) · Repeats: 5 · Cold: 아니오(Warm)
> Benchmark ID: `extract-S-20260921T113012Z`

## 가설·측정 범위

원천(Postgres)에서 Bronze로 내리는 방식 두 가지를 비교한다.

- `full`: Watermark 없이 대상 테이블 전체를 매번 다시 추출한다.
- `incremental`: `t0`(이전 추출 시점) 이후 변경분만 Watermark Cursor 기준으로
  추출한다.

두 Arm은 서로 다른 SQL을 실행하므로 `scan`(실험 C)과 달리 같은 `result_hash`를
Row 집합 동일성이 아니라 **Watermark Anchor(t0/t_boundary/t1)로 고정한 같은
변경 구간을 가리키는지**로 검증한다(`src/benchmark/experiments/extract.py`,
[[035_phase9-extract-anchor-derivation]]). Anchor 산출 방식은
[[031_phase9-extract-watermark-and-change-rate]] → [[033_phase9-extract-t0-anchor-collision]]
→ [[034_phase9-extract-watermark-noop-gate]] → [[035_phase9-extract-anchor-derivation]]
로 4차례 정정을 거쳤다.

## 재현

```bash
uv run python -m src.benchmark run --scenario extract --scale S
uv run python -m src.benchmark report --benchmark-id extract-S-20260921T113012Z
```

`run` Subcommand가 `PREPARE_HOOKS`를 통해 `prepare_extract_fixture`(Watermark
Anchor 산출)를 반복 시작 전 자동으로 호출한다.

## 결과 — S Scale

```
Incremental: raw=[5.771192877, 5.802919774, 5.847424009, 5.858535690, 5.938126311]
             median=5.847424009
Full:        raw=[281.230140027, 281.563224499, 282.089758063, 282.399340809, 285.429070391]
             median=282.089758063
result_hash(양쪽 동일): e21c24726b9c3b5600cb6faa9cbe0fc2f4044884ac68e8b52ac469547b563773
change_rate(양쪽 동일): 0.013475091293743515 (약 1.35%)
```

5/5 `VALID`, 두 Arm `result_hash` 일치.

Full 대 Incremental 비율: `282.089758063 / 5.847424009 ≈ 48.2배`.

Watermark Anchor: `t0=2026-09-21T11:52:53+00:00`, `t_boundary=11:53:53`,
`t1=11:54:53`. 원천 Table별 t0/delta/t1은 `docs/benchmarks/00-environment.md`
§4에 있다(orders: t0=732110, delta=10000, t1=742110 — change_rate 1.35%는
이 값에서 나온다).

Harness Overhead(`4.95e-05`초, `docs/benchmarks/00-environment.md` §5)와
비교하면 Incremental 최소 관측치(5.771초)조차 Harness 오버헤드의 약
116,000배로, 두 Arm 모두 Harness 잡음이 아니라 실제 작업 시간을 재고 있다.

## M Scale 미실행

Task 14 범위는 lead가 결정했다 — 실험 B/C/D/Harness Overhead만 M Scale로
진행하고, 실험 A는 S Scale 결과를 정본으로 유지한 채 M Scale에서 재실행하지
않는다. 이건 측정 실패가 아니라 범위 결정이며, 이유는 두 가지다.

1. **Setup 소요 시간.** XS(주문 500건, 실측 Setup 487.8초)와 S(주문
   100,000건, 실측 Setup 1931.2초) 두 실측점을 Ingest(누적 행 수 비례)와
   Generator(주문 건수 비례) 두 성분으로 분리해 M(주문 1,000,000건)으로
   외삽하면 약 15,071초(약 251분, 약 4.2시간)로 추정된다. 이 값은 2개
   실측점의 외삽이지 실측값이 아니다.
2. **원천 영구 누적.** 원천 DB는 Benchmark 실행마다 초기화되지 않고 영구
   누적된다([[032_phase9-shared-source-accumulation]]) — M Run을 돌려도
   그 시점 원천에는 이전 모든 실행의 누적분이 섞여 있어, S Run과 절대
   행 수·절대 시간을 비교하는 것 자체가 무효다. Scale 간 비교라는 원래
   목적을 M Run으로는 달성할 수 없다.

### 잃은 것

Full 대 Incremental 비율이 Scale과 change_rate에 따라 어떻게 변하는지는
측정하지 못했다. 남은 것은 S Scale 단일 지점(change_rate 약 1.35%, 비율
약 48.2배) 하나뿐이며, 이 비율이 Scale이 커지거나 change_rate가 달라질 때
어떻게 움직이는지는 이 Phase 범위에서 알 수 없다.

## 알려진 한계

- Bronze Object 정리(Rebaseline)는 이번 Phase 범위에서 명시적으로 제외했다
  ([[032_phase9-shared-source-accumulation]] §5).
