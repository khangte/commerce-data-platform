# 07. Publish Gate 심각도 분리 — 지표 Test 실패가 묶는 주문 Fact

> 증적: `data/reliability/gate-split/20261110T112023854694Z.json` (Git 미추적)
> 실험: `tests/reliability/test_gate_split_experiment.py` (실제 PostgreSQL·SeaweedFS·dbt, 121.99초)
> 측정일: 2026-11-10

## 1. v1 설계 의도와 실험 질문

[ADR 016](../adr/016-publish-mart-via-warehouse-file-swap.md)의 Publish는 복사한 Build 파일에서 dbt build·test를 돌리고, 성공한 Build만 Published 파일로 교체한다. 모든 Table이 같은 Build에서 나오고, 어떤 Test든 실패하면 마지막 성공 Mart가 그대로 남는다. 일관성을 우선한 설계이며 이 보장은 지금도 유지된다.

운영 시나리오를 따져 보면 이 Gate의 비용이 드러난다. 재계산 가능한 지표 Test 하나만 실패해도 멀쩡한 주문 Fact까지 게시되지 못한다. 그래서 일부 일관성을 내주고 관측으로 보완하는 v2를 측정했다.

**실험 질문: 지표 Test 하나가 실패하면 주문 Fact는 얼마나 묶이는가.**

## 2. 측정값

같은 커밋, 같은 Bronze에서 지표 계층 Canary `rpt_publish_gate_canary` 하나만 실패시켰다. v1과 v2의 차이는 dbt var `metrics_test_severity`(`error` / 기본 `warn`)뿐이다.

| 구분 | 값 |
| --- | --- |
| 기준 게시의 Bronze 경계(`bronze_as_of`) | `2026-10-19 11:40:08.083967+00:00` |
| 기준 게시본 `fct_order` | 99,824행, 최대 주문 시각 `2026-10-19 20:40:00+09` |
| 최신 Bronze로 만든 Build의 `fct_order` | 99,829행, 최대 주문 시각 `2026-10-19 20:50:00+09` |

| 항목 | v1 (`error`) | v2 (`warn`) |
| --- | --- | --- |
| Publish 상태 | `FAILED` (`DBT_TEST_ERROR`) | `PUBLISHED` |
| 실패·경고 Test | 실패 1건: `test.commerce_data_platform.rpt_publish_gate_canary` | 경고 1건 (`tests_warned = 1`) |
| 게시본 `fct_order` | 99,824행 (Hash·행 수 불변) | 99,829행 (실패 Build와 동일) |
| 묶인 주문 | 5행 | 0행 |
| 게시본 주문 시각 지연 | 600초 | 0초 |
| Run report 판정 | 미측정 (`mart_publish_runs`에 `FAILED` 기록) | `DBT_TEST_WARN` |

- v1에서 실패한 노드는 Canary 하나뿐이다. Fact·Dimension Test는 모두 통과했는데도 주문이 게시되지 못했다.
- 5행·600초는 Bronze Batch 하나(10분 간격)를 경계로 잡은 실험 값이다. 규모 일반화가 아니라 "묶인다 / 묶이지 않는다"의 증거로 읽어야 한다.
- 세 실행 모두 `bronze_as_of` 재현을 위해 `--full-refresh`로 돌렸다. 운영의 증분 Build 시간은 측정하지 않았다.
- 회귀: R-14와 기존 Canary Gate(`tests/reliability/test_r14_dbt_failure.py`, `tests/integration/test_publish_gate_dbt_integration.py`) 2개 통과(272.41초). `error` Gate 동작은 그대로다.

## 3. 가정 시나리오 (계산값, 측정 아님)

**가정**

- Warehouse DAG는 자체 스케줄이 없고(`schedule=None`) 원천 DAG가 실행마다 트리거한다. 원천 주기는 기본 `@hourly`이며 `SOURCE_DAG_SCHEDULE`로 바꿀 수 있다.
- `DATA_STALE` 판정은 `data_lag_s > stale_after_s`이고, `stale_after_s`는 주기의 2배(`@hourly`면 2시간)다. `data_lag_s`는 판정 시각에서 게시본 `max(purchase_at)`를 뺀 값이다.
- `purchase_at`은 실제 시계를 따르고, 지표 Test 실패는 수정될 때까지 매 실행 반복된다.
- δ는 k개 주기를 뺀 나머지 지연이다(`data_lag_s` − k × 주기). 게시본 최신 주문 시각과 그 실행 정시의 차이, 판정 시 실행 완료 지연이 여기에 들어간다.
- 이 표는 0 ≤ δ < 1시간으로 가정한다. 그래서 첫 실패(1시간 + δ)는 임계값 아래이고 v2 지연(δ)도 임계값을 넘지 않는다. δ 값은 운영에서 측정한 것이 아니다.

21:00 실행까지 성공하고 22:00부터 실패, 09:00 실행 전에 수정한 경우:

| 실행 시각 | 연속 실패 k | v1 게시본 지연 | v1 `DATA_STALE` | v2 게시본 지연 | v2 판정 |
| --- | --- | --- | --- | --- | --- |
| 22:00 | 1 | 1시간 + δ | 아니오 | δ | `DBT_TEST_WARN` |
| 23:00 | 2 | 2시간 + δ | 예 (δ = 0이면 경계라 아니오) | δ | `DBT_TEST_WARN` |
| 00:00 | 3 | 3시간 + δ | 예 | δ | `DBT_TEST_WARN` |
| 08:00 | 11 | 11시간 + δ | 예 | δ | `DBT_TEST_WARN` |
| 09:00 (수정 후) | 0 | δ | 아니오 | δ | 정상 |

- v1은 11회 차단되고 주문 Fact가 약 11시간 stale 상태로 남는다. 일반화하면 k회 연속 실패 시 지연은 약 k × 주기다.
- 비교가 `>`이므로 `DATA_STALE`은 δ > 0이면 2회째부터, δ = 0이면 3회째부터 켜진다.
- 판정 기준이 주기에 비례하므로 주기가 10분이면 같은 표에서 시간 단위만 10분으로 바뀐다.

## 4. 포기한 일관성과 보완

| 구분 | 내용 |
| --- | --- |
| 포기 | 게시 파일 안에서 rpt가 Fact와 어긋날 수 있다(계층 간 정합성). |
| 유지 | 파일 단위 원자 교체(모든 Table이 같은 Build 출신), Fact·Dimension 무결성 Gate. |
| 보완 | `mart_publish_runs.tests_warned`와 Run report `DBT_TEST_WARN`으로 경고 게시를 기록한다. `metrics_test_severity=error`로 v1 Gate에 돌아갈 수 있다. |
| 미보완 | BI 사용자는 수정 전까지 어긋난 rpt를 본다. 문제는 Run report로만 드러난다. |

v1이 틀렸던 것이 아니라, 일관성을 우선한 선택의 비용을 운영 시나리오로 확인하고 지표 계층에 한해 그 일부를 내준 것이다.

**남은 한계 (2단계 동기)**

- 분리 단위가 계층 전체다. 어긋난 rpt 하나가 경고로 게시되면 같은 파일의 다른 지표와 구분되지 않고, 사용자에게 표시할 방법도 없다.
- Fact·Dimension Test가 실패하면 여전히 전체 게시가 막힌다. 예를 들어 공유 Dimension 실패 한 건이 관계없는 도메인까지 묶는다.
- 위 두 한계는 도메인별 게시 분리로 이어지며 이번 단계의 범위가 아니다.
