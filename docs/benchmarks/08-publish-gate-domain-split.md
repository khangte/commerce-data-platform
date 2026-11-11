# 08. Publish Gate 도메인 분리 — Test 실패가 묶는 도메인

> 증적: `data/reliability/gate-domain-split/20261110T131419718338Z.json` (Git 미추적)
> 실험: `tests/reliability/test_domain_gate_experiment.py` (실제 PostgreSQL·SeaweedFS·dbt, 563.20초)
> 측정일: 2026-11-10
> 결정: [ADR 021](../adr/021-split-publish-gate-by-domain.md) · 선행: [07](07-publish-gate-split.md)

## 1. 환경과 명령

| 항목 | 값 |
| --- | --- |
| 기준 커밋 | `1aebf53` + Phase 3 작업 트리(도메인 Canary 3개와 실험) |
| dbt | dbt-core 1.12.3, dbt-duckdb 1.11.0 |
| DuckDB | 1.5.5 |
| 저장소 | Docker PostgreSQL, SeaweedFS |

```bash
RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 RUN_DBT_PUBLISH_INTEGRATION=1 \
  uv run pytest tests/reliability/test_domain_gate_experiment.py
```

## 2. 실험 절차

1. 최신 주문 Batch 하나를 뺀 경계 `2026-10-19 11:40:08.083967+00:00`로 Baseline을 `--full-refresh` 게시한다.
2. 각 시나리오의 스위치 끔·켬은 Baseline 사본에서 각각 시작하고, 운영과 같은 증분 `dbt build`로 최신 Bronze까지 처리한다.
3. 기준 파일은 Baseline에서 Canary 없이 증분 게시를 한 번 실행한 결과다.
4. Hash는 도메인 표의 13개 Relation을 전체 컬럼으로 비교한다. D는 기존 `MART_HASH_TARGETS` 9개의 Logical Hash도 함께 비교한다.

"묶인 행"은 되돌리기 전 Build(끔은 실패 격리 파일, 켬은 `failed/` 복사본)의 행 수에서 게시본 행 수를 뺀 값이다.

## 3. 측정값

| 구분 | 값 |
| --- | --- |
| Baseline `fct_order` | 99,824행 |
| Baseline `fct_subscription_payment` | 7행 |
| Baseline 뒤 내용이 바뀐 Relation | `dim_customer`, `fct_order`, `fct_order_item`, `fct_order_payment`, `rpt_customer_order_activity_daily`, `rpt_membership_tier_performance` |
| Baseline 뒤 내용이 바뀐 `subscriptions` Relation | 없음 |

| 시나리오 | 실패시킨 Canary | 끔 상태 | 켬 상태 | 켬 `held_domains` | 켬 판정 |
| --- | --- | --- | --- | --- | --- |
| A | `subscriptions_publish_gate_canary` | `FAILED` (`DBT_TEST_ERROR`) | `PUBLISHED` | `["subscriptions"]` | `PUBLISH_PARTIAL:subscriptions` |
| B | `orders_publish_gate_canary` | `FAILED` | `PUBLISHED` | `["orders"]` | `PUBLISH_PARTIAL:orders` |
| C | `publish_gate_canary` (의존 없음) | `FAILED` | `FAILED` (`DBT_TEST_ERROR`) | `[]` | 미측정 |
| E | `dim_subscription_publish_gate_canary` | `FAILED` | `PUBLISHED` | `["subscriptions"]` | 미측정 |

| 시나리오 | 측정 항목 | 끔 | 켬 |
| --- | --- | --- | --- |
| A | 묶인 `fct_order` 행 | 5 | 0 |
| A | 주문 시각 지연 | 600초 | 0초 |
| B | 묶인 `fct_subscription_payment` 행 | 0 | 0 |
| B | 묶인 `fct_order` 행 / 주문 시각 지연 | 미측정 | 5 / 600초 |
| E | 묶인 `fct_order` 행 | 5 | 0 |

`skipped` Node는 A가 5개, E가 28개다.

| 복구 검증 | 시작 상태 | 결과 | `held_domains` | 13개 Relation 불일치 |
| --- | --- | --- | --- | --- |
| D | A의 부분 게시본 | `PUBLISHED` | `[]` | 없음 |
| D-orders | B의 부분 게시본 | `PUBLISHED` | `[]` | 없음 |

| `build_warehouse` 소요 | 초 |
| --- | --- |
| 정상 증분 실행 (기준 파일) | 37.60 |
| A 부분 게시 (dbt + 되돌리기 + 재검증) | 38.81 |
| D 복구 실행 | 32.66 |
| D-orders 복구 실행 | 34.64 |

## 4. 해석

- **구독 Test 실패가 주문을 묶지 않는다.** 스위치를 끈 전체 Gate에서는 구독 Canary 하나가 주문 5행(주문 시각 600초분)을 묶었다. 켜면 0행이다(A). `dim_subscription`에 붙은 Test가 실패해 하위 28개 Node가 건너뛰어진 경우에도 같다(E).
- **보류된 도메인은 여전히 묶인다.** B에서 스위치를 켜도 주문 5행은 게시되지 않았다. 도메인 분리는 실패를 없애는 것이 아니라 실패의 범위를 그 도메인으로 좁힌다.
- **귀속할 수 없으면 전체 Gate 그대로다.** 의존이 없는 Canary(C)는 스위치와 무관하게 `FAILED`다.
- **되돌린 증분은 다음 실행에서 복구된다.** 부분 게시본에서 Canary 없이 다시 실행하면 13개 Relation이 한 번에 게시한 기준 파일과 모두 같다(D, D-orders). 재계산 경계가 전진하지 않아 밀린 Key를 다시 계산하기 때문이다.
- **추가 비용은 작다.** 부분 게시 1회(되돌리기와 재검증 포함)는 38.81초로 정상 증분 실행 37.60초보다 1.21초 길었다. 설계가 정한 전환 기준(정상 실행의 2배)에 한참 못 미친다. 각 1회 측정이다.
- **대가는 시점 불일치다.** 부분 게시본에서 보류 도메인은 직전 게시 시점, 나머지와 공유 Dimension은 최신 시점이다. 이 실험의 A에서는 `subscriptions`에 변화가 없어 값 차이로 드러나지 않았다.

## 5. 측정 범위의 한계

- 이 Bronze의 마지막 Batch에는 구독·구독 결제 변화가 없다. 그래서 B의 구독 결제 묶임이 끔·켬 모두 0이고, D는 `subscriptions`의 증분 복구를 검증하지 못한다. 되돌린 증분이 복구되는지는 실제 변화가 있는 `orders`를 보류한 D-orders로 확인했다.
- 5행·600초는 Bronze Batch 하나(10분 간격)를 경계로 잡은 실험 값이다.
- 소요 시간은 각 1회 측정값이다.
- 경계 Test 재검증 실패(전체 차단)는 합성 파일 단위 테스트(`tests/test_warehouse_partial_publish.py`, `tests/test_warehouse_domain_gate.py`)로 검증했고 이 실험에는 없다.
