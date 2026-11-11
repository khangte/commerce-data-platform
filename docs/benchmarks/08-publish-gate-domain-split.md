# 08. Publish Gate 도메인 분리 — Test 실패가 묶는 도메인

> 증적: `data/reliability/gate-domain-split/20261110T131419718338Z.json` (Git 미추적)
> 실험: `tests/reliability/test_domain_gate_experiment.py` (실제 PostgreSQL·SeaweedFS·dbt, 563.20초)
> 측정일: 2026-11-10 (F·G와 C·E 판정은 2026-11-11 재실행, [6절](#6-미검증-3건-재현-2026-11-11))
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
| C | `publish_gate_canary` (의존 없음) | `FAILED` | `FAILED` (`DBT_TEST_ERROR`) | `[]` | `PUBLISH_FAILED` (11-11 재실행) |
| E | `dim_subscription_publish_gate_canary` | `FAILED` | `PUBLISHED` | `["subscriptions"]` | `PUBLISH_PARTIAL:subscriptions` (11-11 재실행) |

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
- 경계 Test 재검증 실패(전체 차단)는 처음에 합성 파일 단위 테스트(`tests/test_warehouse_partial_publish.py`, `tests/test_warehouse_domain_gate.py`)로만 검증했다. 2026-11-11 재실행의 G에서 실제 dbt Build로 확인했다(6절).

## 6. 미검증 3건 재현 (2026-11-11)

> 증적: `data/reliability/gate-domain-split/20261111T094058794989Z.json` (Git 미추적), 발췌·명령은 [evidence/publish-gate-domain-boundary/](evidence/publish-gate-domain-boundary/README.md)
> 실험: 같은 `tests/reliability/test_domain_gate_experiment.py`에 F·G를 더해 A–G를 한 번에 실행 (663.94초, 1 passed)
> 기준 커밋: `93f3c7a` + Phase 2 작업 트리

경계는 `2026-10-19 11:50:09.506602+00:00`로 2026-11-10보다 Batch 하나 뒤다. 그 사이 Bronze가 늘어 A·B·E의 수치가 바뀌었다(묶인 `fct_order` 5행·600초 → 20행·1,967,174초, B 끔의 묶인 구독 결제 0 → 2행). 판정과 `held_domains`는 2026-11-10과 같다.

| 시나리오 | 조건 | 켬 상태 | 켬 `held_domains` | 켬 판정 | 비고 |
| --- | --- | --- | --- | --- | --- |
| C | `publish_gate_canary` | `FAILED` (`DBT_TEST_ERROR`) | `[]` | `PUBLISH_FAILED` | 2026-11-10 미측정분 |
| E | `dim_subscription_publish_gate_canary` | `PUBLISHED` | `["subscriptions"]` | `PUBLISH_PARTIAL:subscriptions` | 2026-11-10 미측정분, `skipped` 28개 |
| F | `orders_publish_gate_canary` + `subscriptions_publish_gate_canary` | `FAILED` (`DBT_TEST_ERROR`) | `[]` | `PUBLISH_FAILED` | 끔도 `FAILED`, 13개 Relation Hash Baseline과 같음 |
| G | 구독 Canary + 소급 고객 관측 주입 | `FAILED` (`DBT_TEST_ERROR`) | `[]` | `PUBLISH_FAILED` | 경계 Test 실패로 전체 차단 |
| G 대조 | 소급 고객 관측 주입만 | `PUBLISHED` | `[]` | 없음 | 정상 경로는 결제를 새 Key로 옮긴다 |
| H | Airflow `warehouse_pipeline_dag`, `subscriptions_publish_gate_canary` 강제 | `PUBLISHED` (`dbt_build` Task 성공) | `["subscriptions"]` | `PUBLISH_PARTIAL:subscriptions`, `ok=false` | `fct_order` 99849 → 99869, [evidence 02](evidence/publish-gate-domain-boundary/02-airflow-subscriptions-held.md) |

**F. 두 도메인 동시 실패.** 실패 Test가 `orders`와 `subscriptions`에 하나씩 있어 한 도메인으로 귀속되지 않는다. 스위치를 켜도 되돌리기를 시도하지 않고(오류 요약에 `partial publish fallback` 없음) v1과 같이 전체 차단한다.

**G. 실데이터 경계 실패.** 실제 Cursor 수집에서는 `customer_key`가 바뀌지 않는다. Key는 `md5(customer_id | valid_from | attribute_hash)`이다. 수집은 Watermark 이후 관측만 가져와 관측이 시간순으로 뒤에만 붙고, 등급은 내려가지 않으므로(`dim_customer_membership_tier_non_decreasing`) 기존 Version의 `valid_from`과 Hash가 그대로다. 그래서 `tests/reliability/faults.py`의 `backdated_tier_observation`으로 조건을 주입했다.

- 주입: 구독 결제가 참조하는 첫 고객 Version(고객 `24379aeb…`, `BRONZE`)과 같은 등급에, 생성·수정 시각을 하루 앞당긴 관측 1건을 별도 Prefix(`reliability-faults/…`)의 Parquet으로 올렸다. 그리고 이번 Build 파일의 `control.bronze_files`에만 추가했다. PostgreSQL Metadata는 건드리지 않았고 Object는 실험 끝에 지웠다.
- 결과: 첫 Version의 `valid_from`이 바뀌어 Key `b16e4b48…`가 새 `dim_customer`에서 사라진다. Baseline에서는 구독 결제 3행이 이 Key를 참조했다.
  - 대조(보류 없음): 영향 Key 재계산이 결제를 새 Key로 옮겨 `PUBLISHED`다.
  - 구독 보류: 되돌린 결제가 옛 Key를 가리킨다. 그래서 `relationships_fct_subscription_payment_customer_key__customer_key__ref_dim_customer_`가 재검증에서 실패한다. 이어 오류 요약 첫 줄 `DBT_TEST_ERROR: partial publish fallback: subscriptions: boundary tests failed: …`로 전체 차단했다.
  - `failed/`의 되돌리기 전 사본에는 옛 Key가 없고, 결제도 옮겨져 있다. Build 파일은 지워졌고 게시본 Hash는 Baseline과 같다.
- 주입은 합성 조건이다. dbt Build·되돌리기·재검증은 실제 경로다. 운영에서 이 조건이 생기려면 Watermark 아래로 소급된 관측이 Bronze에 들어와야 한다(재적재·수동 Backfill).

**H. Airflow에서 `subscriptions`만 보류.** DAG에는 dbt var를 넘기는 Param이 없다. 그래서 `subscriptions_publish_gate_canary.sql`의 var 기본값을 잠시 `true`로 바꾸고 `source_simulation_dag`로 원천 변경을 만들었다. `manual__2026-11-11T09:40:00+00:00`의 결과는 다음과 같다.

- `dbt_build` Task 성공. 실패 Node는 구독 Canary 하나이고 로그는 `held_domains=['subscriptions']`다.
- `mart_publish_runs`: `PUBLISHED`, `held_domains=["subscriptions"]`, `tests_failed=1`
- 주문 Fact는 최신이다. `changed_relations`에 `fct_order`·`fct_order_item`·`fct_order_payment`·`dim_customer`가 들어 있고, `fct_order`는 99849 → 99869가 됐다.
- 구독 결제는 9행 그대로다.
- Run report: `ok=false`, `PUBLISH_PARTIAL:subscriptions`
- `REPOINT_FAILED`는 Metabase를 띄우지 않아 생긴 것으로, 이 판정과 무관하다.
- Canary SQL은 `git checkout`으로 복원했다.
