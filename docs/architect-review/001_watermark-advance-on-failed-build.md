# 001. 실패한 Build에서 Watermark가 전진하는 문제

- 판정: **수정 필요 (1건)**
- 대상: `P6-10` / `P6-22`, 브랜치 `feature/phase6-remaining`
- 제기: reviewer
- 판정일: 2026-09-18
- 설계 문서: `docs/architecture/06-late-arrival-affected-keys-and-incremental.md` 6장 (D-4)

## 1. 지적 내용

`advance_processed_batch_watermark()` (`dbt/macros/processed_batch_watermark.sql:37-65`) 는 `selected_resources`로 네 Fact가 **선택**됐는지만 확인한다. 실제로 **성공**했는지는 확인하지 않는다. dbt는 Model이 실패해도 `on-run-end` Hook을 실행하므로, 일부 Fact가 실패한 Build도 Watermark를 전진시킨다. 실패한 Fact의 미반영 범위는 다음 Build의 재계산 대상에서 빠진다.

## 2. 판단: 타당하다

설계 문서 6장은 "Build가 중간에 실패하면 Watermark는 전진하지 않는다"를 안전성 근거로 명시한다. 현재 구현은 그 문장을 강제하지 못한다.

dbt-core 1.12.3 소스로 확인한 사실이다.

- `dbt/task/run.py:1321` `after_run()`은 Model 실패 여부와 무관하게 `safe_run_hooks(adapter, RunHookType.End, extras)`를 호출한다. `on-run-end`는 실패한 Build에서도 실행된다.
- 같은 함수가 `extras`에 `results`를 넣는다. 즉 `on-run-end` Context에서 Node별 실행 결과를 읽을 수 있다. 판정 근거가 이미 Context 안에 있다.
- 상태 값은 `dbt/artifacts/schemas/results.py:58` `NodeStatus`다. 차단 대상은 `error`, `fail`, `runtime error`, `skipped`이고, `warn`은 Build를 중단시키지 않으므로 차단 대상이 아니다.

설계 원칙 2번(누락보다 과다 포함)에 비추어도 결론은 같다. Watermark를 전진시키지 않으면 다음 Build가 상위 집합을 재계산한다. 재계산은 멱등이므로 비용만 늘고 정확성은 유지된다. 반대로 조용한 누락은 복구되지 않는다.

## 3. 채택 방향: `results` 상태 Guard

제시된 세 갈래 중 `results` 상태 확인을 채택한다.

- **`--fail-fast` 강제**: 채택하지 않는다. `--fail-fast`는 실패 시점을 앞당길 뿐이고, `on-run-end`는 그대로 실행된다. 문제를 해결하지 못한다.
- **운영 절차 문서화**: 채택하지 않는다. 사람의 절차로 조용한 누락을 막는 설계는 검증할 수 없다.
- **`results` 상태 Guard**: 채택한다. 판정에 필요한 정보가 Hook Context 안에 있고, Test로 고정할 수 있다.

기존 `selected_resources` 확인은 남긴다. 두 Guard는 서로 다른 것을 막는다.

- Guard 1 (선택 범위): 부분 선택 Build가 보지 않은 Fact의 범위를 전진시키는 것을 막는다.
- Guard 2 (실행 결과): 선택은 됐지만 실패하거나 Skip된 Node가 있는 Build를 막는다.

## 4. developer 수정 지시

### 4.1 `dbt/macros/processed_batch_watermark.sql`

`advance_processed_batch_watermark()`의 `{%- else -%}` 분기 안, `selects` 조립 앞에 결과 Guard를 넣는다. 차단 Node가 있으면 `log`만 남기고 전진하지 않는다.

```jinja
{%- set blocking_statuses = ['error', 'fail', 'runtime error', 'skipped'] -%}
{%- set blocking_nodes = [] -%}
{%- for result in results -%}
    {%- if result.status in blocking_statuses -%}
        {%- do blocking_nodes.append(result.node.unique_id ~ '=' ~ result.status) -%}
    {%- endif -%}
{%- endfor -%}
{%- if blocking_nodes | length > 0 -%}
    {%- do log("Skipping watermark advance: build had failures " ~ blocking_nodes, info=true) -%}
{%- else -%}
    ... 기존 selects 조립과 insert ...
{%- endif -%}
```

- `result.status`는 `str` 계열 Enum이다. `in` 비교는 문자열 값과 일치한다. `| string` 필터를 쓰지 않는다. Python 버전에 따라 `NodeStatus.Error` 형태가 나올 수 있다.
- `warn`을 차단 목록에 넣지 않는다.

### 4.2 Test

**Unit (`tests/test_dbt_watermark_macro.py`)**: 매크로 원문에 네 차단 상태와 `result.status` 확인이 있고, `warn`이 차단 목록에 없다는 것을 고정한다. Text 검증이므로 4.3의 통합 Test와 함께 쓴다.

**Integration (`tests/integration/test_subscription_payment_temporal_join_integration.py`)**: `test_watermark_does_not_advance_when_a_model_fails`를 추가한다.

1. 기존 Fixture로 첫 Build를 성공시키고 `control.dbt_processed_batch`의 `max(processed_batch_id)`를 읽어 둔다.
2. 새 Batch를 수집해 `_append_fixture_catalog`로 등록한다.
3. 그 새 행의 `object_key`를 존재하지 않는 Key로 바꿔 `stg_subscription_payments` 읽기를 실패시킨다.
4. Build를 실행하고 `returncode != 0`을 확인한다.
5. `max(processed_batch_id)`가 1번에서 읽은 값과 같은지 확인한다.
6. 이어서 Catalog를 정상 Key로 되돌리고 다시 Build해서, 성공 후 Watermark가 전진하고 누락된 결제가 Fact에 들어오는지 확인한다. 상위 집합 재처리가 실제로 복구하는지 보는 부분이다.

### 4.3 문서

설계 문서 6장에 두 Guard를 이미 반영했다. developer는 문서를 고치지 않는다.

## 5. 남은 검증

Docker/SeaweedFS 미기동으로 dbt 증분 Build와 통합 E2E 4건(late subscription payment, contract rebind, multi-batch, 기존 late order AC-11)이 미실행이다. 4.2의 새 Test를 포함해 컨테이너 기동 후 재실행한다. 재실행 전에는 `P6-10` / `P6-22` Checkbox를 갱신하지 않는다.
