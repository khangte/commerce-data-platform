# 046. Mart Build Session TimeZone 고정 (UTC)

- 날짜: 2026-09-23
- 판정: 수정 필요. 045 적용 Publish(`742206ef`)는 계약 기준에 맞지 않는다. TimeZone 고정 후 재Publish한다
- 계기: 045 §7 중단 기준 발동. Full Refresh Publish `742206ef`의 `changed_relations`가 `fct_order_item` 외에 `dim_customer`, `dim_date`, `fct_order`를 포함함. 직전 Publish `5d74a326` 이후 신규 Bronze Object 0건
- 관련: 045 (`fct_order_item.purchase_date_key`), Mart Grain 계약 §1 Surrogate Key 결정성

## 1. 조사

기준 데이터는 실패 Build `8224d2a7`이다. 이 파일은 `5d74a326` Published 파일의 복사본이다. dbt가 SeaweedFS 연결 오류로 Mart를 바꾸기 전에 실패했다. 이 파일의 Mart Hash는 `5d74a326` 기록 Hash와 모두 일치한다. `742206ef` Published 파일과 비교한 결과:

| Relation | 행 수 (전/후) | 차이 |
| -------- | ------------- | ---- |
| `dim_date` | 774 / 774 | 범위가 `20160904–20181017`에서 `20160905–20181018`로 하루 밀림 |
| `dim_customer` | 96096 / 96096 | `valid_from` 값은 같음. `customer_key` 96096건 전부 다름 |
| `fct_order` | 99441 / 99441 | `customer_key` 99441건, `purchase_date_key` 53379건, `carrier_handoff_days`·`delivery_days`·`delivery_delay_days` 수만 건 다름 |
| `fct_order_item` | 112650 / 112650 | 기존 컬럼 차이 0건. `sum(item_price)` = 13591643.70으로 같음. 새 컬럼만 추가됨 |

`dim_subscription`, `fct_subscription_payment`는 0행이라 차이가 드러나지 않았다.

## 2. 원인

Full Refresh와 Incremental의 불일치가 아니다. DuckDB Session TimeZone이 고정되지 않은 것이 원인이다.

- `dbt/profiles.yml`에 `TimeZone` 설정이 없다. DuckDB는 Process의 Local TimeZone을 쓴다. Host는 `Asia/Seoul`이다. `5d74a326`은 UTC 환경에서 Build됐고, `742206ef`는 Host(KST)에서 Build됐다.
- `purchase_at`, `valid_from` 등은 `TIMESTAMPTZ`다. `date_trunc('day', ...)`는 Session TimeZone 기준으로 날짜를 자른다. 그래서 `purchase_date_key`와 `dim_date` 범위가 하루씩 밀렸다. 배송 일수의 `date_diff('day', ...)`도 날짜 경계가 바뀌어 달라졌다.
- `customer_key = md5(customer_id | cast(valid_from as varchar) | attribute_hash)`다. `cast(TIMESTAMPTZ as varchar)`는 Session TimeZone Offset(`+00`/`+09`)을 문자열에 넣는다. 같은 시각이어도 Key가 바뀐다. `dim_subscription.subscription_key`도 같은 식이라 같은 결함이 있다.

같은 입력에서 같은 Key가 나온다는 계약 §1 가정이 실행 환경에 따라 깨진다. Airflow(UTC)와 Host 수동 실행(KST)이 번갈아 Publish하면 Fact의 FK가 매번 바뀐다.

## 3. 판정

1. §7 중단은 올바른 판단이었다.
2. `742206ef`는 KST 날짜 기준이라 UTC 계약과 맞지 않는다. Rollback하지 않고 아래 수정 후 재Publish로 덮는다. 둘 다 Publish Pipeline을 거치고, 재Publish가 더 적은 절차로 올바른 상태를 만든다.
3. 계약에 규칙을 추가했다(`docs/reference/mart-grain.md` §1): 모든 Mart Build는 Session TimeZone을 `UTC`로 고정하고, 모든 `*_date_key`는 UTC 기준 날짜다.

## 4. 수정 지시

1. `dbt/profiles.yml` `settings`에 `TimeZone: 'UTC'`를 추가한다.
2. `profiles.yml`에 `TimeZone: UTC`가 있는지 확인하는 pytest 단언을 추가한다. `tests/test_warehouse_quality_contract.py` 또는 profile 설정을 검사하는 기존 테스트 파일에 둔다.
3. Key 식(`cast(valid_from as varchar)`)은 바꾸지 않는다. TimeZone을 고정하면 결정적이다. 식을 바꾸면 모든 기존 Key가 한 번 더 바뀐다.
4. `python -m src.warehouse.publish --full-refresh --pipeline-name contract_046_utc_republish`로 재Publish한다.

## 5. 검증 기준

재Publish 결과의 Mart Hash를 `5d74a326`(UTC Build)의 기록 Hash와 비교한다. `742206ef`와 비교하지 않는다.

- `5d74a326` 대비 Hash가 다른 Relation은 `facts.fct_order_item` 하나여야 한다.
- `dim_date` 범위는 `20160904–20181017`이다.
- `fct_order_item` 행 수 112650, `sum(item_price)` 13591643.70.
- `fct_order_item_date_matches_order`를 포함한 dbt test가 모두 통과한다.
- `RUN_PUBLISHED_MART_CHECK=1`로 `tests/test_published_mart_queryable.py`가 통과한다.

`5d74a326` 대비 다른 Relation이 또 바뀌면 중단하고 보고한다.

## 6. 남는 위험

`5d74a326`이 정말 UTC에서 Build됐는지는 날짜 범위로 추정했다. 재Publish 결과가 `5d74a326` Hash와 일치하면 이 추정이 확인된다.
