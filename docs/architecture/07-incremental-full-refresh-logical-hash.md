# Incremental과 Full Refresh의 Mart Logical Hash 비교

> 대상 Task: `P6-23`
> 브랜치: `feature/phase6-remaining`
> 상태: lead 승인 대기 (2026-09-18)
> 선행 설계: `docs/architecture/06-late-arrival-affected-keys-and-incremental.md`

## 1. 문제

Phase 6의 완료 조건은 "Incremental 결과는 동일 입력의 Full Refresh와 Logical Hash가 같아야 한다"다 (PRD 14.4, `docs/phases/phase-06-dimensional-modeling.md`). 지금은 이 문장을 검증하는 수단이 없다. Mart의 Logical Hash를 계산하는 코드가 없고, 두 Build 결과를 같은 입력 위에서 비교하는 절차도 없다.

`P6-22`가 Fact를 영향 Key 기반 부분 재계산으로 바꿨기 때문에 이 검증은 선택이 아니다. 부분 재계산은 다음 세 가지로 조용히 틀어질 수 있고, 셋 다 Model Test로는 잡히지 않는다.

1. **삭제 잔존**: 원천에서 사라진 자식 행이 Fact에 남는다. Grain Test는 중복만 보므로 통과한다.
2. **시점 결합 재배열**: 지연 관측이 SCD2 구간을 재배열했는데 그 구간과 겹치는 사건이 영향 Key에서 빠진다. 참조 무결성 Test는 통과한다. 이전 Version Key가 여전히 실재하기 때문이다.
3. **경계 누락**: Watermark 경계 밖 Batch의 변경이 재계산 대상에서 빠진다.

셋 다 "Full Refresh와 값이 다르다"로만 드러난다. 그래서 Hash 비교가 이 세 결함의 유일한 Gate다.

## 2. 원칙

- Hash는 **논리 내용**만 담는다. 물리적 행 순서, 저장 순서, Build 시각, `invocation_id`는 담지 않는다.
- Hash가 다르면 **어디가 다른지**까지 알려준다. 같다/다르다만 알려주는 비교는 고치는 데 쓸 수 없다.
- 비교 절차는 **같은 Bronze 입력** 위에서 돈다. 입력이 다르면 Hash 차이는 결함의 증거가 아니다.
- 새 Mart가 비교 대상에서 조용히 빠지지 않는다. 목록 누락을 Test로 막는다.

---

## 3. D-1. Hash 대상은 `table`로 물리화된 Mart뿐이다

### 결정

Hash 대상은 `dimensions` 5개와 `facts` 4개, 합계 9개다.

| Schema | Model | 정렬 Key |
| ------ | ----- | -------- |
| `dimensions` | `dim_customer` | `customer_key` |
| `dimensions` | `dim_date` | `date_key` |
| `dimensions` | `dim_product` | `product_id` |
| `dimensions` | `dim_seller` | `seller_id` |
| `dimensions` | `dim_subscription` | `subscription_key` |
| `facts` | `fact_orders` | `order_id` |
| `facts` | `fact_order_items` | `order_id`, `order_item_id` |
| `facts` | `fact_payments` | `order_id`, `payment_sequence` |
| `facts` | `fact_subscription_payments` | `payment_id` |

`metrics` Schema의 Report Model 3개는 제외한다. `view`이므로 저장된 상태가 없고 조회 시점에 Mart 위에서 다시 계산된다. Incremental과 Full Refresh라는 구분 자체가 성립하지 않는다. 아홉 개 Mart가 같으면 그 위의 View도 같다.

`staging`과 `intermediate`도 같은 이유로 제외한다.

### 정렬 Key는 실제 컬럼 이름을 쓴다

`dim_product`와 `dim_seller`는 계약 문서가 `product_key` / `seller_key`를 Primary Key로 적지만 Model이 내보내는 컬럼은 `product_id` / `seller_id`다. Hash 정렬 Key는 실재하는 컬럼을 써야 하므로 위 표는 Model 기준이다. 계약 문서 쪽 불일치는 6장에 별건으로 정리한다.

### 목록 누락 방지

`dbt/models/marts/dimensions/*.sql`와 `dbt/models/marts/facts/*.sql`의 파일 집합이 Hash 대상 목록과 정확히 일치하는지 Test로 강제한다. Mart를 추가하고 목록에 넣지 않으면 그 Mart는 영원히 비교되지 않는다. 이 실패는 조용하므로 Test로만 막을 수 있다.

---

## 4. D-2. Hash는 Key 정렬 행의 Canonical JSON 누적 SHA-256이다

### 결정

Model 하나의 Logical Hash를 다음으로 정의한다.

1. Model의 모든 컬럼을 정렬 Key 오름차순으로 읽는다.
2. 행마다 `{컬럼명: 값}` Dict를 만들고 컬럼명 기준으로 정렬한 Canonical JSON 문자열로 만든다.
3. 각 행의 JSON을 UTF-8로 SHA-256에 누적하고 행 사이에 `\n`을 넣는다.
4. 최종 Hex Digest가 그 Model의 Logical Hash다.

이 형태는 Bronze의 `table_logical_hash` (`src/ingestion/bronze.py:266`)와 같다. 같은 프로젝트 안에서 "Logical Hash"라는 말이 두 가지를 뜻하지 않게 하려는 것이다.

### 값 표현 규칙

Canonical JSON은 다음을 고정한다. 이 규칙이 흔들리면 같은 데이터가 다른 Hash를 낸다.

- `timestamptz`는 UTC로 변환한 ISO 8601 문자열. `tzinfo`가 없는 값은 오류로 막는다. Hash가 실행 환경의 Local Timezone에 의존하게 되기 때문이다.
- `date`는 ISO 8601 날짜 문자열.
- `Decimal`은 지수 표기 없는 고정 소수점 문자열.
- `UUID`는 소문자 Hex 문자열.
- `None`은 JSON `null`. 빈 문자열과 구분된다.
- Dict Key는 항상 정렬한다. `ensure_ascii=False`, 구분자는 `(",", ":")`.

`float`은 Python 기본 표현(round-trip 가능한 `repr`)을 쓴다. 자리수를 줄이는 반올림을 넣지 않는다. 반올림은 실제 값 차이를 감추는 쪽으로 작동한다.

### 왜 컬럼 전량인가

Surrogate Key를 포함한 모든 컬럼을 넣는다. Surrogate Key는 `md5(business_key|valid_from|attribute_hash)`로 결정적이므로 Full Refresh에서도 같은 값이 나온다. 결정적이지 않다면 그 자체가 결함이고, Hash 비교가 그것을 잡아야 한다.

### 왜 SQL `md5(concat(...))`가 아닌가

DuckDB 안에서 `md5(concat(...))`로 계산하는 방법은 쓰지 않는다.

- `concat`은 `NULL`을 빈 문자열로 흡수한다. `NULL`과 `''`이 같은 Hash를 낸다.
- 숫자·Timestamp의 문자열 표현이 타입과 설정에 따라 달라진다.
- 컬럼을 하나 추가하면 Hash 식도 손으로 고쳐야 한다. 빠뜨리면 조용히 검증 범위가 줄어든다.

Python 쪽에서 컬럼 목록을 Runtime에 읽어 계산하면 세 문제가 모두 없다.

---

## 5. D-3. 비교는 Warehouse 파일 복사본 위에서 Full Refresh를 돌린다

### 결정

비교 절차를 다음으로 고정한다.

```text
1. 평소대로 Incremental Build를 마친다.            (상태 A)
2. 상태 A의 Mart Logical Hash 9개를 계산한다.
3. warehouse.duckdb 파일을 복사한다.               (상태 B)
4. 상태 B에서 dbt build --full-refresh를 실행한다.
5. 상태 B의 Mart Logical Hash 9개를 계산한다.
6. 9개를 Model 이름으로 대응시켜 비교한다.
```

### 왜 복사본인가

Full Refresh를 원본에 실행하면 비교 대상인 상태 A가 사라진다. Hash를 미리 계산해 두면 값은 남지만, 불일치가 났을 때 어느 행이 다른지 볼 원본이 없다. 진단 없는 Gate는 고치는 데 쓸 수 없다.

DuckDB Warehouse는 단일 파일이므로 복사가 곧 상태 복제다. `control.bronze_files` Catalog도 함께 복사되므로 **두 Build의 입력이 Byte 단위로 같다**는 조건이 자동으로 충족된다. 이 조건이 비교의 전제다.

### Full Refresh Build에서 영향 Key와 Watermark

`--full-refresh`에서는 `is_incremental()`이 거짓이므로 Fact의 영향 Key 필터가 통째로 빠지고 전량이 다시 만들어진다. 이것이 비교의 의도다.

복사본에서도 `on-run-end`가 Watermark를 전진시키고 `control.affected_keys`에 감사 행을 남긴다. 복사본은 비교 후 버리므로 원본 상태에 영향이 없다. `control` Table은 Hash 대상이 아니므로 비교 결과도 오염시키지 않는다.

### 비교가 의미를 가지려면 지연 도착이 있어야 한다

Batch 하나만 넣고 비교하면 Incremental Build가 사실상 최초 생성이므로 두 결과가 같은 것이 당연하다. 1장의 결함 세 가지는 **두 번째 Build**에서만 드러난다. 그래서 검증은 반드시 지연 도착을 포함한 2회 이상의 Build 뒤에 수행한다.

---

## 6. D-4. 불일치는 Model·Key 단위로 좁혀서 보고한다

### 결정

Hash가 다른 Model에 대해 다음을 계산해 보고한다.

- 양쪽 행 수
- 한쪽에만 있는 Key (양방향, 각각 최대 20개)
- 양쪽에 다 있지만 행 내용이 다른 Key (최대 20개)와 그 Key의 컬럼별 값 차이

이 세 가지가 1장의 결함 세 가지에 그대로 대응한다. 한쪽에만 있는 Key는 삭제 잔존이나 경계 누락이고, 내용이 다른 Key는 시점 결합 재배열이다.

상한을 20개로 두는 이유는 실패 출력이 수천 줄이 되면 아무도 읽지 않기 때문이다. 상한은 진단용이고 판정은 Hash가 한다.

---

## 7. D-5. 계약 문서 불일치는 별건으로 분리한다

조사 중 확인한 `docs/reference/mart-grain.md`와 구현의 불일치다. lead가 이번 브랜치 범위 밖으로 보류 결정했다. 별도 이슈로만 기록하고 Phase 6에서는 다루지 않는다. 기록은 `docs/architect-review/002_mart-grain-contract-drift.md`에 있다.

| 항목 | 계약 문서 | 구현 |
| ---- | --------- | ---- |
| `dim_product` Primary Key | `product_key` | `product_id` 컬럼만 존재 |
| `dim_seller` Primary Key | `seller_key` | `seller_id` 컬럼만 존재 |
| `dim_date` Business Key | `full_date` | `calendar_date` |
| Fact Model 이름 | `fct_order`, `fct_order_item`, `fct_order_payment`, `fct_subscription_payment` | `fact_orders`, `fact_order_items`, `fact_payments`, `fact_subscription_payments` |
| `fct_order_payment` Unique Key | `payment_sequential` | `payment_sequence` |

또한 `dim_product`, `dim_seller`, `dim_date`에는 `dbt/models/marts/dimensions/schema.yml`의 Test가 없다. Primary Key의 `not_null`·`unique`가 강제되지 않는다.

---

## 8. 변경 대상

| 경로 | 변경 |
| ---- | ---- |
| `src/warehouse/__init__.py` | 신규. Warehouse 검증 도구 Package |
| `src/warehouse/mart_hash.py` | 신규. Hash 대상 목록, Model별 Hash 계산, 불일치 진단, CLI |
| `tests/test_mart_hash.py` | 신규. Canonical 표현과 정렬 무관성, 목록 누락 방지 |
| `tests/integration/test_incremental_full_refresh_hash_integration.py` | 신규. 지연 도착 뒤 Incremental과 Full Refresh 비교 |

`dbt/` 아래는 바꾸지 않는다. Hash 비교는 Build 산출물을 읽기만 하는 검증이고, Build 자체의 동작을 바꾸지 않는다.

## 9. 검증

| 항목 | 방법 |
| ---- | ---- |
| Canonical 표현 고정 | Unit. Timestamp/Decimal/UUID/None의 JSON 표현을 값으로 고정 |
| 물리 순서 무관성 | Unit. 같은 행을 다른 삽입 순서로 넣은 두 Table의 Hash가 같다 |
| 실제 차이 검출 | Unit. 값 하나만 다른 Table의 Hash가 다르다 |
| 목록 누락 방지 | Unit. Mart SQL 파일 집합 == Hash 대상 목록 |
| 진단 정확도 | Unit. 한쪽에만 있는 Key와 값이 다른 Key를 각각 찾아낸다 |
| Incremental == Full Refresh | Integration. 지연 도착 2 Batch 뒤 9개 Model Hash가 모두 같다 |

## 10. 구현 순서

1. `src/warehouse/mart_hash.py`의 Hash 계산과 Unit Test
2. 대상 목록 누락 방지 Test
3. 불일치 진단과 Unit Test
4. CLI
5. 통합 비교 Test
