# Mart Grain 계약

> 상태: Reference
> 기준 문서: [PRD v1.9](../../PRD_v1.9.md), [데이터 변환 흐름](data-transformation-flow.md), [Phase 6](../phases/phase-06-dimensional-modeling.md)

이 문서는 Warehouse Mart의 Grain, Unique Key, 컬럼, Measure 계약을 정의하는 단일 정본이다. `dbt/models/marts/*/schema.yml`은 이 문서에서 생성한다. 컬럼을 추가하거나 바꿀 때는 이 문서를 먼저 고치고 `schema.yml`을 다시 만든다.

## 1. 작성 규칙

### 1.1 Grain

Grain은 "한 행이 무엇 하나를 나타내는가"의 선언이다. Model마다 Grain 문장을 정의로 삼고, Unique Key는 그 문장을 데이터로 강제하는 수단이다.

컬럼 목록만으로는 중복의 의미를 판정할 수 없다. `(order_id, order_item_id)`가 Unique하다는 사실만으로는 그 Table이 주문 Line을 나타내는지 배송 이벤트를 나타내는지 알 수 없다. 문장이 있어야 새 컬럼이 그 문장을 깨는지 판단할 수 있다.

### 1.2 컬럼 종류

모든 컬럼은 아래 다섯 종류 중 하나다. 종류가 그 컬럼에 적용할 규칙을 정한다.

| 종류         | 의미                            | Test                                                | 비고                                        |
| ------------ | ------------------------------- | --------------------------------------------------- | ------------------------------------------- |
| `PK`         | Grain을 강제하는 Key            | `not_null`, `unique`(단일) 또는 Singular Test(복합) | Grain 문장과 1:1 대응                       |
| `FK`         | Dimension 참조 Key              | `not_null`, `relationships`                         | 대상 Model을 함께 적는다                    |
| `Degenerate` | Dimension 없이 Fact에 남는 속성 | `accepted_values` 등                                | 자체 Dimension을 만들 만큼 속성이 없을 때만 |
| `Measure`    | 집계 대상 수치                  | 범위·부호 Test                                      | 5절 규칙 적용                               |
| `Attribute`  | Dimension의 서술 속성           | 필요 시                                             | Dimension 전용                              |

### 1.3 새 컬럼을 추가할 때

해당 Model의 Grain 문장이 여전히 참인지 먼저 확인한다.

| 증상                          | 의미                                | 조치                                                                    |
| ----------------------------- | ----------------------------------- | ----------------------------------------------------------------------- |
| 한 행에 값이 여러 개 필요하다 | 추가하려는 값의 Grain이 더 세밀하다 | 별도 Model로 분리하거나, Intermediate에서 이 Grain으로 집계한 뒤 넣는다 |
| 여러 행에 같은 값이 반복된다  | 추가하려는 값의 Grain이 더 거칠다   | Dimension으로 분리하고 FK로 연결한다                                    |
| 행 수가 늘어난다              | Grain 문장 자체가 바뀐다            | 새 Model을 만든다. 기존 Model의 Grain을 바꾸지 않는다                   |

### 1.4 1:1 Fact를 따로 만들지 않는다

Grain이 같은 두 Fact는 한 Table이어야 한다. 별도 Model로 쪼개면 같은 Key를 가진 1:1 Fact가 되어 Join 비용만 늘어난다.

### 1.5 Model 작성 양식

각 Model은 아래 양식으로 쓴다.

```markdown
### N.M `<model_name>`

- Grain: 한 행은 <무엇> 하나를 나타낸다.
- Unique Key: `<컬럼>` 또는 `(<컬럼>, <컬럼>)`
- Materialization: `table` | `incremental` | `view`
- 출처: `<상류 Model>`

| 컬럼 | 타입 | 종류 | Null | 정의 / Test |
| ---- | ---- | ---- | ---- | ----------- |
|      |      |      |      |             |
```

- 타입은 DuckDB 타입으로 적는다. 금액은 `decimal(14,2)`를 쓰고 `double`을 쓰지 않는다.
- `Null` 열은 `Y`(허용) 또는 `N`(불가)로 적는다.
- `정의 / Test` 열에는 Measure의 계산식, FK의 참조 대상, 값 목록을 적는다.

## 2. Dimension

| Model | Grain | Unique Key | Materialization |
| ----- | ----- | ---------- | --------------- |
|       |       |            |                 |

### 2.1 `<dim_name>`

- Grain:
- Unique Key:
- Materialization:
- 출처:

| 컬럼 | 타입 | 종류 | Null | 정의 / Test |
| ---- | ---- | ---- | ---- | ----------- |
|      |      |      |      |             |

## 3. Fact

| Model | Grain | Unique Key | Materialization |
| ----- | ----- | ---------- | --------------- |
|       |       |            |                 |

### 3.1 `<fact_name>`

- Grain:
- Unique Key:
- Materialization:
- 출처:

| 컬럼 | 타입 | 종류 | Null | 정의 / Test |
| ---- | ---- | ---- | ---- | ----------- |
|      |      |      |      |             |

## 4. Report

`rpt_*`는 Intermediate가 아닌 Mart 위에서 파생되는 Metrics Model이다. BI가 Source·Bronze·Staging을 직접 조회하지 않도록 Mart만 읽는 소비 계층을 제공한다.

| Model | Grain | Unique Key | Materialization |
| ----- | ----- | ---------- | --------------- |
|       |       |            |                 |

### 4.1 `<rpt_name>`

- Grain:
- Unique Key:
- Materialization:
- 출처:

| 컬럼 | 타입 | 종류 | Null | 정의 / Test |
| ---- | ---- | ---- | ---- | ----------- |
|      |      |      |      |             |

### 4.2 Report Model을 만들 때

- 날짜 축은 필요한 가장 낮은 Grain으로 유지한다. 접힌 Grain은 되돌릴 수 없지만, 펼쳐진 Grain은 BI가 roll-up할 수 있다.
- 단일 Fact 한 개를 단순 `group by`하면 되는 지표는 Report Model로 만들지 않는다. BI가 직접 집계한다.
- Grain이 다른 Fact를 결합하는 Model은 `count(distinct ...)`로 접은 컬럼임을 이름에 드러낸다.

### 4.3 알려진 제약

Grain을 접어서 잃은 분석 축, 이름이 실제 의미보다 넓게 읽히는 컬럼을 여기에 적는다.

## 5. Measure 규칙

Measure는 자신이 속한 Grain에서만 유효하다. 계산식은 각 Model의 컬럼 표에 적고, 이 절에는 Grain을 넘어 적용되는 규칙만 둔다.

### 5.1 계산 위치

Fact는 계산하지 않고 투영한다. 집계와 파생 계산은 Intermediate에서 끝낸다. Fact의 Grain·Key 제공 책임과 집계 책임을 분리하기 위해서다.

### 5.2 Additive 구분

| 구분          | 의미                             | 허용 집계          |
| ------------- | -------------------------------- | ------------------ |
| Additive      | 모든 축으로 합산 가능            | `SUM`              |
| Semi-additive | 일부 축(주로 시간)에서 합산 불가 | 축별로 명시        |
| Non-additive  | 합산 불가                        | 평균·분위수·비율만 |

기간 차이, 비율, Flag는 Non-additive다. 컬럼 표의 `정의` 열에 구분을 함께 적는다.

### 5.3 금액 Measure

- Grain이 다른 Raw 입력을 직접 다대다 Join한 뒤 SUM하지 않는다. 6절 참조.
- 주문 금액과 실제 결제 금액을 같은 컬럼으로 합치지 않는다. 할부·환불·실패로 값이 달라진다.
- 금액 타입은 `decimal(14,2)`를 유지한다. `double`은 Full Refresh와 Incremental 사이에서 합계가 달라진다.

### 5.4 NULL 처리

- 원천 Timestamp가 `NULL`이면 그 Timestamp로 계산한 Measure도 `NULL`이다.
- `NULL`인 행은 평균과 비율의 분모에서 제외한다. `0`으로 바꾸지 않는다.
- Flag Measure는 근거 컬럼이 `NULL`이면 `NULL`이다. `false`로 바꾸지 않는다.

## 6. Fan-out 방지

Grain이 다른 두 Fact를 직접 Join하면 양쪽 행 수가 곱해져 Measure가 부풀려진다. 에러는 나지 않는다.

```sql
-- 잘못된 방식: 왼쪽 3행 × 오른쪽 2행 = 6행이 되어 양쪽 합계가 부풀려진다
SELECT a.key, SUM(b.value), SUM(c.value)
FROM a JOIN b USING (key)
       JOIN c USING (key)
GROUP BY a.key
```

올바른 방식은 각각을 먼저 목표 Grain으로 접은 뒤 1:1로 Join하는 것이다.

```text
<상류 A> → <A 집계 Intermediate> ┐
                                 ├→ <결합 Intermediate> → <Fact>
<상류 B> → <B 집계 Intermediate> ┘
```

Fact끼리는 서로 직접 Join하지 않는다.

## 7. 검증

Grain 계약은 문서가 아니라 Test로 강제한다. 아래 표는 컬럼 표의 `정의 / Test` 열이 어떤 수단으로 구현되는지를 정한다.

| 검증 대상            | 수단                                              |
| -------------------- | ------------------------------------------------- |
| 단일 컬럼 Unique Key | `schema.yml`의 `unique` Data Test                 |
| 복합 Unique Key      | `dbt/tests/<model>_unique.sql` Singular Test      |
| 참조 무결성          | `schema.yml`의 `relationships` 또는 Singular Test |
| 값 목록              | `schema.yml`의 `accepted_values`                  |
| Null 불가            | `schema.yml`의 `not_null`                         |
| 계층 책임 분리       | `tests/test_fact_layer_contract.py`               |

`dbt build`가 전부 통과해야 Publish된다. Grain/Measure Test 실패는 Phase 7 품질 Gate에서 Publish를 차단한다.

## 8. schema.yml 생성 규칙

이 문서의 컬럼 표에서 `dbt/models/marts/*/schema.yml`을 만든다. 생성물에는 Grain 문장을 중복해 적지 않고 이 문서를 가리킨다.

```yaml
models:
  - name: <model_name>
    description: "Grain: docs/reference/mart-grain.md#<anchor>"
    columns:
      - name: <컬럼>
        data_tests: [...]
```

| 문서                  | schema.yml         |
| --------------------- | ------------------ |
| 컬럼 이름             | `columns[].name`   |
| `Null` = `N`          | `not_null`         |
| 종류 = `PK`, 단일 Key | `unique`           |
| 종류 = `PK`, 복합 Key | Singular Test 파일 |
| 종류 = `FK`           | `relationships`    |
| `정의` 열의 값 목록   | `accepted_values`  |

타입은 `schema.yml`에 적지 않는다. Model SQL의 `CAST`가 타입의 정본이다.

## 관련 문서

- [데이터 변환 흐름](data-transformation-flow.md) — Source에서 Staging까지의 이름·타입·값 변환
- [PRD v1.9](../../PRD_v1.9.md) — Section 14 dbt Model, Section 15 SCD2와 Temporal Join
- [Phase 6. Dimensional Modeling](../phases/phase-06-dimensional-modeling.md) — Model 구현 순서와 Task
- [Phase 7. Data Quality & Publish](../phases/phase-07-data-quality-publish.md) — Grain/Measure Test의 Publish Gate
- [Phase 10. BI](../phases/phase-10-bi.md) — Report Model 소비와 Dashboard Metric 정의
