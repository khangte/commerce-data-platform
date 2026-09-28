# ADR 008. Olist 원천 Schema를 보존하고 dbt Staging에서 표준화한다

## Status

Accepted (PRD v1.14)

## Context

원천 Olist Column 이름과 상태값은 수집 Lineage와 Raw-compatible 계약을 위해 보존돼야 한다.
동시에 분석 계층은 의미가 분명한 이름, UTC Timestamp, 대문자 표준 상태, 사람 단위 Business Key를
필요로 한다. Source/Bronze에서 바로 Rename하면 두 책임을 구분할 수 없다.

## Decision

- Source와 Bronze는 선택한 Olist Column의 `snake_case` 이름·Prefix·원천 상태값을 보존한다.
- 프로젝트 확장 Column만 기존 Olist Column과 충돌하지 않는 `snake_case` 이름을 쓴다.
- dbt Staging에서 Prefix 제거, Timestamp Suffix 통일, 대문자 상태값 변환, 분석 Key Mapping을 수행한다.
- Source `customer_id`는 `source_customer_id`로 보존하고, 분석 `customer_id`는
  `customer_unique_id`에서 노출한다.
- Column Rename은 Bronze Schema 변경이 아니라 Staging Alias로 처리한다.

## Alternatives

| 대안 | 기각 사유 |
| --- | --- |
| Source 적재 시 분석용 Rename | Raw-compatible Source 계약과 Lineage가 깨진다. |
| Bronze마다 표준화된 상태값 저장 | 원천 값 보존과 표준화 책임이 섞여 재처리 비교가 어려워진다. |
| Mart에서 처음 표준화 | Intermediate·Mart가 원천 용어에 의존하고 변환 책임이 늦어진다. |

## Consequences

Source Allowlist와 Staging Mapping Test가 계약의 일부다. 분석용 Join은 Staging 이후 Key를 사용하며,
Intermediate/Mart는 Source Prefix를 직접 참조하지 않는다. Source/Bronze의 구조적 변경은 ADR-015를
따른다.

## Validation

- Source Header Allowlist와 DDL이 Olist 원본 이름·타입을 보존하는지 확인한다.
- Staging Mapping Test로 Prefix, Timestamp Alias, 상태 표준화, 고객 Business Key를 행 단위 대조한다.
- Intermediate/Mart에 Raw Source Prefix 직접 참조가 없는지 정적·dbt 검증한다.
