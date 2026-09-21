# 015. R-07 SCHEMA_VERSION 변형은 미지원 Version 경로로 재현한다

> 판정: architect, 2026-09-21 (Phase 8 Task 8 착수 전 developer 질의)
> 관련: AC-24, [Phase 8 계획](../superpowers/plans/2026-09-20-phase8-reliability-scenarios.md) Task 8, [014 판정](014_ingestion-error-type-vocabulary.md)

## 질의

Task 8 계획은 `SCHEMA_VERSION` 변형의 기대 거절 메시지를 `OrphanReconciliationError("Manifest schema version differs from the table contract")`로 적었다. 그러나 이 분기(`src/ingestion/orphan.py:91-92`)는 `assert_supported_schema_version`을 통과한 뒤에만 도달한다. `SUPPORTED_BRONZE_SCHEMA_VERSIONS = frozenset({3})`이고 모든 `TableConfig.schema_version`이 3이므로, "지원되지만 계약과 다른 Version"은 현재 구성에서 만들 수 없다. 재현 가능한 경로는 `assert_supported_schema_version`이 `SourceContractError`를 던지는 미지원 Version뿐이다.

## 판정

**(1) 채택. 변형을 미지원 Version(`schema_version=4`)으로 만들고 기대 예외를 `src.ingestion.schema.SourceContractError`로 계획 문서를 수정한다. (2) 소스 변경은 반려한다.**

근거는 셋이다.

1. **AC-24의 문언과 일치한다.** Phase 3 수용 기준은 "미지원 Version을 dbt 이전에 차단"이다. 차단 주체는 `assert_supported_schema_version`이고 계약 오류는 `SOURCE_CONTRACT_ERROR`다. 미지원 Version을 주입하는 변형이 AC-24를 직접 증명한다. 지원 범위 안의 불일치를 꾸며내는 편이 오히려 AC-24에서 멀어진다.
2. **[014 판정](014_ingestion-error-type-vocabulary.md)의 원칙을 그대로 적용한다.** Phase 8 Test는 현재 동작을 그대로 단언한다. 기대 문자열에 코드를 맞추지 않는다. `reconcile_orphan`이 `SourceContractError`를 `OrphanReconciliationError`로 재포장하면, Test를 통과시키려고 실패 어휘를 한 겹 덮는 것이 된다. 014가 지적한 것과 같은 종류의 어휘 훼손이다.
3. **재포장은 오히려 계약을 흐린다.** `SOURCE_CONTRACT_ERROR`는 Catalog·Reader 전역의 계약 위반을 뜻하고 `OrphanReconciliationError`는 한 Orphan의 자동 재조정 거부를 뜻한다. 전자를 후자로 감싸면 Orphan 경로에서만 미지원 Version이 다른 이름으로 보인다. 운영자가 같은 결함을 두 이름으로 보게 된다.

## 조치

| 대상 | 조치 |
| ---- | ---- |
| `docs/superpowers/plans/.../phase8-reliability-scenarios.md` Task 8 | `SCHEMA_VERSION` 행의 기대 거절을 `SourceContractError: SOURCE_CONTRACT_ERROR: unsupported schema_version=4`로 고친다. 주입 값은 `4`로 명시한다. |
| `tests/reliability/test_r01_r07_ingestion_commit.py` | `SCHEMA_VERSION` 변형만 `pytest.raises(SourceContractError, match="SOURCE_CONTRACT_ERROR")`로 단언한다. import는 `src.ingestion.schema`에서 한다. `src.ingestion.validation`에 같은 이름의 다른 클래스가 있으므로 혼동하지 않는다. 나머지 두 변형은 계획 그대로 `OrphanReconciliationError`다. |
| `tests/reliability/test_r01_r07_ingestion_commit.py` | 세 변형 모두 거절 뒤 `COMMITTED` Row 부재와 Watermark 불변을 단언하는 요건은 그대로다. 예외 타입만 변형별로 다르다. |
| `docs/runbooks/r07-broken-manifest.md` | AC-24 대응은 `SCHEMA_VERSION` 변형이 맡는다고 적되, 차단 지점이 `assert_supported_schema_version`이고 오류 코드가 `SOURCE_CONTRACT_ERROR`임을 명시한다. |
| `src/ingestion/orphan.py` | 변경하지 않는다. |

## 도달 불가 분기 처리

`orphan.py:91-92`의 Config 불일치 분기는 현재 도달할 수 없다. 그러나 제거하지 않는다. 지원 Version이 둘 이상이 되는 순간(예: 3과 4 동시 지원) 이 분기가 유일한 Table 단위 방어선이 된다. 삭제하면 그때 조용히 잘못된 Version의 Manifest가 Commit된다. 방어적 분기로 남기고, Test는 쓰지 않는다. 지원 Version이 둘 이상으로 늘어나는 시점에 이 분기의 Test를 추가한다.
