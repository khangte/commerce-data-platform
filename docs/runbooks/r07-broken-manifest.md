# R-07 Broken Manifest

## 문제

VERIFIED Manifest와 실제 Object·Run 상태가 어긋나면 `reconcile_orphan`이 자동 Commit을 거부해야 한다.
`CHECKSUM`, `ROW_RANGE`, `SCHEMA_VERSION` 3종 변형으로 변조해 각각 확인한다.

## 재현 조건과 명령

`RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability/test_r01_r07_ingestion_commit.py -k r07 -v`를 실행한다.
각 변형은 `crash_metadata_commit`으로 VERIFIED Object와 Manifest만 게시한 뒤, `tests/reliability/faults.py`의
`corrupt_manifest`로 Manifest 한 필드만 변조하고 `reconcile_orphan`을 호출한다.

## 기대/실제 관측

| 변형 | 주입 값 | 기대 예외 | 실제 |
|---|---|---|---|
| `CHECKSUM` | `content_sha256`을 전부 `0`으로 변조 | `OrphanReconciliationError`: `Object HEAD or checksum differs from the VERIFIED manifest` | 일치 |
| `ROW_RANGE` | `extract_upper_bound`을 `{timestamp: null, keys: []}`로 변조 | `OrphanReconciliationError`: `Pipeline run range differs from the orphan manifest` | 일치 |
| `SCHEMA_VERSION` | `schema_version`을 미지원 값 `4`로 변조 | `src.ingestion.schema.SourceContractError`: `SOURCE_CONTRACT_ERROR: unsupported schema_version=4` | 일치 |

세 변형 모두 거절 뒤 `bronze_objects`에 COMMITTED 행이 생기지 않고 `watermarks`가 변경 없이 유지된다.

`SCHEMA_VERSION` 변형은 AC-24(미지원 Version을 dbt 이전에 차단)를 담당한다. 차단 지점은
`OrphanReconciliationError`가 아니라 `assert_supported_schema_version`(`src/ingestion/schema.py:12`)이고,
오류 코드는 `SOURCE_CONTRACT_ERROR`다. `reconcile_orphan`(`src/ingestion/orphan.py:90`)이 Manifest를
검증하는 도중 이 계약 검사를 그대로 통과시켜 호출하기 때문이며, Orphan 재조정 전용 오류로 재포장하지
않는다. 판정 근거는 [015](../architect-review/015_r07-schema-version-variant.md)를 따른다.

`orphan.py:91-92`의 "지원되지만 Table 계약과 다른 Version" 분기는 `SUPPORTED_BRONZE_SCHEMA_VERSIONS = {3}`인
현재 구성에서 도달할 수 없다. 방어 코드로 남아 있으며 이 시나리오로는 검증하지 않는다.

## 원인과 불변 조건

`reconcile_orphan`은 HEAD 크기·Checksum, Row Count, Table 계약 Schema Version, Logical Hash,
Pipeline Run 범위, 현재 Watermark, Quarantine 존재 여부를 실제 상태와 대조한 뒤에만 Metadata·Watermark를
전이한다. 하나라도 불일치하면 Transaction을 시작하지 않아 자동 재조정이 검증되지 않은 Object를
COMMITTED로 만들 수 없다.

## 복구 절차

거절된 Orphan 후보의 `object_key`, `manifest_key`, `batch_id`를 확인한다. Manifest 변조가 실수(예: 수동
편집, 손상된 재전송)라면 원본 Manifest를 복구한 뒤 `reconcile_orphan`을 재시도한다. 원본을 복구할 수
없으면 수동 검토 후 Object를 폐기한다.

## 재검증 명령과 결과

Evidence `r07-checksum`, `r07-row_range`, `r07-schema_version`에서 각 변형의 `refusal` 메시지와
`watermarks_before == watermarks_after`를 확인하고, 위 pytest 명령과
`RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability/test_r01_r07_ingestion_commit.py -v`
(전체 File Green) 통과를 확인한다.
