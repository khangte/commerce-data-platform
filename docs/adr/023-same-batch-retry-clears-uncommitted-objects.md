# ADR 023. 같은 Batch 재시도는 자기 미확정 Object를 지우고 다시 올린다

## Status

Accepted (2026-11-13). 구현·격리 환경 통합 및 실제 Airflow 재시도 검증 완료. [ADR 014](014-use-metadata-as-bronze-commit-authority.md)의 "Commit 권한은 Metadata에 있다"를 재시도 경로에 적용하고, [ADR 018](018-ingestion-error-type-classification.md)이 범위 밖으로 남긴 412 → `FileExistsError` → `UNKNOWN_ERROR` 경로 가운데 같은 Batch 재시도 부분을 닫는다.

계획: `docs/superpowers/plans/2026-11-13-audit-remediation.md` Phase 1

## Context

Table 수집은 Quarantine Upload → Bronze `data.parquet` Upload → `manifest.json` Upload → Metadata Commit 순서로 돈다. Object Key는 `batch_id`로만 정해지고, 같은 DagRun의 Airflow 재시도는 같은 `batch_id`와 새 `run_id`를 쓴다.

Upload는 기존 Key를 거부한다(HEAD 확인과 `IfNoneMatch="*"`). 그래서 Object 하나라도 올라간 뒤 실패하면 재시도가 다음과 같이 끝난다.

| 실패 지점 | 재시도 결과 | 문서의 약속 |
| ---- | ---- | ---- |
| Quarantine은 올라가고 Bronze PUT이 5xx | `FileExistsError` → `UNKNOWN_ERROR` → 재시도 없이 실패 | R-02: 같은 `batch_id`로 재시도하면 복구된다 |
| `data.parquet`는 올라가고 Manifest PUT이 5xx | 같음 | R-02와 같음 |
| Upload는 끝나고 Metadata Commit이 연결 오류 | 같음 | R-03: 처리된 실패는 자동 복구를 거부한다 |

위 두 경우는 문서의 약속과 다르다. 세 번째는 문서대로지만 오류 이름이 원인을 말해 주지 않는다. 세 경우 모두 첫 실패는 재시도 가능으로 분류되므로(`OBJECT_STORAGE_ERROR` 5xx, `SOURCE_CONNECTION_ERROR`) Airflow는 재시도하고, 그 재시도가 반드시 실패한다.

데이터는 잃지 않는다. Watermark가 움직이지 않아 다음 시간대 실행이 같은 지점부터 다시 뽑는다. 잃는 것은 그 시간대 DagRun과 버려진 Object다.

남은 Object를 Hash로 비교해 재사용하는 방법은 쓸 수 없다. Bronze Parquet에는 시도마다 다른 `_run_id`·`_ingested_at` 열이 들어가 같은 행을 다시 뽑아도 Byte가 다르다.

## Decision

같은 `batch_id`의 재시도는 자기 미확정 Object를 지우고 처음부터 다시 올린다.

| 항목 | 내용 |
| ---- | ---- |
| 시점 | `ingest_table`이 Table Lease를 얻은 뒤, Upload 전 |
| 조건 | `bronze_objects`에 해당 `table_batch_id` 또는 `object_key` 행이 없고, `quarantine_batches`에 해당 `table_batch_id` 행이 없다 |
| 대상 | `table_object_keys`·`quarantine_object_keys`가 돌려주는 Key 네 개. Prefix 삭제는 하지 않는다 |
| Fencing | Metadata 확인 뒤, 삭제 전에 Table Lease 소유를 다시 확인한다 |
| 이후 | 지금과 같은 경로다. 추출 → 조건부 PUT → Metadata Commit |
| 기록 | 지운 Key를 로그 한 줄로 남긴다. Metadata 열은 늘리지 않는다 |

바뀌지 않는 것:

- Commit된 Object는 수정·덮어쓰기·삭제하지 않는다. 조건의 Metadata 확인이 이를 지킨다.
- 기존 Key 거부와 `IfNoneMatch="*"`는 그대로다. 다른 Batch의 Object, Metadata가 있는 Key와의 충돌은 계속 `FileExistsError`다.
- Object Key 구조와 Orphan 재조정 조건은 그대로다.

이 결정이 안전한 이유:

1. **Commit 권한은 Metadata에 있다.** `COMMITTED` 행이 없는 Object는 dbt가 읽지 않고 Watermark도 가리키지 않는다. 지워도 관측 가능한 상태는 바뀌지 않는다.
2. **채택이 아니라 다시 하기다.** R-03이 자동 복구를 거부한 이유는 남은 Object를 채택해도 되는지 추론할 수 없어서였다. 이 결정은 남은 Object를 믿지 않는다. 원천에서 다시 뽑는다.
3. **범위가 같다.** 추출 재시도 동안 Global Source Mutation Lease가 유지된다. 해제 Task는 `all_done`으로 추출이 끝난 뒤에 돈다. Lease가 만료됐다면 재시도는 Lease 확인에서 실패한다.
4. **경쟁이 없다.** 삭제도 Commit도 같은 Table Lease 아래에서 한다. 지우는 사이 다른 실행이 같은 `table_batch_id`를 Commit할 수 없다.

## Alternatives

| 대안 | 기각 이유 |
| ---- | ---- |
| 남은 Object의 크기·SHA-256이 같으면 재사용 | `_run_id`·`_ingested_at` 때문에 Byte가 같을 수 없다. 조건이 한 번도 참이 되지 않는다 |
| 논리 Hash(`table_logical_hash`)가 같으면 남은 Object를 채택 | Object를 내려받아야 하고, 파일 안의 `_run_id`와 Commit하는 Run이 달라져 계보가 어긋난다. Manifest가 없는 경우는 따로 처리해야 한다 |
| 동작 유지, 전용 Error Type과 Runbook만 추가 | 그 시간대 DagRun은 계속 실패한다. R-02의 약속을 "첫 PUT 실패에만 해당"으로 좁혀야 한다 |
| Object Key에 시도 식별자 추가 | 충돌은 없어지지만 PRD의 Key 구조가 바뀌고 버려진 Object가 시도마다 쌓인다 |
| 조건부 PUT을 풀고 덮어쓰기 | 다른 Batch나 Commit된 Object를 덮어쓸 길이 열린다. 지운 뒤 조건부 PUT을 유지하는 쪽이 방어선을 하나 더 남긴다 |

## Consequences

- 업로드 도중 실패와 Commit 실패가 Airflow 재시도 안에서 복구된다. R-02의 약속이 부분 업로드에도 참이 된다.
- R-03의 Handled 분기가 바뀐다. 같은 Batch 재시도는 복구되고, "새 명시 Batch로 재수집"은 재시도가 소진된 경우의 절차로 남는다.
- R-03의 Crash 분기(`RUNNING`으로 남은 Run)도 같은 Batch 재시도가 오면 재조정 없이 다시 수집된다. 결과 상태는 같다(해당 범위가 `COMMITTED`).
- Orphan 재조정은 재시도가 오지 않은 경우에만 쓰인다. 코드와 조건은 그대로 두고 실행 명령을 단다(계획 Phase 4).
- 코드에 Object 삭제 호출이 처음 생긴다. 호출 지점은 한 곳이고 Key 네 개로 제한한다.
- 재시도가 지운 Object의 흔적은 로그에만 남는다. 실패한 시도는 `pipeline_runs`에 `FAILED`로 이미 남아 있다.
- 남는 한계: 재시도가 모두 소진되면 Object가 남는다. 다음 시간대 Batch는 다른 Key를 쓰므로 영향이 없고, 남은 Object는 Orphan 후보로 보인다.

## Validation

미확정 객체 삭제·메타데이터 보호 경로 및 아래 통합·신뢰성 검증을 구현했다. 객체 삭제의 지정 키 제한·없는 키 건너뛰기·권한 오류 전파와 고아 객체 CLI 단위 테스트는 12건 통과했다. 격리 환경에서 `tests/integration` 125 passed / 1 skipped(403.71초), R-01~R-07 10 passed(6.15초)로 아래 통합 조건을 확인했다. 실제 Airflow 재시도도 아래 진단 DAG로 확인했다(계획 Phase 1의 완료조건).

- Manifest PUT 503 뒤 같은 `request` 재시도가 `SUCCESS`이고 `bronze_objects`가 1행 `COMMITTED`다.
- Quarantine Upload 뒤 Bronze PUT 503, Commit의 `OperationalError`도 재시도로 `SUCCESS`다.
- `COMMITTED` 행이 있는 Batch의 Object는 재실행 뒤에도 Byte가 같다.
- R-01~R-07 신뢰성 시나리오가 통과한다. R-03은 새 계약으로 증거를 다시 만든다.
- Airflow 안에서 실패를 주입한 DagRun이 재시도로 `success`가 된다.


2026-11-13 관측:

- Manifest PUT 503·Quarantine 게시 뒤 Bronze PUT 503·커밋 `OperationalError` 이후 각각 같은 요청 재시도가 `SUCCESS`였고, `COMMITTED` 1행과 워터마크 전진을 확인했다. 커밋된 객체의 바이트 보존 테스트도 통과했다.
- R-03 처리된 실패 증적 `data/reliability/r03/20261113T034854794449Z.json`: `retry_status=SUCCESS`, `retry_catalog_status=COMMITTED`, 재시도 후 고아 후보 0건. R-02 `data/reliability/r02/20261113T034853349061Z.json`은 실패 전후 워터마크가 같다.
- R-06 `data/reliability/r06/20261113T034851139833Z.json`: CLI 조회·단건 수용·Quarantine 거부 검증 통과.
- 실제 Airflow Manifest PUT 실패·재시도는 아래 진단 DAG에서 검증했다.


### 실제 Airflow 재시도 관측 (2026-11-13)

기존 Airflow 스케줄러·실행기에서 임시 진단 DAG `audit_retry_probe`의 단일 태스크 `manifest_retry`를 실행했다. `manual__2026-11-13T03:50:39.214680+00:00`은 최종 `success`, 태스크는 `try_number=2`로 성공했다. 같은 `ingest_table`을 호출하되 설정을 `commerce_source_it`·`pipeline_metadata_it`·`commerce-lake-it`으로 고정해 공유 데이터를 보호했다.

첫 시도는 `data.parquet`의 HEAD 존재를 확인한 뒤 Manifest PUT에 HTTP 503을 주입해 `FAILED`·`OBJECT_STORAGE_ERROR`로 끝났다. 두 번째 실제 스케줄러 재시도는 같은 `batch_id=audit_retry_probe__20261113T035039Z`의 미확정 `data.parquet`를 삭제한 로그를 남기고 `SUCCESS`로 끝났으며, `COMMITTED`는 정확히 1행(99,881행 수집)이었다. 실행 증거 묶음의 `airflow-retry-proof.json`, `airflow-attempt-1.log`, `airflow-attempt-2.log`, 임시 DAG 원본 `audit_retry_probe.py`에 각각 메타데이터·시도별 로그·주입 코드가 남아 있다.

이는 실제 Airflow 재시도와 같은 수집 함수를 검증한 결과다. 정규 `warehouse_pipeline_dag` 전체 처리·동적 매핑·후속 dbt 단계까지 재실행한 증거는 아니다. 신뢰성·Airflow 검증 전후 공유 원천과 실제 워터마크·Bronze 메타데이터 585행·저장 객체 1,177개 스냅샷은 완전히 일치했다.
