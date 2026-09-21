# Commit과 Lease 오류 유형

`src/ingestion/metadata.py`의 `commit_table_run`(Watermark CAS·Table Lease Fencing)과
`src/ingestion/orphan.py`의 `reconcile_orphan`이 낼 수 있는 예외를 원인·조치와 함께 정리한다.
관련 시나리오는 R-04(CAS 충돌), R-05(만료 Lease), R-06(Orphan Object), R-07(깨진 Manifest)이다.

| 예외/코드 | 발생 지점 | 원인 | 조치 |
| --- | --- | --- | --- |
| `WATERMARK_CONFLICT` | `commit_table_run` Watermark CAS | 같은 `pipeline_name`/`source_table`, 같은 기준 Watermark로 두 Run이 동시에 Commit을 시도한다. 먼저 도착한 Run만 `version` UPDATE가 성공하고, 나중 Run은 `rowcount == 0`으로 Transaction 전체가 롤백된다 | 재시도 불가능(이 시도 기준). 승자의 Commit이 이미 반영됐는지 확인한 뒤 새 `batch_id`로 재수집한다 |
| `LEASE_OWNERSHIP_LOST` | `assert_table_lease`, `commit_table_run`(ADR-017 이후) | Table Lease TTL이 만료된 뒤 새 Owner가 인수하면, 이전(Stale) Owner는 이후 어떤 Metadata 쓰기 시도에서도 Fencing된다 | 재시도 불가능. Stale Owner Run을 FAILED로 처리하고, 새 Owner Run이 정상 진행 중인지 확인한 뒤 개입 없이 완료를 기다린다 |
| `OrphanReconciliationError` | `reconcile_orphan` | Orphan 후보의 HEAD Checksum/Row Count/Pipeline Run 범위가 VERIFIED Manifest와 불일치하거나, 같은 Batch에 Quarantine Reject가 존재한다 | 자동 재조정이 거부된다. Manifest 변조·손상이 원인이면 원본을 복구한 뒤 재시도하고, 복구할 수 없으면 수동 검토 후 Object를 폐기한다 |
| `SOURCE_CONTRACT_ERROR` | `reconcile_orphan` → `assert_supported_schema_version` | Manifest의 `schema_version`이 Table 계약이 지원하지 않는 값으로 변조되었다 | 재시도 불가능. Source 쪽 계약 위반(지원하지 않는 Schema Version)을 고친 뒤 재실행한다 |

`reconcile_orphan`은 이 두 예외를 호출자에게 그대로 던질 뿐 `classify_error`/`record_failed_run`을
부르지 않는다 — 사람이 수동으로 호출하는 재조정 절차라 `pipeline_runs`에 `FAILED` 행을 남기지 않는다.
`SOURCE_CONTRACT_ERROR` 자체는 `classify_error`가 반환하는 계약 코드이고, `ingest_table` 경로에서
발생하면 `pipeline_runs`에 그대로 남는다([Ingestion 오류](ingestion-errors.md)) — 여기서 기록되지
않는 건 코드가 아니라 `reconcile_orphan`이라는 호출 경로 자체의 문제다.

## 알려진 위험이었던 사전 검사와 Commit 사이 창(수정 완료)

`commit_table_run`은 원래 `lease_owner`를 확인하지 않고 Watermark CAS(`version`/`watermark_timestamp`/
`watermark_keys`)만 검사했다. Lease 인수 직후 새 Owner가 아직 아무것도 Commit하지 않은 상태라면
`version`이 인수 전과 같아, Stale Owner가 `assert_table_lease` 없이 `commit_table_run`을 직접 호출하면
Commit이 성공하는 창이 있었다. `commit_table_run` Transaction 맨 앞에서 `watermarks` 행을 `FOR UPDATE`로
잠그고 `lease_owner`/`lease_expires_at`를 검사하도록 고쳐 창을 닫았다. 근거는
[ADR-017](../adr/017-commit-table-run-lease-fencing.md)과
[R-05 Runbook](../runbooks/r05-expired-lease.md)을 따른다.

## 관련 Runbook

- [R-04 Watermark CAS 충돌](../runbooks/r04-watermark-cas-conflict.md)
- [R-05 만료된 Lease](../runbooks/r05-expired-lease.md)
- [R-06 Orphan Object](../runbooks/r06-orphan-object.md)
- [R-07 Broken Manifest](../runbooks/r07-broken-manifest.md)
