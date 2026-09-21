# Reliability 오류 코드 색인

각 시나리오가 증명한 오류 코드의 원인과 조치를 정리한다. 시나리오 열의 `R-XX`는 실측 시나리오,
`—(문서 근거)`는 Reliability 시나리오 없이 코드·문서 근거로만 확인한 항목이다. Runbook에는 실행
식별자만 기록하며 인증 정보, 원시 Payload, 절대 경로는 기록하지 않는다.

| 오류 코드 또는 예외 | 시나리오 | 원인 | 조치 | 문서 |
| --- | --- | --- | --- | --- |
| `OBJECT_STORAGE_ERROR` | R-02 | `ClientError`(S3 503) — 재시도 가부는 HTTP 상태(5xx/429)로 갈림 | 5xx/429면 같은 `batch_id`로 재시도, 4xx면 원인(권한·설정) 수정 | [업로드 실패](../runbooks/r02-upload-failure.md), [Ingestion 오류](ingestion-errors.md) |
| `UNKNOWN_ERROR` | R-03 | `RuntimeError`(Metadata Commit 직전 주입) — 분류 규칙에 없는 예외의 기본값 | Orphan 후보로 남으면 자동 재조정 대상인지 확인, 원인 예외 메시지로 근본 원인 파악 | [Metadata 실패](../runbooks/r03-metadata-failure.md), [Ingestion 오류](ingestion-errors.md) |
| `LEASE_UNAVAILABLE` | —(문서 근거) | `LeaseUnavailableError`(원천 동시성 잠금)·`TableLeaseUnavailableError`(테이블별 수집 잠금) — 다른 활성 실행이 잠금을 쥐고 있음 | 재시도 가능. 잠금 보유자가 끝날 때까지 기다린 뒤 재시도, 어느 잠금이었는지는 `error_message`로 확인 | [Ingestion 오류](ingestion-errors.md) |
| `WATERMARK_CONFLICT` | R-04 | 같은 `pipeline_name`/`source_table`, 같은 기준 Watermark로 두 Run이 동시에 Commit 시도 — 나중 Run의 CAS UPDATE가 `rowcount == 0` | 승자 Commit이 반영됐는지 확인 후 새 `batch_id`로 재수집 | [CAS 충돌](../runbooks/r04-watermark-cas-conflict.md), [Commit과 Lease](commit-and-lease.md) |
| `LEASE_OWNERSHIP_LOST` | R-05 | Table Lease TTL 만료 후 새 Owner가 인수, Stale Owner가 이후 쓰기 시도에서 Fencing됨 | Stale Owner Run은 FAILED 처리, 새 Owner Run 완료를 개입 없이 대기 | [만료 Lease](../runbooks/r05-expired-lease.md), [Commit과 Lease](commit-and-lease.md) |
| `OrphanReconciliationError` | R-06, R-07 | Orphan 후보의 Checksum/Row Count/Run 범위가 VERIFIED Manifest와 불일치, 또는 Quarantine Reject 존재 | 자동 재조정 거부됨. 원본 복구 가능하면 재시도, 불가하면 수동 검토 후 Object 폐기 | [Orphan Object](../runbooks/r06-orphan-object.md), [깨진 Manifest](../runbooks/r07-broken-manifest.md), [Commit과 Lease](commit-and-lease.md) |
| `SOURCE_CONTRACT_ERROR` | R-07 | Manifest의 `schema_version`이 Table 계약이 지원하지 않는 값으로 변조됨 | 재시도 불가능. Schema Version 계약 위반을 고친 뒤 재실행 | [깨진 Manifest](../runbooks/r07-broken-manifest.md), [Commit과 Lease](commit-and-lease.md) |
| `DBT_TEST_ERROR` | R-14 | Singular/Generic Test 실패 | Upstream Model/Source 수정, Published Mart는 불변 | [dbt 실패](../runbooks/r14-dbt-failure.md), [Warehouse Publish](warehouse-publish.md) |
| `DBT_BUILD_ERROR` | —(문서 근거) | Model/Seed/Snapshot Node Error | 실패 Node SQL/의존성 수정 후 재Build | [dbt 실패](../runbooks/r14-dbt-failure.md), [Warehouse Publish](warehouse-publish.md) |
| `UNKNOWN_ERROR` (Abandoned Run) | —(문서 근거) | `PUBLISH_STALE_AFTER` 경과한 Building/Publishing Run | 다음 실행이 자동 회수, Published 파일 일치만 확인 | [Warehouse Publish](warehouse-publish.md) |
| `PublishInProgressError` | —(문서 근거) | 다른 활성 Publish Run 존재 | 종료 대기 또는 자동 Abandoned 회수 대기 | [Warehouse Publish](warehouse-publish.md) |
| `PublishedWalError` | —(문서 근거) | Published 파일 옆 WAL 잔존 | CHECKPOINT 상태 확인 후 WAL 제거 | [Warehouse Publish](warehouse-publish.md) |
| `SOURCE_CONNECTION_ERROR` | R-15 | `psycopg.OperationalError`(Snapshot 연결 열기 실패) | 재시도 가능. Object/Watermark 불변이므로 같은 `batch_id`로 즉시 재시도 | [Source 연결 실패](../runbooks/r15-source-connection-failure.md), [Ingestion 오류](ingestion-errors.md) |
