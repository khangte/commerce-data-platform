# Reliability Runbook 색인

## 공통 사전 조건

PostgreSQL·SeaweedFS를 기동하고 Airflow와 다른 상태 변경 테스트를 멈춘 뒤 격리 DB·버킷을 만든다.
스크립트는 기존 격리 데이터를 지우므로 테스트 실행 중에는 다시 실행하지 않는다.
공유 DB는 Airflow 실행에만 쓰고 상태 변경 테스트는 한 담당자만 격리 환경에서 순서대로 실행한다.
dbt를 실행하는 시나리오는 `RUN_DBT_PUBLISH_INTEGRATION=1`도 필요하다.

```bash
docker compose up -d postgres seaweedfs
./scripts/setup_test_isolation.sh
COMMERCE_SOURCE_DB=commerce_source_it PIPELINE_METADATA_DB=pipeline_metadata_it SEAWEEDFS_BUCKET=commerce-lake-it RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability -v
# 특정 시나리오: -k "r02 or r03 or r15"
```

증적 JSON은 `data/reliability/`에 생성되며 버전 관리하지 않는다. Runbook에는 `batch_id`, `run_id`,
`table_batch_id`, `publish_run_id`만 필요한 범위에서 기록한다.

| ID | 시나리오 | 증적 식별자 | Runbook |
| --- | --- | --- | --- |
| R-01 | Duplicate Batch | `batch_id`, `run_ids`(3회 재실행) | [중복 Batch](r01-duplicate-batch.md) |
| R-02 | Upload Failure | `batch_id`, `object_key` | [업로드 실패](r02-upload-failure.md) |
| R-03 | Metadata Failure | `batch_id` | [Metadata 실패](r03-metadata-failure.md) |
| R-04 | Watermark CAS Conflict | `pipeline_name`, 승자/패자 `run_id`, `winner_table_batch_id` | [Watermark CAS 충돌](r04-watermark-cas-conflict.md) |
| R-05 | Expired Lease | `pipeline_name`, Stale/신규 `owner_id` | [만료 Lease](r05-expired-lease.md) |
| R-06 | Orphan Object | `batch_id`(accepted/rejected), `object_key`, `manifest_key` | [Orphan Object](r06-orphan-object.md) |
| R-07 | Broken Manifest | `batch_id`, `object_key`, `manifest_key` | [깨진 Manifest](r07-broken-manifest.md) |
| R-08 | Late Order | `pipeline_name`, `late_order_id` | [늦게 도착한 주문](r08-late-order.md) |
| R-09 | Late Payment | `pipeline_name`, `order_id` | [늦게 도착한 결제](r09-late-payment.md) |
| R-10 | Customer history change | `pipeline_name`, `customer_unique_id`, `subscription_id` | [고객 이력 변경](r10-customer-history-change.md) |
| R-11 | Missing Schedule | `pipeline_name`, `recovery_object_key`, `run_c_object_key` | [누락 구간 명시 Batch 복구](r11-missing-schedule.md) |
| R-12 | Backfill Replay | `pipeline_name`, `bronze_as_of` | [Backfill Replay](r12-backfill-replay.md) |
| R-13 | Re-extract | `pipeline_name`, `original_batch_id`, `re_extract_batch_id` | [재추출](r13-re-extract.md) |
| R-14 | dbt Failure | `first_publish_run_id`, `failing_publish_run_id`, `second_publish_run_id` | [dbt 실패](r14-dbt-failure.md) |
| R-15 | Source Connection Failure | `batch_id` | [Source 연결 실패](r15-source-connection-failure.md) |
