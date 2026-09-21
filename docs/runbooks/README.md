# Reliability Runbook 색인

## 공통 사전 조건

PostgreSQL과 SeaweedFS가 포함된 Compose 서비스를 기동한 뒤 기존 통합 환경 플래그를 설정한다.
dbt를 실행하는 시나리오는 `RUN_DBT_PUBLISH_INTEGRATION=1`도 필요하다.

```bash
docker compose up -d
RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability -v
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
