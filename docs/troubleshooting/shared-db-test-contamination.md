# 공유 DB의 통합 테스트 오염과 복구

2026-11-13 통합 테스트가 공유 원천 DB의 실제 시뮬레이션 행을 진행시키면서 `updated_at`(변경 시각)을 2100년으로 바꿨다. 테스트가 누출한 `COMMITTED`(커밋 완료) 카탈로그 행도 있어, 후속 수집의 워터마크와 실제 마트에 테스트 데이터가 섞일 위험이 있었다. Airflow를 중지하고 두 DAG를 일시정지한 상태에서 복구했다.

## 원인과 범위

`tests/integration/test_generator_service_integration.py`는 실제 생성기를 실행하여 열린 주문과 구독을 모두 진행시킨다. 테스트 정리는 자신이 만든 행만 제거한다. 다른 통합 테스트의 2100년 고정 시각 데이터가 같은 DB에 존재하는 동안 생성기가 실행돼 실제 행까지 해당 시각으로 진행됐다. 단독 실행도 공유 데이터의 상태를 변경하므로, 실행 순서만 조절해서는 보호할 수 없다.

복구 전 깨끗한 HEAD 코드에서도 같은 통합 테스트 12건이 실패했다. 복구 검증을 위해 공유 DB에서 테스트를 다시 실행하지 않았다. 조회와 커밋된 Bronze 데이터 대조로만 복구를 확인했다.

## 백업과 실행 기록

백업 위치는 저장소 밖 `~/commerce-data-platform-backups/2026-11-13/`이다. 인증 정보는 문서에 기록하지 않는다.

| 파일 | 크기(바이트) | SHA-256 |
| --- | ---: | --- |
| `commerce_source.dump` | 27,544,108 | `cd572a6b61db090d331f4e9f5830d3266172813cfed644433677264cbd7a39e2` |
| `pipeline_metadata.dump` | 227,540 | `8213f81ca29d3cd5aa20d1372711f8db749913bd8430fa33e689ceac8a866ec6` |

두 파일은 `pg_dump -Fc`로 만든 백업이며, 복구 쓰기 전에 각각 임시 `*_restorecheck` DB에 `pg_restore --exit-on-error --no-owner --no-privileges`로 실제 복원했다. 모든 테이블의 행 수가 원본과 같음을 확인한 후 임시 DB를 지웠다. 삭제할 S3 객체 6개도 같은 디렉터리의 `s3-objects/`에 원래 키대로 보존하고 SHA-256과 실제 바이트 일치를 확인했다.

실행한 명령은 다음과 같다. 이 스크립트들은 이번 사고의 정확한 대상만 다루는 저장소 밖 기록이며, 다른 사고에 그대로 적용하지 않는다.

```bash
PYTHONPATH=. uv run python ~/commerce-data-platform-backups/2026-11-13/verify_backups.py
PYTHONPATH=. uv run python ~/commerce-data-platform-backups/2026-11-13/recover_phase0.py
PYTHONPATH=. uv run python ~/commerce-data-platform-backups/2026-11-13/recover_phase0.py --apply
```

첫 복구 실행은 대상과 원본을 확인하는 조회 전용 실행이다. 실제 실행은 원천 테이블을 잠근 뒤 대상이 달라지지 않았는지 재확인하고, 모든 원천 복구와 삭제를 하나의 트랜잭션에서 수행했다. 메타데이터 정리도 별도 트랜잭션에서 정확한 식별자만 지정했다. `recovery-plan.json`에 복구 전 행, 복구할 전체 열, 원본 객체 키, 삭제 대상 행과 객체 키를 기록했다. `backup-verification.json`과 `recovery-validation.json`에 검증 결과를 남겼다.

## 복구한 행과 삭제한 테스트 산출물

주문과 주문 결제는 실제 `warehouse_pipeline_dag`의 2026-11-13 02:02 UTC 커밋 데이터에서, 구독은 2026-11-11 06:10 UTC 커밋 데이터에서 복구했다. 상태·업무 일시·청구 필드를 포함하여 수집 부가 열을 제외한 **모든 원천 열**을 원본으로 돌렸다.

| 테이블 | 복구한 기본 키 |
| --- | --- |
| `orders` | `33dac5bedbec57df82c8962ea712d2af`, `39bc61fa3ffa548b8672fabc7897bbd0`, `3d39716bea035a678f8f3e4235180b47`, `6bda7931271d5049b747512739c6a99b`, `ea0cf527625b5950843d493cf92df4e4`, `f10def2d030451539d5a968ce75cd44a`, `fd7475bfd3965580899315e4a84244e5`, `9e96ff467d5453acabe86223d7331ae2`, `f2c65703939e5e77a8d45b25de9f60a5`, `ff5151d2bf85500ea1da67ec5ae68a5f`, `6f12116ee9eb5b7ba99a43a8a30ec667`, `0f6068f4aafa5999859aaf9b2ffd5f03` |
| `order_payments` | (`0f6068f4aafa5999859aaf9b2ffd5f03`, `payment_sequential=1`) |
| `customer_subscriptions` | `ca2a6fb7-710c-5cda-b2b1-5daa09f05539`, `d3fc3b15-0172-56c7-94ab-e05f47e76446` |

새로 생성된 `subscription_payments` 테스트 행 4개는 `payment_id`로 정확히 지정해 삭제했다: `9b90d907-bd76-58cc-95d2-67430fbda8fd`, `370b0b99-4de0-5e44-9392-8a7fade5d109`, `58f7b9e9-5032-5fc9-9190-6337d3a28e31`, `82f91ed8-4c23-549f-9c19-3397d7e6732f`.

다음 누출 배치 3개의 `bronze_objects` 행과 각 배치의 `data.parquet`·`manifest.json` 객체를 삭제했다. 이 배치들에 연결된 `pipeline_runs`, `watermarks`, `quarantine_batches` 행은 조회 결과 모두 0개였다.

- `test_two_batches_one_build_ff5371f06ef8439d917742d58ee175c1_1__21000102T000000Z`
- `test_two_batches_one_build_f9559398b5944478b5e38bb360df2327_1__21000102T000000Z`
- `test_two_batches_one_build_f4284f424d544a9a8885c3f3f57964e6_1__21000102T000000Z`

9월부터 존재한 `test_orders_lease_conflict_9cc52be1e4804b07ab10389eaa28fbfa` 워터마크는 이번 오염과 무관하여 보존했다. 연결된 실행과 Bronze 배치는 각각 0건이었다. 이 행의 테스트용 2100년 워터마크 값은 원천 DB의 커서 열이나 실제 `*_bronze` 워터마크가 아니다.

## 복구 검증 결과

2026-11-13 12:38 KST 기준으로 다음을 확인했다.

- 복구한 원천 15행의 모든 열이 커밋된 Bronze 원본과 일치했다. 트랜잭션 커밋 전에 대조했다.
- 해당 주문의 `order_items` 21행은 원본과 일치하며, 복구 전후에도 동일했다.
- 원천 모든 테이블의 `created_at`·`updated_at`에 2099년 이후 값은 0건이었다.
- 실제 워터마크 9행과 `generator_commits` 72행의 내용 SHA-256이 복구 전후 동일했다.
- 원천 테이블 행 수는 모두 백업과 같고, 의도적으로 삭제한 구독 결제만 13행에서 9행이 됐다.
- `bronze_objects`는 588행에서 585행이 됐으며 테스트 식별자로 시작하는 배치는 0건이었다. 지정 객체 6개는 삭제 후 HEAD 요청이 모두 404였다.

실제 워터마크 해시는 `f1fe925a7a0eef357585d7f879fe8ab4b6fdd49d1b1d1454e0ab7e3736db3930`, 생성기 커밋 해시는 `109b9642f9d7b6f250b1b6e7ddc308c58c657700cc8f19663304bee23b59416e`였다. 해시는 복구 스크립트의 정렬된 JSON 표현 기준이다.

## 재발 방지

상태 변경 테스트는 공유 DB에서 실행하지 않는다. `scripts/setup_test_isolation.sh`로 격리 DB와 버킷을 준비한 다음 다음 명령을 사용한다. 공유 DB를 사용하는 Airflow 실행과 격리 테스트의 실행 소유자를 구분하고, 격리 환경에서도 상태 변경 테스트는 한 실행씩 수행한다.

```bash
bash scripts/setup_test_isolation.sh
COMMERCE_SOURCE_DB=commerce_source_it PIPELINE_METADATA_DB=pipeline_metadata_it SEAWEEDFS_BUCKET=commerce-lake-it RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/integration
```

이 문서의 복구 완료는 격리 환경의 통합 테스트 통과를 뜻하지 않는다. 격리 스크립트의 반복 실행, 공유 환경 차단 검사와 격리 테스트 결과는 [Phase 1 문서](../phases/phase-01-source-environment.md)의 후속 검증 기록을 따른다.
