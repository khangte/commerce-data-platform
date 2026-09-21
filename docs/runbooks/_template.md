# R-XX 시나리오 제목

## 문제

발생한 신뢰성 시나리오와 영향을 한 문장으로 기록한다. 관련 `batch_id`, `run_id`,
`table_batch_id`, `publish_run_id`, `reprocess_id` 중 실제로 생성된 식별자만 기록한다.

## 재현 조건과 명령

필요한 Compose 서비스와 기존 통합 환경 플래그를 적고, 상대 경로 기준의 재현 명령을 기록한다.
명령에는 인증 정보나 절대 경로를 포함하지 않는다. 재현에 사용한 `batch_id`와 `run_id`를 기록한다.

## 기대/실제 관측

기대 상태 전이와 실제 상태 전이를 표로 비교한다. Evidence JSON의 식별자, Object Key, Hash 앞 12자,
Row Count를 요약하며 필요하면 `table_batch_id`와 `publish_run_id`를 함께 적는다.

## 원인과 불변 조건

원인과 보호해야 하는 불변 조건을 설명한다. Watermark, COMMITTED Metadata, Published Mart,
Lease 중 해당하는 상태가 왜 유지되거나 전이돼야 하는지 적는다.

## 복구 절차

복구 전에 확인할 식별자와 실행 순서를 적는다. 재수집, Orphan 재조정, 명시 Batch, Replay 중 적용한
방법과 대상 `batch_id`, `table_batch_id`, `reprocess_id`를 기록한다.

## 재검증 명령과 결과

상대 경로의 검증 명령과 통과 결과를 기록한다. 결과에는 재검증한 `run_id` 또는 `publish_run_id`와
핵심 Hash·Row Count 요약을 포함한다.
