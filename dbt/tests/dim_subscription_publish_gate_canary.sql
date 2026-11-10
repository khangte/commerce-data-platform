-- subscriptions 도메인 Gate 분리 검증용 Canary. --vars '{dim_subscription_publish_gate_canary: true}'일 때만 1 Row를 반환해 실패한다.
-- depends_on: {{ ref('dim_subscription') }}
select 1 as failure
where {{ var('dim_subscription_publish_gate_canary', false) }}
