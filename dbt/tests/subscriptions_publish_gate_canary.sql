-- subscriptions 도메인 Gate 분리 검증용 Canary. --vars '{subscriptions_publish_gate_canary: true}'일 때만 1 Row를 반환해 실패한다.
-- depends_on: {{ ref('fct_subscription_payment') }}
select 1 as failure
where {{ var('subscriptions_publish_gate_canary', false) }}
