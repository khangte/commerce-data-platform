-- 지표 계층 Gate 분리 검증용 Canary. --vars '{rpt_publish_gate_canary: true}'일 때만 1 Row를 반환한다(기본 warn).
{{ config(severity=var('metrics_test_severity', 'warn')) }}
select 1 as failure
where {{ var('rpt_publish_gate_canary', false) }}
