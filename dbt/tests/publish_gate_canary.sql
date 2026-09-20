-- Publish Gate 검증용 Canary. --vars '{publish_gate_canary: true}'일 때만 1 Row를 반환해 실패한다.
select 1 as failure
where {{ var('publish_gate_canary', false) }}
