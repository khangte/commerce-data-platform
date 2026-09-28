-- 현재 시점 구독 상태별 계약 수 분포: 계약마다 최신 SCD2 Version 하나만 사용한다.
select
    subscription_status,
    count(distinct subscription_id) as contract_count
from dimensions.dim_subscription
where is_current = true
group by subscription_status
order by contract_count desc, subscription_status;
