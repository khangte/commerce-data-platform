with bounds as (
    select
        min(date_trunc('day', purchase_at)) as min_date,
        max(date_trunc('day', purchase_at)) as max_date
    from {{ ref('stg_orders') }}
),
date_spine as (
    select unnest(generate_series(bounds.min_date, bounds.max_date, interval 1 day)) as calendar_date
    from bounds
)
select
    cast(strftime(calendar_date, '%Y%m%d') as integer) as date_key,
    calendar_date as calendar_date,
    extract(year from calendar_date) as year,
    extract(month from calendar_date) as month,
    extract(day from calendar_date) as day,
    extract(quarter from calendar_date) as quarter,
    extract(dow from calendar_date) as day_of_week
from date_spine
