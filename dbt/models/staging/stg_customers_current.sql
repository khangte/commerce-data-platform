with customer_versions as (
    select
        customer_id as source_customer_id,
        customer_unique_id as customer_id,
        customer_city as city,
        customer_state as state,
        {{ standardized_membership_level('membership_level') }} as membership_level,
        created_at,
        updated_at,
        _batch_id,
        _run_id,
        _ingested_at,
        _source_table,
        _schema_version
    from {{ current_bronze_records('customers', ['customer_id']) }}
),
orders_current as (
    select
        customer_id as source_customer_id,
        order_id,
        order_purchase_timestamp as purchase_at
    from {{ current_bronze_records('orders', ['order_id']) }}
),
latest_order_by_source_customer as (
    select
        source_customer_id,
        order_id,
        purchase_at
    from (
        select
            *,
            row_number() over (
                partition by source_customer_id
                order by purchase_at desc, order_id desc
            ) as _order_rank
        from orders_current
    )
    where _order_rank = 1
),
customer_bounds as (
    select
        customer_id,
        min(created_at) as created_at,
        max(updated_at) as updated_at
    from customer_versions
    group by customer_id
),
representative_customer as (
    select
        customer_versions.*,
        customer_bounds.created_at as customer_created_at,
        customer_bounds.updated_at as customer_updated_at,
        row_number() over (
            partition by customer_versions.customer_id
            order by
                customer_versions.updated_at desc,
                latest_order_by_source_customer.purchase_at desc nulls last,
                latest_order_by_source_customer.order_id desc nulls last,
                customer_versions.source_customer_id desc
        ) as _customer_rank
    from customer_versions
    inner join customer_bounds using (customer_id)
    left join latest_order_by_source_customer using (source_customer_id)
),
representative_attributes as (
    select
        customer_id,
        membership_level,
        city,
        state
    from representative_customer
    where _customer_rank = 1
)
select
    customer_versions.source_customer_id,
    customer_versions.customer_id,
    representative_attributes.membership_level,
    representative_attributes.city,
    representative_attributes.state,
    customer_bounds.created_at,
    customer_bounds.updated_at,
    customer_versions._batch_id,
    customer_versions._run_id,
    customer_versions._ingested_at,
    customer_versions._source_table,
    customer_versions._schema_version
from customer_versions
inner join customer_bounds using (customer_id)
inner join representative_attributes using (customer_id)
