{% macro standardized_order_status(column_name) -%}
    case {{ column_name }}
        when 'created' then 'CREATED'
        when 'approved' then 'APPROVED'
        when 'processing' then 'PROCESSING'
        when 'invoiced' then 'INVOICED'
        when 'shipped' then 'SHIPPED'
        when 'delivered' then 'DELIVERED'
        when 'canceled' then 'CANCELED'
        when 'unavailable' then 'UNAVAILABLE'
        else null
    end
{%- endmacro %}

{% macro standardized_payment_status(column_name) -%}
    case {{ column_name }}
        when 'pending' then 'PENDING'
        when 'completed' then 'COMPLETED'
        when 'failed' then 'FAILED'
        when 'refunded' then 'REFUNDED'
        else null
    end
{%- endmacro %}

{% macro standardized_membership_level(column_name) -%}
    case {{ column_name }}
        when 'bronze' then 'BRONZE'
        when 'silver' then 'SILVER'
        when 'gold' then 'GOLD'
        else null
    end
{%- endmacro %}
