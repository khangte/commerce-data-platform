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

{% macro standardized_membership_tier(column_name) -%}
    case {{ column_name }}
        when 'BRONZE' then 'BRONZE'
        when 'SILVER' then 'SILVER'
        when 'GOLD' then 'GOLD'
        else null
    end
{%- endmacro %}

{% macro standardized_subscription_status(column_name) -%}
    case {{ column_name }}
        when 'NON_MEMBER' then 'NON_MEMBER'
        when 'TRIAL' then 'TRIAL'
        when 'ACTIVE' then 'ACTIVE'
        when 'PAYMENT_FAILED' then 'PAYMENT_FAILED'
        when 'CANCEL_REQUESTED' then 'CANCEL_REQUESTED'
        when 'CHURNED' then 'CHURNED'
        else null
    end
{%- endmacro %}
