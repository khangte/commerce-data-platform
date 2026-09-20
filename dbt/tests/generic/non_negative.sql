{% test non_negative(model, column_name) %}
-- 금액·수량 Column에 음수가 없는지 검증한다. NULL은 not_null Test가 따로 판단한다.
select *
from {{ model }}
where {{ column_name }} < 0
{% endtest %}
