{% test greater_than_zero(model, column_name) %}

with validation as (

    select
        {{ column_name }} as greater_than_zero

    from {{ model }}

),

validation_errors as (

    select
        greater_than_zero

    from validation
    -- if this is true, then even_field is actually odd!
    where greater_than_zero <= 0

)

select *
from validation_errors

{% endtest %}