{% test duplicate_check(model, column_name) %}

    select
        {{ column_name }} as unique_field
    from {{ model }}
    group by 1
    having count(*) > 1


{% endtest %}