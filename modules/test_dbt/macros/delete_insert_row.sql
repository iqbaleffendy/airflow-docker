{% macro delete_row(table_name) %}

    delete from {{table_name}}
    where month_year = '{{var("month_year")}}'

{% endmacro %}


{% macro insert_row(table_target, table_to_insert) %}

    insert into {{table_target}} (select * from {{table_to_insert}})

{% endmacro %}

