{% macro drop_staging_tables(table_prefix, dryrun = False) %}

{% set cleanup_query_statement %}
    select 
        concat('DROP ', table_type, ' IF EXISTS ', table_schema, '.', table_name) as statement
    from information_schema.tables 
    where table_type = 'VIEW'
    and table_name ilike '{{table_prefix}}_%'
{% endset %}

{% do log(cleanup_query_statement, info=True) %}
{% set drop_commands = run_query(cleanup_query_statement).columns[0].values() %}

{% if drop_commands %}

    {% if dryrun | as_bool == False %}
        {% do log('Executing DROP commands...', True) %}
    {% else %}
        {% do log('Printing DROP commands...', True) %}
    {% endif %}

    {% for drop_command in drop_commands %}
        {% do log(drop_command, True) %}
        {% if dryrun | as_bool == False %}
            {% do run_query(drop_command) %}
        {% endif %}
    {% endfor%}

{% else %}
    {% do log('No relations to clean.', True) %}

{% endif %}


{% endmacro %}