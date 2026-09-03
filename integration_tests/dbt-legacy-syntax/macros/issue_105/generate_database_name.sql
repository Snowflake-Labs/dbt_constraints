{% macro generate_database_name(custom_database_name, node) -%}
    {# PostgreSQL, Oracle, and Snowflake: use target.database directly #}
    {%- if target.type == 'postgres' or target.type == 'oracle' or target.type == 'snowflake' -%}
        {{ target.database }}
    {%- elif custom_database_name is none -%}
        {{ target.database }}
    {%- elif custom_database_name == 'custom_db' -%}
        {# For testing purposes, map 'custom_db' to the target database with a prefix #}
        {%- set prefix = env_var('DBT_TEST_DB_PREFIX', target.user ~ '_') -%}
        {{ prefix ~ target.database }}
    {%- else -%}
        {{ custom_database_name }}
    {%- endif -%}
{%- endmacro %}
