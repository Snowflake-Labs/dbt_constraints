{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {# Replace <env> placeholder with environment variable or 'dev' #}
        {%- set env_value = env_var('DBT_TEST_ENV', 'dev') -%}
        {{ custom_schema_name | replace('<env>', env_value) }}
    {%- endif -%}
{%- endmacro %}
