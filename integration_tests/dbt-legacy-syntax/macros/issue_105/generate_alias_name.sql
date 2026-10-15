{% macro generate_alias_name(custom_alias_name, node) -%}
    {%- if custom_alias_name is none -%}
        {{ node.name }}
    {%- else -%}
        {# Replace <suffix> placeholder with environment variable or '_test' #}
        {%- set suffix_value = env_var('DBT_TEST_SUFFIX', '_test') -%}
        {{ custom_alias_name | replace('<suffix>', suffix_value) }}
    {%- endif -%}
{%- endmacro %}
