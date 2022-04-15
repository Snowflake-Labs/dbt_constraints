{#- Test if the primary key is valid -#}
{%- macro default__test_primary_key(model, column_names, quote_columns=false) -%}

{%- set columns_csv = dbt_constraints.get_quoted_column_csv(column_names, quote_columns) %}

{#- This test will return for any duplicates and if any of the key columns is null -#}
with validation_errors as (
    select
        {{columns_csv}}, count(*)
    from {{model}}
    group by {{columns_csv}}
    having count(*) > 1
        {% for column in column_names -%}
        or {{column}} is null
        {% endfor %}
)

select *
from validation_errors

{%- endmacro -%}



{#- Test if the unique key is valid -#}
{%- macro default__test_unique_key(model, column_names, quote_columns=false) -%}

{%- set columns_csv = dbt_constraints.get_quoted_column_csv(column_names, quote_columns) %}

{#- This test will return any duplicates -#}
with validation_errors as (
    select
        {{columns_csv}}
    from {{model}}
    group by {{columns_csv}}
    having count(*) > 1
)

select *
from validation_errors

{%- endmacro -%}



{#- Test if the foreign key is valid -#}
{%- macro default__test_foreign_key(model, fk_column_names, pk_table_name, pk_column_names, quote_columns=false) -%}

{%- set fk_columns_list=dbt_constraints.get_quoted_column_list(fk_column_names, quote_columns) %}
{%- set pk_columns_list=dbt_constraints.get_quoted_column_list(pk_column_names, quote_columns) %}
{%- set fk_columns_csv=dbt_constraints.get_quoted_column_csv(fk_column_names, quote_columns) %}
{%- set pk_columns_csv=dbt_constraints.get_quoted_column_csv(pk_column_names, quote_columns) %}

{#- This test will return if all the columns are not null
    and the values are not found in the referenced PK table -#}
with fk_table as (
  select
    {{fk_columns_csv}}
  from {{model}}
  where 1=1
        {% for column in fk_columns_list -%}
        and {{column}} is not null
        {% endfor %}
),

pk_table as (
    select
        {{pk_columns_csv}}
    from {{pk_table_name}}
    where 1=1
        {% for column in pk_columns_list -%}
        and {{column}} is not null
        {% endfor %}
),

validation_errors as (
    select
        {{fk_columns_csv}}
    from fk_table
    where ( {{fk_columns_csv}} )
        not in (
            select {{pk_columns_csv}}
            from pk_table
        )
)
select *
from validation_errors

{%- endmacro -%}


{%- macro get_quoted_column_list(column_array, quote_columns=false) -%}

    {%- if not quote_columns -%}
        {%- set column_list=column_array -%}
    {%- elif quote_columns -%}
        {%- set column_list=[] -%}
        {%- for column in column_array -%}
            {%- set column_list = column_list.append( adapter.quote(column) ) -%}
        {%- endfor -%}
    {%- else -%}
        {{ exceptions.raise_compiler_error(
            "`quote_columns` argument must be one of [True, False] Got: '" ~ quote ~"'.'"
        ) }}
    {%- endif -%}

    {{ return(column_list) }}

{%- endmacro -%}


{%- macro get_quoted_column_csv(column_array, quote_columns=false) -%}

    {%- set column_list = dbt_constraints.get_quoted_column_list(column_array, quote_columns) -%}
    {%- set columns_csv=column_list | join(', ') -%}
    {{ return(columns_csv) }}

{%- endmacro -%}
