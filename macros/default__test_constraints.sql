{#- Test if the primary key is valid -#}
{%- macro default__test_primary_key(model, column_names, quote_columns=false) -%}
{#
NOTE: This test is designed to implement the "primary key" as specified in ANSI SQL 92 which states the following:
   "A unique constraint is satisfied if and only if no two rows in
    a table have the same non-null values in the unique columns. In
    addition, if the unique constraint was defined with PRIMARY KEY,
    then it requires that none of the values in the specified column or
    columns be the null value."
#}

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
{#
NOTE: This test is designed to implement the "unique constraint" as specified in ANSI SQL 92 which states the following:
   "A unique constraint is satisfied if and only if no two rows in
    a table have the same non-null values in the unique columns."
#}

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
{#
NOTE: This test is designed to implement the "referential constraint" as specified in ANSI SQL 92 which states the following:
   "A referential constraint is satisfied if one of the following con-
    ditions is true, depending on the <match option> specified in the
    <referential constraint definition>:

    -  If no <match type> was specified then, for each row R1 of the
        referencing table, either at least one of the values of the
        referencing columns in R1 shall be a null value, or the value of
        each referencing column in R1 shall be equal to the value of the
        corresponding referenced column in some row of the referenced
        table."

The implications of this standard is that if one column is NULL in a compound foreign key, the other column
does NOT need to match a row in a referenced unique key. This is implemented by first excluding any
rows from the test that have a NULL value in any of the columns.
#}

{%- set fk_columns_list=dbt_constraints.get_quoted_column_list(fk_column_names, quote_columns) %}
{%- set pk_columns_list=dbt_constraints.get_quoted_column_list(pk_column_names, quote_columns) %}
{%- set fk_columns_csv=dbt_constraints.get_quoted_column_csv(fk_column_names, quote_columns) %}
{%- set pk_columns_csv=dbt_constraints.get_quoted_column_csv(pk_column_names, quote_columns) %}
{%- set join_conditions = [] -%}
{%- for x in range(fk_columns_list|count) -%}
    {%- set join_conditions = join_conditions.append( 'parent.' ~ pk_columns_list[x] ~ ' = child.' ~ fk_columns_list[x] ) -%}
{%- endfor -%}

{#- This test will return if all the columns are not null
    and the values are not found in the referenced PK table #}
with child as (
  select
    {{fk_columns_csv}}
  from {{model}}
  where 1=1
        {% for column in fk_columns_list -%}
        and {{column}} is not null
        {% endfor %}
),

parent as (
    select
        {{pk_columns_csv}}
    from {{pk_table_name}}
),

validation_errors as (
    select
        child.*
    from child
    left join parent
        on {{join_conditions | join(' and ')}}

    where parent.{{pk_columns_list | first}} is null
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
