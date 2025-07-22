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

{%- set prefixed_columns_list = dbt_constraints.get_prefixed_column_list(column_names, 'pk_test', quote_columns) -%}

{#- This test will return for any duplicates and if any of the key columns is null -#}
select validation_errors.* from (
    select
        {{prefixed_columns_list | join(', ')}}, count(*) as n_records
    from {{model}} pk_test
    group by {{prefixed_columns_list | join(', ')}}
    having count(*) > 1
        {% for column in prefixed_columns_list -%}
        or {{column}} is null
        {% endfor %}
) validation_errors
{%- endmacro -%}



{#- Test if the unique key is valid -#}
{%- macro default__test_unique_key(model, column_names, quote_columns=false) -%}
{#
NOTE: This test is designed to implement the "unique constraint" as specified in ANSI SQL 92 which states the following:
   "A unique constraint is satisfied if and only if no two rows in
    a table have the same non-null values in the unique columns."
#}

{%- set prefixed_columns_list = dbt_constraints.get_prefixed_column_list(column_names, 'uk_test', quote_columns) -%}

{#- This test will return any duplicates -#}
select validation_errors.* from (
    select
        {{prefixed_columns_list | join(', ')}}, count(*) as n_records
    from {{model}} uk_test
    group by {{prefixed_columns_list | join(', ')}}
    having count(*) > 1
) validation_errors
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

{%- set fk_columns_inner_list=dbt_constraints.get_prefixed_column_list(fk_column_names, 'fk_child_inner', quote_columns) %}
{%- set pk_columns_inner_list=dbt_constraints.get_prefixed_column_list(pk_column_names, 'fk_parent_inner', quote_columns) %}
{%- set pk_columns_outer_list=dbt_constraints.get_prefixed_column_list(pk_column_names, 'fk_parent', quote_columns) %}
{%- set join_conditions = dbt_constraints.get_join_conditions(fk_column_names, 'fk_child', pk_column_names, 'fk_parent', quote_columns) -%}

{#- This test will return if all the columns are not null
    and the values are not found in the referenced PK table #}

select validation_errors.* from (
    select
        fk_child.*
    from (
        select
            {{ fk_columns_inner_list | join(', ') }}
        from {{model}} fk_child_inner
        where 1=1
            {% for column in fk_columns_inner_list -%}
            and {{column}} is not null
            {% endfor -%}
        ) fk_child
    left join (
        select
            {{ pk_columns_inner_list | join(', ') }}
        from {{pk_table_name}} fk_parent_inner
        ) fk_parent
            on {{ join_conditions | join(' and ') }}

    where {{ pk_columns_outer_list | first }} is null
) validation_errors
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


{%- macro get_prefixed_column_list(column_array, prefix_alias, quote_columns=false) -%}
    {%- set column_list = dbt_constraints.get_quoted_column_list(column_array, quote_columns) -%}

    {%- set prefixed_column_list = [] -%}
    {%- for x in range(column_list|count) -%}
        {%- set prefixed_column_list = prefixed_column_list.append( prefix_alias ~ '.' ~ column_list[x] ) -%}
    {%- endfor -%}
    {{ return(prefixed_column_list) }}
{%- endmacro -%}


{%- macro get_join_conditions(column_array_left, prefix_alias_left, column_array_right, prefix_alias_right, quote_columns=false) -%}
    {%- set column_list_left = dbt_constraints.get_prefixed_column_list(column_array_left, prefix_alias_left, quote_columns) -%}
    {%- set column_list_right = dbt_constraints.get_prefixed_column_list(column_array_right, prefix_alias_right, quote_columns) -%}

    {%- set join_conditions = [] -%}
    {%- for x in range(column_list_left|count) -%}
        {%- set join_conditions = join_conditions.append( column_list_left[x] ~ ' = ' ~ column_list_right[x] ) -%}
    {%- endfor -%}
    {{ return(join_conditions) }}
{%- endmacro -%}
