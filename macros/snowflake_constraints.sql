

{#- Test if the primary key is valid -#}
{%- macro snowflake__primary_key(model, column_names, quote_columns=false) -%}

{%- set columns_csv = dbt_constraints.get_quoted_column_csv(column_names, quote_columns) %}

{#- This test will return for any duplicates and if any of the key columns is null -#}
with validation_errors as (
    select
        {{columns_csv}}
    from {{model}}
    group by {{columns_csv}}
    having count(*) > 1 
        or sum( 0
            {% for column in fk_columns_list -%}
            + case when {{column}} is null then 1 else 0 end
            {% endfor %}
        ) > 0
)

select *
from validation_errors

{%- endmacro -%}



{#- Test if the foreign key is valid -#}
{%- macro snowflake__foreign_key(model, fk_column_names, pk_table_name, pk_column_names, quote_columns=false) -%}

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



{#- This macro is used later to avoid duplicate constraints and to limit
    FK where no PK/UK constraint exists on the target -#}
{%- macro snowflake__pk_uk_constraint_exists(table_relation, pk_name, uk_name) -%}

        {%- set uk_lookup_query -%}
        SHOW UNIQUE KEYS IN TABLE {{table_relation}};
        {%- endset -%}
        {%- set existing_uk_results = run_query(uk_lookup_query) -%}
        {%- set constraint_list = existing_uk_results.columns["constraint_name"].values() -%}
        {%- if uk_name|upper in( constraint_list|map('upper') ) -%}
        {%- do log("Found UK key: " ~ table_relation ~ " " ~ uk_name, info=false) -%}
            {{ return(true) }}
        {%- endif -%}

        {%- set pk_lookup_query -%}
        SHOW PRIMARY KEYS IN TABLE {{table_relation}};
        {%- endset -%}
        {%- set existing_pk_results = run_query(pk_lookup_query) -%}
        {%- set constraint_list = existing_pk_results.columns["constraint_name"].values() -%}
        {%- if pk_name|upper in( constraint_list|map('upper') ) -%}
        {%- do log("Found PK key: " ~ table_relation ~ " " ~ pk_name, info=false) -%}
            {{ return(true) }}
        {%- endif -%}
        
        {#- If we get this far then the table does not have either constraint -#}
        {%- do log("No PK/UK key: " ~ pk_name ~ " or " ~ uk_name ~ " on " ~ table_relation, info=false) -%}
        {{ return(false) }}

{%- endmacro -%}



{%- macro snowflake__create_primary_key(table_relation, column_names, quote_columns=false) -%}

    {%- set columns_csv = dbt_constraints.get_quoted_column_csv(column_names, quote_columns) -%}
    {%- set pk_name = (table_relation.identifier ~ "_" ~ column_names|sort|join('_') ~ "_PK") | upper -%}
    {%- set uk_name = (table_relation.identifier ~ "_" ~ column_names|sort|join('_') ~ "_UK") | upper -%}

    {#- Check that the table does not already have this PK/UK -#} 
    {%- if not dbt_constraints.pk_uk_constraint_exists(table_relation, pk_name, uk_name) -%}

        {%- set query -%}
    ALTER TABLE {{table_relation}} ADD CONSTRAINT {{pk_name}} PRIMARY KEY ( {{columns_csv}} ) RELY
        {%- endset -%}
        {%- do log("Creating primary key: " ~ query, info=true) -%}
        {%- do run_query(query) -%}

    {%- else -%}            
        {%- do log("Skipping primary key because it already exists: " ~ pk_name ~ " or " ~ uk_name ~ " on " ~ table_relation, info=true) -%}
    {%- endif -%}

{%- endmacro -%}



{%- macro snowflake__create_unique_key(table_relation, column_names, quote_columns=false) -%}

    {%- set columns_csv = dbt_constraints.get_quoted_column_csv(column_names, quote_columns) -%}
    {%- set pk_name = (table_relation.identifier ~ "_" ~ column_names|sort|join('_') ~ "_PK") | upper -%}
    {%- set uk_name = (table_relation.identifier ~ "_" ~ column_names|sort|join('_') ~ "_UK") | upper -%}

    {#- Check that the table does not already have this PK/UK -#} 
    {%- if not dbt_constraints.pk_uk_constraint_exists(table_relation, pk_name, uk_name) -%}

        {%- set query -%}
        ALTER TABLE {{table_relation}} ADD CONSTRAINT {{uk_name}} UNIQUE ( {{columns_csv}} ) RELY
        {%- endset -%}
        {%- do log("Creating unique key: " ~ query, info=true) -%}
        {%- do run_query(query) -%}
    {%- else -%}            
        {%- do log("Skipping unique key because it already exists: " ~ pk_name ~ " or " ~ uk_name ~ " on " ~ table_relation, info=false) -%}
    {%- endif -%}

{%- endmacro -%}



{%- macro snowflake__create_foreign_key(test_model, pk_table_relation, pk_column_names, fk_table_relation, fk_column_names, quote_columns=false) -%}

    {#- First check if the table already has this foreign key -#} 
    {%- set pk_name = (pk_table_relation.identifier ~ "_" ~ pk_column_names|sort|join('_') ~ "_PK") | upper -%}
    {%- set uk_name = (pk_table_relation.identifier ~ "_" ~ pk_column_names|sort|join('_') ~ "_UK") | upper -%}
    {%- set fk_columns_csv = dbt_constraints.get_quoted_column_csv(fk_column_names, quote_columns) -%}
    {%- set pk_columns_csv = dbt_constraints.get_quoted_column_csv(pk_column_names, quote_columns) -%}
    {%- set constraint_name = (fk_table_relation.identifier ~ "_" ~ fk_column_names|sort|join('_') ~ "_FK") | upper -%}

    {#- Check that the PK table has a PK or UK -#}
    {%- if dbt_constraints.pk_uk_constraint_exists(pk_table_relation, pk_name, uk_name) -%}

        {%- set fk_lookup_query -%}
        SHOW IMPORTED KEYS IN TABLE {{fk_table_relation}};
        {%- endset -%}
        {%- set existing_fk_results = run_query(fk_lookup_query) -%}
        {%- set constraint_list = existing_fk_results.columns["fk_name"].values() -%}
        {%- if constraint_name not in( constraint_list ) -%}

            {%- set query -%}
            ALTER TABLE {{fk_table_relation}} ADD CONSTRAINT {{constraint_name}} FOREIGN KEY ( {{fk_columns_csv}} ) REFERENCES {{pk_table_relation}} ( {{pk_columns_csv}} ) RELY
            {%- endset -%}
            {%- do log("Creating foreign key: " ~ query, info=true) -%}
            {%- do run_query(query) -%}
                    
        {%- else -%}            
            {%- do log("Skipping foreign key because it already exists: " ~ constraint_name ~ " in " ~ constraint_list, info=false) -%}
        {%- endif -%}
    {%- else -%} 
        {%- do log("Skipping foreign key because a PK/UK was not found on the pk table: " ~ pk_name ~ " or " ~ uk_name ~ " on " ~ pk_table_relation, info=true) -%}
    {%- endif -%}

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
