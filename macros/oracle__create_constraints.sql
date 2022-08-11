{# Oracle specific implementation to create a primary key #}
{%- macro oracle__create_primary_key(table_relation, column_names, verify_permissions, quote_columns=false) -%}
    {%- set constraint_name = (table_relation.identifier ~ "_" ~ column_names|join('_') ~ "_PK") | upper -%}

    {%- if constraint_name|length > 30 %}
        {%- set constraint_name_query %}
        select  'PK_' ||  ora_hash( '{{ constraint_name }}' ) as "constraint_name" from dual
        {%- endset -%}
        {%- set results = run_query(constraint_name_query) -%}
        {%- set constraint_name = results.columns[0].values()[0] -%}
    {% endif %}

    {%- set columns_csv = dbt_constraints.get_quoted_column_csv(column_names, quote_columns) -%}

    {#- Check that the table does not already have this PK/UK -#}
    {%- if not dbt_constraints.unique_constraint_exists(table_relation, column_names) -%}

        {%- if dbt_constraints.have_ownership_priv(table_relation, verify_permissions) -%}

            {%- set query -%}
BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE {{table_relation}} ADD CONSTRAINT {{constraint_name}} PRIMARY KEY ( {{columns_csv}} )';
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.ENABLE(BUFFER_SIZE => NULL);
        DBMS_OUTPUT.PUT_LINE('Unable to create constraint: ' || SQLERRM);
END;
            {%- endset -%}
            {%- do log("Creating primary key: " ~ constraint_name, info=true) -%}
            {%- do run_query(query) -%}

        {%- else -%}
            {%- do log("Skipping " ~ constraint_name ~ " because of insufficient privileges: " ~ table_relation, info=false) -%}
        {%- endif -%}

    {%- else -%}
        {%- do log("Skipping " ~ constraint_name ~ " because PK/UK already exists: " ~ table_relation ~ " " ~ column_names, info=false) -%}
    {%- endif -%}

{%- endmacro -%}



{# Oracle specific implementation to create a unique key #}
{%- macro oracle__create_unique_key(table_relation, column_names, verify_permissions, quote_columns=false) -%}
    {%- set constraint_name = (table_relation.identifier ~ "_" ~ column_names|join('_') ~ "_UK") | upper -%}

    {%- if constraint_name|length > 30 %}
        {%- set constraint_name_query %}
        select  'UK_' || ora_hash( '{{ constraint_name }}' ) as "constraint_name" from dual
        {%- endset -%}
        {%- set results = run_query(constraint_name_query) -%}
        {%- set constraint_name = results.columns[0].values()[0] -%}
    {% endif %}

    {%- set columns_csv = dbt_constraints.get_quoted_column_csv(column_names, quote_columns) -%}

    {#- Check that the table does not already have this PK/UK -#}
    {%- if not dbt_constraints.unique_constraint_exists(table_relation, column_names) -%}

        {%- if dbt_constraints.have_ownership_priv(table_relation, verify_permissions) -%}

            {%- set query -%}
BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE {{table_relation}} ADD CONSTRAINT {{constraint_name}} UNIQUE ( {{columns_csv}} )';
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.ENABLE(BUFFER_SIZE => NULL);
        DBMS_OUTPUT.PUT_LINE('Unable to create constraint: ' || SQLERRM);
END;
            {%- endset -%}
            {%- do log("Creating unique key: " ~ constraint_name, info=true) -%}
            {%- do run_query(query) -%}

        {%- else -%}
            {%- do log("Skipping " ~ constraint_name ~ " because of insufficient privileges: " ~ table_relation, info=false) -%}
        {%- endif -%}

    {%- else -%}
        {%- do log("Skipping " ~ constraint_name ~ " because PK/UK already exists: " ~ table_relation ~ " " ~ column_names, info=false) -%}
    {%- endif -%}

{%- endmacro -%}



{# Oracle specific implementation to create a foreign key #}
{%- macro oracle__create_foreign_key(pk_table_relation, pk_column_names, fk_table_relation, fk_column_names, verify_permissions, quote_columns=true) -%}
    {%- set constraint_name = (fk_table_relation.identifier ~ "_" ~ fk_column_names|join('_') ~ "_FK") | upper -%}

    {%- if constraint_name|length > 30 %}
        {%- set constraint_name_query %}
        select  'FK_' || ora_hash( '{{ constraint_name }}' ) as "constraint_name" from dual
        {%- endset -%}
        {%- set results = run_query(constraint_name_query) -%}
        {%- set constraint_name = results.columns[0].values()[0] -%}
    {% endif %}

    {%- set fk_columns_csv = dbt_constraints.get_quoted_column_csv(fk_column_names, quote_columns) -%}
    {%- set pk_columns_csv = dbt_constraints.get_quoted_column_csv(pk_column_names, quote_columns) -%}
    {#- Check that the PK table has a PK or UK -#}
    {%- if dbt_constraints.unique_constraint_exists(pk_table_relation, pk_column_names) -%}
        {#- Check if the table already has this foreign key -#}
        {%- if not dbt_constraints.foreign_key_exists(fk_table_relation, fk_column_names) -%}

            {%- if dbt_constraints.have_ownership_priv(fk_table_relation, verify_permissions) and dbt_constraints.have_references_priv(pk_table_relation, verify_permissions) -%}

                {%- set query -%}
BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE {{fk_table_relation}} ADD CONSTRAINT {{constraint_name}} FOREIGN KEY ( {{fk_columns_csv}} ) REFERENCES {{pk_table_relation}} ( {{pk_columns_csv}} )';
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.ENABLE(BUFFER_SIZE => NULL);
        DBMS_OUTPUT.PUT_LINE('Unable to create constraint: ' || SQLERRM);
END;
                {%- endset -%}
                {%- do log("Creating foreign key: " ~ constraint_name ~ " referencing " ~ pk_table_relation.identifier ~ " " ~ pk_column_names, info=true) -%}
                {%- do run_query(query) -%}

            {%- else -%}
                {%- do log("Skipping " ~ constraint_name ~ " because of insufficient privileges: " ~ fk_table_relation ~ " referencing " ~ pk_table_relation, info=true) -%}
            {%- endif -%}

        {%- else -%}
            {%- do log("Skipping " ~ constraint_name ~ " because FK already exists: " ~ fk_table_relation ~ " " ~ fk_column_names, info=false) -%}
        {%- endif -%}
    {%- else -%}
        {%- do log("Skipping " ~ constraint_name ~ " because a PK/UK was not found on the PK table: " ~ pk_table_relation ~ " " ~ pk_column_names, info=true) -%}
    {%- endif -%}

{%- endmacro -%}



{#- This macro is used in create macros to avoid duplicate PK/UK constraints
    and to skip FK where no PK/UK constraint exists on the parent table -#}
{%- macro oracle__unique_constraint_exists(table_relation, column_names) -%}
    {%- set lookup_query -%}
select
    cols.constraint_name as "constraint_name",
    upper(cols.column_name) as "column_name"
from
         all_constraints cons
    join all_cons_columns cols on cons.constraint_name = cols.constraint_name
                                  and cons.owner = cols.owner
where
    cons.constraint_type in ( 'P', 'U' )
    and upper(cons.owner) = upper('{{table_relation.schema}}')
    and upper(cons.table_name) = upper('{{table_relation.identifier}}')
order by 1, 2
    {%- endset -%}
    {%- do log("Lookup: " ~ lookup_query, info=false) -%}
    {%- set constraint_list = run_query(lookup_query) -%}
    {%- if constraint_list.columns["column_name"].values() | count > 0 -%}
        {%- for constraint in constraint_list.group_by("constraint_name") -%}
            {%- if dbt_constraints.column_list_matches(constraint.columns["column_name"].values(), column_names ) -%}
                {%- do log("Found PK/UK key: " ~ table_relation ~ " " ~ column_names, info=false) -%}
                {{ return(true) }}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}#}

    {#- If we get this far then the table does not have either constraint -#}
    {%- do log("No PK/UK key: " ~ table_relation ~ " " ~ column_names, info=false) -%}
    {{ return(false) }}
{%- endmacro -%}



{#- This macro is used in create macros to avoid duplicate FK constraints -#}
{%- macro oracle__foreign_key_exists(table_relation, column_names) -%}
    {%- set lookup_query -%}
select
    cols.constraint_name as "fk_name",
    upper(cols.column_name) as "fk_column_name"
from
         all_constraints cons
    join all_cons_columns cols on cons.constraint_name = cols.constraint_name
                                  and cons.owner = cols.owner
where
    cons.constraint_type in ( 'R' )
    and upper(cons.owner) = upper('{{table_relation.schema}}')
    and upper(cons.table_name) = upper('{{table_relation.identifier}}')
order by 1, 2
    {%- endset -%}
    {%- do log("Lookup: " ~ lookup_query, info=false) -%}
    {%- set constraint_list = run_query(lookup_query) -%}
    {%- if constraint_list.columns["fk_column_name"].values() | count > 0 -%}
        {%- for constraint in constraint_list.group_by("fk_name") -%}
            {%- if dbt_constraints.column_list_matches(constraint.columns["fk_column_name"].values(), column_names ) -%}
                {%- do log("Found FK key: " ~ table_relation ~ " " ~ column_names, info=false) -%}
                {{ return(true) }}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}

    {#- If we get this far then the table does not have this constraint -#}
    {%- do log("No FK key: " ~ table_relation ~ " " ~ column_names, info=false) -%}
    {{ return(false) }}
{%- endmacro -%}

{#- Oracle lacks a simple way to verify privileges so we will instead use an exception handler -#}
{%- macro oracle__have_references_priv(table_relation, verify_permissions) -%}
    {{ return(true) }}
{%- endmacro -%}

{#- Oracle lacks a simple way to verify privileges so we will instead use an exception handler -#}
{%- macro oracle__have_ownership_priv(table_relation, verify_permissions) -%}
    {{ return(true) }}
{%- endmacro -%}
