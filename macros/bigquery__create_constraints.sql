{# Bigquery specific implementation to create a primary key #}
{%- macro bigquery__create_primary_key(table_relation, column_names, verify_permissions, quote_columns, constraint_name, lookup_cache, rely_clause) -%}
    {%- set constraint_name = (constraint_name or table_relation.identifier ~ "_" ~ column_names|join('_') ~ "_PK") | upper -%}
    {%- set columns_csv = dbt_constraints.get_quoted_column_csv(column_names, quote_columns) -%}

    {#- Check that the table does not already have this PK/UK -#}
    {%- if not dbt_constraints.unique_constraint_exists(table_relation, column_names, lookup_cache) -%}

        {%- if dbt_constraints.have_ownership_priv(table_relation, verify_permissions, lookup_cache) -%}

            {%- do log("Creating primary key: " ~ constraint_name, info=true) -%}
            {%- set query -%}
            ALTER TABLE {{ table_relation }} ADD PRIMARY KEY ( {{ columns_csv }} ) NOT ENFORCED
            {%- endset -%}
            {%- do run_query(query) -%}

        {%- else -%}
            {%- do log("Skipping " ~ constraint_name ~ " because of insufficient privileges: " ~ table_relation, info=false) -%}
        {%- endif -%}

    {%- else -%}
        {%- do log("Skipping " ~ constraint_name ~ " because PK/UK already exists: " ~ table_relation ~ " " ~ column_names, info=false) -%}
    {%- endif -%}

{%- endmacro -%}

{# Bigquery specific implementation to create a unique key #}
{%- macro bigquery__create_unique_key(table_relation, column_names, verify_permissions, quote_columns, constraint_name, lookup_cache, rely_clause) -%}
    {%- set columns_list = dbt_constraints.get_quoted_column_list(column_names, quote_columns) -%}

    {%- do log("Skipping unique constraint for " ~ columns_list | join(", ") ~ " in " ~ table_relation ~ " because ALTER COLUMN SET UNIQUE is not supported", info=true) -%}
{%- endmacro -%}

{# Bigquery specific implementation to create a not null constraint #}
{%- macro bigquery__create_not_null(table_relation, column_names, verify_permissions, quote_columns, lookup_cache, rely_clause) -%}
    {%- set columns_list = dbt_constraints.get_quoted_column_list(column_names, quote_columns) -%}

    {%- do log("Skipping not null constraint for " ~ columns_list | join(", ") ~ " in " ~ table_relation ~ " because ALTER COLUMN SET NOT NULL is not supported", info=true) -%}
{%- endmacro -%}

{# Bigquery specific implementation to create a foreign key #}
{%- macro bigquery__create_foreign_key(pk_table_relation, pk_column_names, fk_table_relation, fk_column_names, verify_permissions, quote_columns, constraint_name, lookup_cache, rely_clause) -%}
    {%- set constraint_name = (constraint_name or fk_table_relation.identifier ~ "_" ~ fk_column_names|join('_') ~ "_FK") | upper -%}

    {%- set fk_columns_csv = dbt_constraints.get_quoted_column_csv(fk_column_names, quote_columns) -%}
    {%- set pk_columns_csv = dbt_constraints.get_quoted_column_csv(pk_column_names, quote_columns) -%}
    {#- Check that the PK table has a PK or UK -#}
    {%- if dbt_constraints.unique_constraint_exists(pk_table_relation, pk_column_names, lookup_cache) -%}
        {#- Check if the table already has this foreign key -#}
        {%- if not dbt_constraints.foreign_key_exists(fk_table_relation, fk_column_names) -%}

            {%- if dbt_constraints.have_ownership_priv(fk_table_relation, verify_permissions, lookup_cache) and dbt_constraints.have_references_priv(pk_table_relation, verify_permissions, lookup_cache) -%}

                {%- do log("Creating foreign key: " ~ constraint_name ~ " referencing " ~ pk_table_relation.identifier ~ " " ~ pk_column_names, info=true) -%}
                {%- set query -%}
                --Note: ON DELETE not supported in Bigquery
                ALTER TABLE {{ fk_table_relation }} ADD FOREIGN KEY({{ fk_columns_csv }} ) {{ pk_table_relation }}( {{ pk_columns_csv }} ) NOT ENFORCED
                 {%- endset -%}
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
{%- macro bigquery__unique_constraint_exists(table_relation, column_names, lookup_cache) -%}
    {%- set lookup_query -%}
    SELECT
        kc.constraint_name
        , lower(kc.column_name) as column_name
    FROM {{table_relation.schema}}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kc
        JOIN {{table_relation.schema}}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            ON kc.table_name = tc.table_name
                AND kc.table_schema = tc.table_schema
                AND kc.constraint_name = tc.constraint_name
    WHERE tc.constraint_type in ('PRIMARY KEY', 'UNIQUE')
        AND kc.table_schema like '{{table_relation.schema}}'
        AND kc.table_name like '{{table_relation.identifier}}'
    order by kc.constraint_name
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
{%- macro bigquery__foreign_key_exists(table_relation, column_names, lookup_cache) -%}
    {%- set lookup_query -%}
    SELECT
        kc.constraint_name fk_name
        , lower(kc.column_name) as fk_column_name
    FROM {{table_relation.schema}}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kc
        JOIN {{table_relation.schema}}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            ON kc.table_name = tc.table_name
                AND kc.table_schema = tc.table_schema
                AND kc.constraint_name = tc.constraint_name
    WHERE tc.constraint_type='FOREIGN KEY'
        AND kc.table_schema like '{{table_relation.schema}}'
        AND kc.table_name like '{{table_relation.identifier}}'
    order by kc.constraint_name
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


{%- macro bigquery__have_references_priv(table_relation, verify_permissions, lookup_cache) -%}
    {%- if verify_permissions is sameas true -%}
        {% set rel_location = adapter.get_dataset_location(table_relation) %}

        {%- set lookup_query -%}
        with union_obj_priv as (
            -- Union together all project priveleges & table.  Specific to BQ permissions inherritence.
            select * 
            from {{table_relation.database}}.`region-{{ rel_location }}`.INFORMATION_SCHEMA.OBJECT_PRIVILEGES
            where (upper(object_schema) = upper('{{table_relation.schema}}') and upper(object_name) = upper('{{table_relation.identifier}}')) 
            union all
            select * 
            from {{table_relation.database}}.`region-{{ rel_location }}`.INFORMATION_SCHEMA.OBJECT_PRIVILEGES
            where object_name= '{{table_relation.schema}}'
        )
        select case when count(*) > 0 then 'y' else 'n' end as have_references
        from union_obj_priv
        where upper(t.table_schema) = upper('')
            and upper(t.table_name) = upper('{{table_relation.identifier}}')
        {%- endset -%}
        {%- do log("Lookup: " ~ lookup_query, info=false) -%}
        {%- set results = run_query(lookup_query) -%}
        {%- if "y" in( results.columns["have_references"].values() ) -%}
            {{ return(true) }}
        {%- endif -%}

        {{ return(false) }}
    {%- else -%}
        {{ return(true) }}
    {%- endif -%}
{%- endmacro -%}


{%- macro bigquery__have_ownership_priv(table_relation, verify_permissions, lookup_cache) -%}
    {%- if verify_permissions is sameas true -%}

        {%- set lookup_query -%}
        with union_obj_priv as (
            -- Union together all project priveleges & table.  Specific to BQ permissions inherritence.
            select * 
            from {{table_relation.database}}.`region-{{ rel_location }}`.INFORMATION_SCHEMA.OBJECT_PRIVILEGES
            where (upper(object_schema) = upper('{{table_relation.schema}}') and upper(object_name) = upper('{{table_relation.identifier}}')) 
            union all
            select * 
            from {{table_relation.database}}.`region-{{ rel_location }}`.INFORMATION_SCHEMA.OBJECT_PRIVILEGES
            where object_name= '{{table_relation.schema}}'
        )
        select case when count(*) > 0 then 'y' else 'n' end as "have_ownership"
        from union_obj_priv
        {%- endset -%}
        {%- do log("Lookup: " ~ lookup_query, info=false) -%}
        {%- set results = run_query(lookup_query) -%}
        {%- if "y" in( results.columns["have_ownership"].values() ) -%}
            {{ return(true) }}
        {%- endif -%}

        {{ return(false) }}
    {%- else -%}
        {{ return(true) }}
    {%- endif -%}
{%- endmacro -%}


{% macro bigquery__drop_referential_constraints(relation) -%}
    {%- set lookup_query -%}
    select constraint_name
    from {{table_relation.schema}}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS
    where table_schema = '{{relation.schema}}'
    and table_name='{{relation.identifier}}'
    and constraint_type in ('FOREIGN KEY', 'PRIMARY KEY', 'UNIQUE')
    {%- endset -%}
    {%- set constraint_list = run_query(lookup_query) -%}

    {%- for constraint_name in constraint_list.columns["constraint_name"].values() -%}
        {%- do log("Dropping constraint: " ~ constraint_name ~ " from table " ~ relation, info=false) -%}
        {%- set query -%}
        ALTER TABLE {{relation}} DROP CONSTRAINT "{{constraint_name}}"
        {%- endset -%}
        {%- do run_query(query) -%}
    {% endfor %}

{% endmacro %}

{#- Bigquery will error if you try to truncate tables with FK constraints or tables with PK/UK constraints
    referenced by FK so we will drop all constraints before truncating tables -#}
{% macro bigquery__truncate_relation(relation) -%}
    {{ bigquery__drop_referential_constraints(relation) }}
    {{ return(adapter.dispatch('truncate_relation', 'dbt')(relation)) }}
{% endmacro %}

{#- Bigquery will get deadlocks if you try to drop tables with FK constraints or tables with PK/UK constraints
    referenced by FK so we will drop all constraints before dropping tables -#}
{% macro bigquery__drop_relation(relation) -%}
    {{ bigquery__drop_referential_constraints(relation) }}
    {{ return(adapter.dispatch('drop_relation', 'dbt')(relation)) }}
{% endmacro %}
