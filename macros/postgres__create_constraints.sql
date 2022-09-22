{# PostgreSQL specific implementation to create a primary key #}
{%- macro postgres__create_primary_key(table_relation, column_names, verify_permissions, quote_columns=false) -%}
    {%- set constraint_name = (table_relation.identifier ~ "_" ~ column_names|join('_') ~ "_PK") | upper -%}

    {%- if constraint_name|length > 63 %}
        {%- set constraint_name_query %}
        select  'PK_' || md5( '{{ constraint_name }}' )::varchar as "constraint_name"
        {%- endset -%}
        {%- set results = run_query(constraint_name_query) -%}
        {%- set constraint_name = results.columns[0].values()[0] -%}
    {% endif %}

    {%- set columns_csv = dbt_constraints.get_quoted_column_csv(column_names, quote_columns) -%}

    {#- Check that the table does not already have this PK/UK -#}
    {%- if not dbt_constraints.unique_constraint_exists(table_relation, column_names) -%}

        {%- if dbt_constraints.have_ownership_priv(table_relation, verify_permissions) -%}

            {%- do log("Creating primary key: " ~ constraint_name, info=true) -%}
            {%- call statement('add_pk', fetch_result=False, auto_begin=True) -%}
            ALTER TABLE {{table_relation}} ADD CONSTRAINT {{constraint_name}} PRIMARY KEY ( {{columns_csv}} )
            {%- endcall -%}
            {{ adapter.commit() }}

        {%- else -%}
            {%- do log("Skipping " ~ constraint_name ~ " because of insufficient privileges: " ~ table_relation, info=false) -%}
        {%- endif -%}

    {%- else -%}
        {%- do log("Skipping " ~ constraint_name ~ " because PK/UK already exists: " ~ table_relation ~ " " ~ column_names, info=false) -%}
    {%- endif -%}

{%- endmacro -%}



{# PostgreSQL specific implementation to create a unique key #}
{%- macro postgres__create_unique_key(table_relation, column_names, verify_permissions, quote_columns=false) -%}
    {%- set constraint_name = (table_relation.identifier ~ "_" ~ column_names|join('_') ~ "_UK") | upper -%}

    {%- if constraint_name|length > 63 %}
        {%- set constraint_name_query %}
        select  'UK_' || md5( '{{ constraint_name }}' )::varchar as "constraint_name"
        {%- endset -%}
        {%- set results = run_query(constraint_name_query) -%}
        {%- set constraint_name = results.columns[0].values()[0] -%}
    {% endif %}

    {%- set columns_csv = dbt_constraints.get_quoted_column_csv(column_names, quote_columns) -%}

    {#- Check that the table does not already have this PK/UK -#}
    {%- if not dbt_constraints.unique_constraint_exists(table_relation, column_names) -%}

        {%- if dbt_constraints.have_ownership_priv(table_relation, verify_permissions) -%}

            {%- do log("Creating unique key: " ~ constraint_name, info=true) -%}
            {%- call statement('add_uk', fetch_result=False, auto_begin=True) -%}
            ALTER TABLE {{table_relation}} ADD CONSTRAINT {{constraint_name}} UNIQUE ( {{columns_csv}} )
            {%- endcall -%}
            {{ adapter.commit() }}

        {%- else -%}
            {%- do log("Skipping " ~ constraint_name ~ " because of insufficient privileges: " ~ table_relation, info=false) -%}
        {%- endif -%}

    {%- else -%}
        {%- do log("Skipping " ~ constraint_name ~ " because PK/UK already exists: " ~ table_relation ~ " " ~ column_names, info=false) -%}
    {%- endif -%}

{%- endmacro -%}

{# PostgreSQL specific implementation to create a not null constraint #}
{%- macro postgres__create_not_null(table_relation, column_names, verify_permissions, quote_columns=false) -%}
    {%- set columns_list = dbt_constraints.get_quoted_column_list(column_names, quote_columns) -%}

    {%- if dbt_constraints.have_ownership_priv(table_relation, verify_permissions) -%}

            {%- set modify_statements= [] -%}
            {%- for column in columns_list -%}
                {%- set modify_statements = modify_statements.append( "ALTER COLUMN " ~ column ~ " SET NOT NULL" ) -%}
            {%- endfor -%}
            {%- set modify_statement_csv = modify_statements | join(", ") -%}
            {%- do log("Creating not null constraint for: " ~ columns_list | join(", ") ~ " in " ~ table_relation, info=true) -%}
            {%- call statement('add_nn', fetch_result=False, auto_begin=True) -%}
                ALTER TABLE {{table_relation}} {{ modify_statement_csv }};
            {%- endcall -%}
            {{ adapter.commit() }}

    {%- else -%}
        {%- do log("Skipping not null constraint for " ~ columns_list | join(", ") ~ " in " ~ table_relation ~ " because of insufficient privileges: " ~ table_relation, info=true) -%}
    {%- endif -%}
{%- endmacro -%}

{# PostgreSQL specific implementation to create a foreign key #}
{%- macro postgres__create_foreign_key(pk_table_relation, pk_column_names, fk_table_relation, fk_column_names, verify_permissions, quote_columns=true) -%}
    {%- set constraint_name = (fk_table_relation.identifier ~ "_" ~ fk_column_names|join('_') ~ "_FK") | upper -%}

    {%- if constraint_name|length > 63 %}
        {%- set constraint_name_query %}
        select  'FK_' || md5( '{{ constraint_name }}' )::varchar as "constraint_name"
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

                {%- do log("Creating foreign key: " ~ constraint_name ~ " referencing " ~ pk_table_relation.identifier ~ " " ~ pk_column_names, info=true) -%}
                {%- call statement('add_fk', fetch_result=False, auto_begin=True) -%}
                ALTER TABLE {{fk_table_relation}} ADD CONSTRAINT {{constraint_name}} FOREIGN KEY ( {{fk_columns_csv}} ) REFERENCES {{pk_table_relation}} ( {{pk_columns_csv}} ) ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED
                {%- endcall -%}
                {{ adapter.commit() }}

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
{%- macro postgres__unique_constraint_exists(table_relation, column_names) -%}
    {%- set lookup_query -%}
    select c.oid as constraint_name
        , upper(col.attname) as column_name
    from pg_constraint c
    cross join lateral unnest(c.conkey) as con(conkey)
    join pg_class tbl on tbl.oid = c.conrelid
    join pg_namespace ns on ns.oid = tbl.relnamespace
    join pg_attribute col on (col.attrelid = tbl.oid
                            and col.attnum = con.conkey)
    where c.contype in ('p', 'u')
    and ns.nspname ilike '{{table_relation.schema}}'
    and tbl.relname ilike '{{table_relation.identifier}}'
    order by constraint_name
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
{%- macro postgres__foreign_key_exists(table_relation, column_names) -%}
    {%- set lookup_query -%}
    select c.oid as fk_name
        , upper(col.attname) as fk_column_name
    from pg_constraint c
    cross join lateral unnest(c.conkey) as con(conkey)
    join pg_class tbl on tbl.oid = c.conrelid
    join pg_namespace ns on ns.oid = tbl.relnamespace
    join pg_attribute col on (col.attrelid = tbl.oid
                            and col.attnum = con.conkey)
    where c.contype in ('f')
    and ns.nspname ilike '{{table_relation.schema}}'
    and tbl.relname ilike '{{table_relation.identifier}}'
    order by fk_name
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


{%- macro postgres__have_references_priv(table_relation, verify_permissions) -%}
    {%- if verify_permissions is sameas true -%}

        {%- set lookup_query -%}
        select case when count(*) > 0 then 'y' else 'n' end as "have_references"
        from information_schema.table_privileges t
        join information_schema.enabled_roles er on t.grantee = er.role_name
        where upper(t.table_schema) = upper('{{table_relation.schema}}')
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


{%- macro postgres__have_ownership_priv(table_relation, verify_permissions) -%}
    {%- if verify_permissions is sameas true -%}

        {%- set lookup_query -%}
        select case when count(*) > 0 then 'y' else 'n' end as "have_ownership"
        from pg_catalog.pg_tables t
        join information_schema.enabled_roles er on t.tableowner = er.role_name
        where upper(t.schemaname) = upper('{{table_relation.schema}}')
        and upper(t.tablename) = upper('{{table_relation.identifier}}')
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


{% macro postgres__drop_referential_constraints(relation) -%}
    {%- set lookup_query -%}
    select constraint_name
    from information_schema.table_constraints
    where table_schema = '{{relation.schema}}'
    and table_name='{{relation.identifier}}'
    and constraint_type in ('FOREIGN KEY', 'PRIMARY KEY', 'UNIQUE')
    {%- endset -%}
    {%- set constraint_list = run_query(lookup_query) -%}

    {%- for constraint_name in constraint_list.columns["constraint_name"].values() -%}
        {%- do log("Dropping constraint: " ~ constraint_name ~ " from table " ~ relation, info=false) -%}
        {%- call statement('drop_constraint_cascade', fetch_result=False, auto_begin=True) -%}
        ALTER TABLE {{relation}} DROP CONSTRAINT IF EXISTS "{{constraint_name}}" CASCADE
        {%- endcall -%}
        {{ adapter.commit() }}
    {% endfor %}

{% endmacro %}

{#- PostgreSQL will error if you try to truncate tables with FK constraints or tables with PK/UK constraints
    referenced by FK so we will drop all constraints before truncating tables -#}
{% macro postgres__truncate_relation(relation) -%}
    {{ postgres__drop_referential_constraints(relation) }}
    {{ return(adapter.dispatch('truncate_relation', 'dbt')(relation)) }}
{% endmacro %}

{#- PostgreSQL will get deadlocks if you try to drop tables with FK constraints or tables with PK/UK constraints
    referenced by FK so we will drop all constraints before dropping tables -#}
{% macro postgres__drop_relation(relation) -%}
    {{ postgres__drop_referential_constraints(relation) }}
    {{ return(adapter.dispatch('drop_relation', 'dbt')(relation)) }}
{% endmacro %}
