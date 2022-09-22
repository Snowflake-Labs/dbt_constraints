{# Snowflake specific implementation to create a primary key #}
{%- macro snowflake__create_primary_key(table_relation, column_names, verify_permissions, quote_columns=false) -%}
    {%- set constraint_name = (table_relation.identifier ~ "_" ~ column_names|join('_') ~ "_PK") | upper -%}
    {%- set columns_csv = dbt_constraints.get_quoted_column_csv(column_names, quote_columns) -%}

    {#- Check that the table does not already have this PK/UK -#}
    {%- if not dbt_constraints.unique_constraint_exists(table_relation, column_names) -%}

        {%- if dbt_constraints.have_ownership_priv(table_relation, verify_permissions) -%}

            {%- set query -%}
            ALTER TABLE {{table_relation}} ADD CONSTRAINT {{constraint_name}} PRIMARY KEY ( {{columns_csv}} ) RELY
            {%- endset -%}
            {%- do log("Creating primary key: " ~ constraint_name, info=true) -%}
            {%- do run_query(query) -%}

        {%- else -%}
            {%- do log("Skipping " ~ constraint_name ~ " because of insufficient privileges: " ~ table_relation, info=true) -%}
        {%- endif -%}

    {%- else -%}
        {%- do log("Skipping " ~ constraint_name ~ " because PK/UK already exists: " ~ table_relation ~ " " ~ column_names, info=false) -%}
    {%- endif -%}

{%- endmacro -%}




{# Snowflake specific implementation to create a unique key #}
{%- macro snowflake__create_unique_key(table_relation, column_names, verify_permissions, quote_columns=false) -%}
    {%- set constraint_name = (table_relation.identifier ~ "_" ~ column_names|join('_') ~ "_UK") | upper -%}
    {%- set columns_csv = dbt_constraints.get_quoted_column_csv(column_names, quote_columns) -%}

    {#- Check that the table does not already have this PK/UK -#}
    {%- if not dbt_constraints.unique_constraint_exists(table_relation, column_names) -%}

        {%- if dbt_constraints.have_ownership_priv(table_relation, verify_permissions) -%}

            {%- set query -%}
            ALTER TABLE {{table_relation}} ADD CONSTRAINT {{constraint_name}} UNIQUE ( {{columns_csv}} ) RELY
            {%- endset -%}
            {%- do log("Creating unique key: " ~ constraint_name, info=true) -%}
            {%- do run_query(query) -%}

        {%- else -%}
            {%- do log("Skipping " ~ constraint_name ~ " because of insufficient privileges: " ~ table_relation, info=true) -%}
        {%- endif -%}

    {%- else -%}
        {%- do log("Skipping " ~ constraint_name ~ " because PK/UK already exists: " ~ table_relation ~ " " ~ column_names, info=false) -%}
    {%- endif -%}

{%- endmacro -%}



{# Snowflake specific implementation to create a foreign key #}
{%- macro snowflake__create_foreign_key(pk_table_relation, pk_column_names, fk_table_relation, fk_column_names, verify_permissions, quote_columns=true) -%}
    {%- set constraint_name = (fk_table_relation.identifier ~ "_" ~ fk_column_names|join('_') ~ "_FK") | upper -%}
    {%- set fk_columns_csv = dbt_constraints.get_quoted_column_csv(fk_column_names, quote_columns) -%}
    {%- set pk_columns_csv = dbt_constraints.get_quoted_column_csv(pk_column_names, quote_columns) -%}
    {#- Check that the PK table has a PK or UK -#}
    {%- if dbt_constraints.unique_constraint_exists(pk_table_relation, pk_column_names) -%}
        {#- Check if the table already has this foreign key -#}
        {%- if not dbt_constraints.foreign_key_exists(fk_table_relation, fk_column_names) -%}

            {%- if dbt_constraints.have_ownership_priv(fk_table_relation, verify_permissions) and dbt_constraints.have_references_priv(pk_table_relation, verify_permissions) -%}

                {%- set query -%}
                ALTER TABLE {{fk_table_relation}} ADD CONSTRAINT {{constraint_name}} FOREIGN KEY ( {{fk_columns_csv}} ) REFERENCES {{pk_table_relation}} ( {{pk_columns_csv}} ) RELY
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

{# Snowflake specific implementation to create a not null constraint #}
{%- macro snowflake__create_not_null(table_relation, column_names, verify_permissions, quote_columns=false) -%}
    {%- set columns_list = dbt_constraints.get_quoted_column_list(column_names, quote_columns) -%}

    {%- if dbt_constraints.have_ownership_priv(table_relation, verify_permissions) -%}

            {%- set modify_statements= [] -%}
            {%- for column in columns_list -%}
                {%- set modify_statements = modify_statements.append( "COLUMN " ~ column ~ " SET NOT NULL" ) -%}
            {%- endfor -%}
            {%- set modify_statement_csv = modify_statements | join(", ") -%}
            {%- set query -%}
                ALTER TABLE {{table_relation}} MODIFY {{ modify_statement_csv }};
            {%- endset -%}
            {%- do log("Creating not null constraint for: " ~ columns_list | join(", ") ~ " in " ~ table_relation, info=true) -%}
            {%- do run_query(query) -%}

    {%- else -%}
        {%- do log("Skipping not null constraint for " ~ columns_list | join(", ") ~ " in " ~ table_relation ~ " because of insufficient privileges: " ~ table_relation, info=true) -%}
    {%- endif -%}
{%- endmacro -%}

{#- This macro is used in create macros to avoid duplicate PK/UK constraints
    and to skip FK where no PK/UK constraint exists on the parent table -#}
{%- macro snowflake__unique_constraint_exists(table_relation, column_names) -%}
    {%- set lookup_query -%}
    SHOW UNIQUE KEYS IN TABLE {{table_relation}}
    {%- endset -%}
    {%- do log("Lookup: " ~ lookup_query, info=false) -%}
    {%- set constraint_list = run_query(lookup_query) -%}
    {%- if constraint_list.columns["column_name"].values() | count > 0 -%}
        {%- for constraint in constraint_list.group_by("constraint_name") -%}
            {%- if dbt_constraints.column_list_matches(constraint.columns["column_name"].values(), column_names ) -%}
                {%- do log("Found UK key: " ~ table_relation ~ " " ~ column_names, info=false) -%}
                {{ return(true) }}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}#}

    {%- set lookup_query -%}
    SHOW PRIMARY KEYS IN TABLE {{table_relation}}
    {%- endset -%}
    {%- do log("Lookup: " ~ lookup_query, info=false) -%}
    {%- set constraint_list = run_query(lookup_query) -%}
    {%- if constraint_list.columns["column_name"].values() | count > 0 -%}
        {%- for constraint in constraint_list.group_by("constraint_name") -%}
            {%- if dbt_constraints.column_list_matches(constraint.columns["column_name"].values(), column_names ) -%}
                {%- do log("Found PK key: " ~ table_relation ~ " " ~ column_names, info=false) -%}
                {{ return(true) }}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}

    {#- If we get this far then the table does not have either constraint -#}
    {%- do log("No PK/UK key: " ~ table_relation ~ " " ~ column_names, info=false) -%}
    {{ return(false) }}
{%- endmacro -%}



{#- This macro is used in create macros to avoid duplicate FK constraints -#}
{%- macro snowflake__foreign_key_exists(table_relation, column_names) -%}
    {%- set lookup_query -%}
    SHOW IMPORTED KEYS IN TABLE {{table_relation}}
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



{%- macro snowflake__have_references_priv(table_relation, verify_permissions) -%}
    {%- if verify_permissions is sameas true -%}

        {%- set lookup_query -%}
        select case when count(*) > 0 then 'y' else 'n' end as "have_references"
        from information_schema.table_privileges t
        join information_schema.enabled_roles er on t.grantee = er.role_name
        where upper(t.table_schema) = upper('{{table_relation.schema}}')
            and upper(t.table_name) = upper('{{table_relation.identifier}}')
            and t.privilege_type = 'REFERENCES'
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


{%- macro snowflake__have_ownership_priv(table_relation, verify_permissions) -%}
    {%- if verify_permissions is sameas true -%}

        {%- set lookup_query -%}
        select case when count(*) > 0 then 'y' else 'n' end as "have_ownership"
        from {{table_relation.database}}.information_schema.tables t
        join {{table_relation.database}}.information_schema.enabled_roles er on t.table_owner = er.role_name
        where upper(t.table_schema) = upper('{{table_relation.schema}}')
            and upper(t.table_name) = upper('{{table_relation.identifier}}')
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
