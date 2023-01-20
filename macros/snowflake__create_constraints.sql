{# Snowflake specific implementation to create a primary key #}
{%- macro snowflake__create_primary_key(table_relation, column_names, verify_permissions, quote_columns=false, constraint_name=none, lookup_cache=none) -%}
    {%- set constraint_name = (constraint_name or table_relation.identifier ~ "_" ~ column_names|join('_') ~ "_PK") | upper -%}
    {%- set columns_csv = dbt_constraints.get_quoted_column_csv(column_names, quote_columns) -%}

    {#- Check that the table does not already have this PK/UK -#}
    {%- if not dbt_constraints.unique_constraint_exists(table_relation, column_names, lookup_cache) -%}

        {%- if dbt_constraints.have_ownership_priv(table_relation, verify_permissions, lookup_cache) -%}

            {%- set query -%}
            ALTER TABLE {{table_relation}} ADD CONSTRAINT {{constraint_name}} PRIMARY KEY ( {{columns_csv}} ) RELY
            {%- endset -%}
            {%- do log("Creating primary key: " ~ constraint_name, info=true) -%}
            {%- do run_query(query) -%}
            {#- Add this constraint to the lookup cache -#}
            {%- do lookup_cache.unique_keys.update({table_relation: {constraint_name: column_names} }) -%}

        {%- else -%}
            {%- do log("Skipping " ~ constraint_name ~ " because of insufficient privileges: " ~ table_relation, info=true) -%}
        {%- endif -%}

    {%- else -%}
        {%- do log("Skipping " ~ constraint_name ~ " because PK/UK already exists: " ~ table_relation ~ " " ~ column_names, info=false) -%}
    {%- endif -%}

{%- endmacro -%}




{# Snowflake specific implementation to create a unique key #}
{%- macro snowflake__create_unique_key(table_relation, column_names, verify_permissions, quote_columns=false, constraint_name=none, lookup_cache=none) -%}
    {%- set constraint_name = (constraint_name or table_relation.identifier ~ "_" ~ column_names|join('_') ~ "_UK") | upper -%}
    {%- set columns_csv = dbt_constraints.get_quoted_column_csv(column_names, quote_columns) -%}

    {#- Check that the table does not already have this PK/UK -#}
    {%- if not dbt_constraints.unique_constraint_exists(table_relation, column_names, lookup_cache) -%}

        {%- if dbt_constraints.have_ownership_priv(table_relation, verify_permissions, lookup_cache) -%}

            {%- set query -%}
            ALTER TABLE {{table_relation}} ADD CONSTRAINT {{constraint_name}} UNIQUE ( {{columns_csv}} ) RELY
            {%- endset -%}
            {%- do log("Creating unique key: " ~ constraint_name, info=true) -%}
            {%- do run_query(query) -%}
            {#- Add this constraint to the lookup cache -#}
            {%- do lookup_cache.unique_keys.update({table_relation: {constraint_name: column_names} }) -%}

        {%- else -%}
            {%- do log("Skipping " ~ constraint_name ~ " because of insufficient privileges: " ~ table_relation, info=true) -%}
        {%- endif -%}

    {%- else -%}
        {%- do log("Skipping " ~ constraint_name ~ " because PK/UK already exists: " ~ table_relation ~ " " ~ column_names, info=false) -%}
    {%- endif -%}

{%- endmacro -%}



{# Snowflake specific implementation to create a foreign key #}
{%- macro snowflake__create_foreign_key(pk_table_relation, pk_column_names, fk_table_relation, fk_column_names, verify_permissions, quote_columns, constraint_name, lookup_cache) -%}
    {%- set constraint_name = (constraint_name or fk_table_relation.identifier ~ "_" ~ fk_column_names|join('_') ~ "_FK") | upper -%}
    {%- set fk_columns_csv = dbt_constraints.get_quoted_column_csv(fk_column_names, quote_columns) -%}
    {%- set pk_columns_csv = dbt_constraints.get_quoted_column_csv(pk_column_names, quote_columns) -%}
    {#- Check that the PK table has a PK or UK -#}
    {%- if dbt_constraints.unique_constraint_exists(pk_table_relation, pk_column_names, lookup_cache) -%}
        {#- Check if the table already has this foreign key -#}
        {%- if not dbt_constraints.foreign_key_exists(fk_table_relation, fk_column_names, lookup_cache) -%}

            {%- if dbt_constraints.have_ownership_priv(fk_table_relation, verify_permissions, lookup_cache) and dbt_constraints.have_references_priv(pk_table_relation, verify_permissions, lookup_cache) -%}

                {%- set query -%}
                ALTER TABLE {{fk_table_relation}} ADD CONSTRAINT {{constraint_name}} FOREIGN KEY ( {{fk_columns_csv}} ) REFERENCES {{pk_table_relation}} ( {{pk_columns_csv}} ) RELY
                {%- endset -%}
                {%- do log("Creating foreign key: " ~ constraint_name ~ " referencing " ~ pk_table_relation.identifier ~ " " ~ pk_column_names, info=true) -%}
                {%- do run_query(query) -%}
                {#- Add this constraint to the lookup cache -#}
                {%- do lookup_cache.foreign_keys.update({fk_table_relation: {constraint_name: fk_column_names} }) -%}

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
{%- macro snowflake__create_not_null(table_relation, column_names, verify_permissions, quote_columns, lookup_cache) -%}

    {%- set existing_not_null_col = lookup_cache.not_null_col[table_relation] -%}

    {%- set columns_to_change = [] -%}
    {%- for column_name in column_names if column_name not in existing_not_null_col -%}
        {%- do columns_to_change.append(column_name) -%}
        {%- do existing_not_null_col.append(column_name) -%}
    {%- endfor -%}
    {%- if columns_to_change|count > 0 -%}
        {%- set columns_list = dbt_constraints.get_quoted_column_list(columns_to_change, quote_columns) -%}

        {%- if dbt_constraints.have_ownership_priv(table_relation, verify_permissions, lookup_cache) -%}

                {%- set modify_statements= [] -%}
                {%- for column in columns_list -%}
                    {%- set modify_statements = modify_statements.append( "COLUMN " ~ column ~ " SET NOT NULL" ) -%}
                {%- endfor -%}
                {%- set modify_statement_csv = modify_statements | join(", ") -%}
                {%- set query -%}
                    ALTER TABLE {{table_relation}} MODIFY {{ modify_statement_csv }};
                {%- endset -%}
                {%- do log("Creating not null constraint for: " ~ columns_to_change | join(", ") ~ " in " ~ table_relation, info=true) -%}
                {%- do run_query(query) -%}
                {#- Add this constraint to the lookup cache -#}
                {%- set constraint_key = table_relation.identifier ~ "_" ~ columns_to_change|join('_') ~ "_NN" -%}
                {%- do lookup_cache.not_null_col.update({table_relation: existing_not_null_col }) -%}

        {%- else -%}
            {%- do log("Skipping not null constraint for " ~ columns_to_change | join(", ") ~ " in " ~ table_relation ~ " because of insufficient privileges: " ~ table_relation, info=true) -%}
        {%- endif -%}
    {%- else -%}
        {%- do log("Skipping not null constraint for " ~ column_names | join(", ") ~ " in " ~ table_relation ~ "  because all columns are already not null", info=false) -%}
    {%- endif -%}
{%- endmacro -%}



{#- This macro is used in create macros to avoid duplicate PK/UK constraints
    and to skip FK where no PK/UK constraint exists on the parent table -#}
{%- macro snowflake__unique_constraint_exists(table_relation, column_names, lookup_cache) -%}

    {#- Check if we can find this constraint in the lookup cache -#}
    {%- if table_relation in lookup_cache.unique_keys -%}
        {%- set cached_unique_keys = lookup_cache.unique_keys[table_relation] -%}
        {%- for cached_columns in cached_unique_keys.values() -%}
            {%- if dbt_constraints.column_list_matches(cached_columns, column_names ) -%}
                {%- do log("Found UK key: " ~ table_relation ~ " " ~ column_names, info=false) -%}
                {{ return(true) }}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}

    {%- set lookup_query -%}
    SHOW UNIQUE KEYS IN TABLE {{table_relation}}
    {%- endset -%}
    {%- set constraint_list = run_query(lookup_query) -%}
    {%- if constraint_list.columns["column_name"].values() | count > 0 -%}
        {%- for constraint in constraint_list.group_by("constraint_name") -%}
            {#- Add this constraint to the lookup cache -#}
            {%- do lookup_cache.unique_keys.update({table_relation: {constraint.key_name: constraint.columns["column_name"].values()} }) -%}
        {% endfor %}
        {%- for constraint in constraint_list.group_by("constraint_name") -%}
            {%- if dbt_constraints.column_list_matches(constraint.columns["column_name"].values(), column_names ) -%}
                {%- do log("Found UK key: " ~ table_relation ~ " " ~ column_names, info=false) -%}
                {{ return(true) }}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}

    {%- set lookup_query -%}
    SHOW PRIMARY KEYS IN TABLE {{table_relation}}
    {%- endset -%}
    {%- set constraint_list = run_query(lookup_query) -%}
    {%- if constraint_list.columns["column_name"].values() | count > 0 -%}
        {%- for constraint in constraint_list.group_by("constraint_name") -%}
            {#- Add this constraint to the lookup cache -#}
            {%- do lookup_cache.unique_keys.update({table_relation: {constraint.key_name: constraint.columns["column_name"].values()} }) -%}
        {% endfor %}
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
{%- macro snowflake__foreign_key_exists(table_relation, column_names, lookup_cache) -%}

    {#- Check if we can find this constraint in the lookup cache -#}
    {%- if table_relation in lookup_cache.foreign_keys -%}
        {%- set cached_foreign_keys = lookup_cache.foreign_keys[table_relation] -%}
        {%- for cached_columns in cached_foreign_keys.values() -%}
            {%- if dbt_constraints.column_list_matches(cached_columns, column_names ) -%}
                {%- do log("Found FK key: " ~ table_relation ~ " " ~ column_names, info=false) -%}
                {{ return(true) }}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}

    {%- set lookup_query -%}
    SHOW IMPORTED KEYS IN TABLE {{table_relation}}
    {%- endset -%}
    {%- set constraint_list = run_query(lookup_query) -%}
    {%- if constraint_list.columns["fk_column_name"].values() | count > 0 -%}
        {%- for constraint in constraint_list.group_by("fk_name") -%}
            {#- Add this constraint to the lookup cache -#}
            {%- do lookup_cache.foreign_keys.update({table_relation: {constraint.key_name: constraint.columns["fk_column_name"].values()} }) -%}
        {% endfor %}
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



{%- macro snowflake__have_references_priv(table_relation, verify_permissions, lookup_cache) -%}
    {%- if verify_permissions is sameas true -%}

        {%- set table_privileges = snowflake__lookup_table_privileges(table_relation, lookup_cache) -%}
        {%- if "REFERENCES" in table_privileges or "OWNERSHIP" in table_privileges -%}
            {{ return(true) }}
        {%- else -%}
            {{ return(false) }}
        {%- endif -%}

    {%- else -%}
        {{ return(true) }}
    {%- endif -%}
{%- endmacro -%}



{%- macro snowflake__have_ownership_priv(table_relation, verify_permissions, lookup_cache) -%}
    {%- if verify_permissions is sameas true -%}

        {%- set table_privileges = snowflake__lookup_table_privileges(table_relation, lookup_cache) -%}
        {%- if "OWNERSHIP" in table_privileges -%}
            {{ return(true) }}
        {%- else -%}
            {{ return(false) }}
        {%- endif -%}

    {%- else -%}
        {{ return(true) }}
    {%- endif -%}
{%- endmacro -%}



{%- macro snowflake__lookup_table_privileges(table_relation, lookup_cache) -%}

    {%- if table_relation.database not in lookup_cache.table_privileges -%}
        {%- set lookup_query -%}
        select distinct
            upper(tp.table_schema) as "table_schema",
            upper(tp.table_name) as "table_name",
            tp.privilege_type as "privilege_type"
        from {{table_relation.database}}.information_schema.table_privileges tp
        where is_role_in_session(tp.grantee)
            and tp.privilege_type in ('OWNERSHIP', 'REFERENCES')
        {%- endset -%}
        {%- do log("Caching privileges for database: " ~ table_relation.database, info=false) -%}
        {%- set privilege_list = run_query(lookup_query) -%}
        {%- do lookup_cache.table_privileges.update({ table_relation.database: privilege_list }) -%}
    {%- endif -%}

    {%- set tab_priv_list = [] -%}
    {%- set schema_name = table_relation.schema|upper -%}
    {%- set table_name = table_relation.identifier|upper -%}
    {%- for row in lookup_cache.table_privileges[table_relation.database].rows -%}
        {%- if row["table_schema"] == schema_name and row["table_name"] == table_name -%}
            {%- do tab_priv_list.append(row["privilege_type"]) -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(tab_priv_list) }}

{%- endmacro -%}



{%- macro snowflake__lookup_table_columns(table_relation, lookup_cache) -%}

    {%- if table_relation not in lookup_cache.table_columns -%}
        {%- set lookup_query -%}
        SHOW COLUMNS IN TABLE {{table_relation}}
        {%- endset -%}
        {%- set results = run_query(lookup_query) -%}

        {%- set not_null_col = [] -%}
        {%- set upper_column_list = [] -%}
        {%- for row in results.rows -%}
            {%- do upper_column_list.append(row["column_name"]|upper) -%}
            {%- if row['null?'] == 'false' -%}
                {%- do not_null_col.append(row["column_name"]|upper) -%}
            {%- endif -%}
        {%- endfor -%}
        {%- do lookup_cache.table_columns.update({ table_relation: upper_column_list }) -%}
        {%- do lookup_cache.not_null_col.update({ table_relation: not_null_col }) -%}
    {%- endif -%}
    {{ return(lookup_cache.table_columns[table_relation]) }}

{%- endmacro -%}
