{# PostgreSQL specific implementation to create a primary key #}
{%- macro postgres__create_primary_key(table_relation, column_names, verify_permissions, quote_columns, constraint_name, lookup_cache, rely_clause) -%}
    {%- set constraint_name = dbt_constraints.sanitize_constraint_name((constraint_name or table_relation.identifier ~ "_" ~ column_names|join('_') ~ "_PK")) -%}

    {%- if constraint_name|length > 63 %}
        {%- set constraint_name_query %}
        select  'PK_' || md5( '{{ constraint_name }}' )::varchar as "constraint_name"
        {%- endset -%}
        {%- set results = run_query(constraint_name_query) -%}
        {%- set constraint_name = results.columns[0].values()[0] -%}
    {% endif %}

    {%- set columns_csv = dbt_constraints.get_quoted_column_csv(column_names, quote_columns) -%}

    {#- Check that the table does not already have this PK/UK -#}
    {%- if not dbt_constraints.unique_constraint_exists(table_relation, column_names, lookup_cache) -%}

        {%- if dbt_constraints.have_ownership_priv(table_relation, verify_permissions, lookup_cache) -%}

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
{%- macro postgres__create_unique_key(table_relation, column_names, verify_permissions, quote_columns, constraint_name, lookup_cache, rely_clause) -%}
    {%- set constraint_name = dbt_constraints.sanitize_constraint_name((constraint_name or table_relation.identifier ~ "_" ~ column_names|join('_') ~ "_UK")) -%}

    {%- if constraint_name|length > 63 %}
        {%- set constraint_name_query %}
        select  'UK_' || md5( '{{ constraint_name }}' )::varchar as "constraint_name"
        {%- endset -%}
        {%- set results = run_query(constraint_name_query) -%}
        {%- set constraint_name = results.columns[0].values()[0] -%}
    {% endif %}

    {%- set columns_csv = dbt_constraints.get_quoted_column_csv(column_names, quote_columns) -%}

    {#- Check that the table does not already have this PK/UK -#}
    {%- if not dbt_constraints.unique_constraint_exists(table_relation, column_names, lookup_cache) -%}

        {%- if dbt_constraints.have_ownership_priv(table_relation, verify_permissions, lookup_cache) -%}

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
{%- macro postgres__create_not_null(table_relation, column_names, verify_permissions, quote_columns, lookup_cache, rely_clause) -%}
    {%- set columns_list = dbt_constraints.get_quoted_column_list(column_names, quote_columns) -%}

    {%- if dbt_constraints.have_ownership_priv(table_relation, verify_permissions, lookup_cache) -%}

            {%- set modify_statements= [] -%}
            {%- for column in columns_list -%}
                {%- do modify_statements.append( "ALTER COLUMN " ~ column ~ " SET NOT NULL" ) -%}
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
{%- macro postgres__create_foreign_key(pk_table_relation, pk_column_names, fk_table_relation, fk_column_names, verify_permissions, quote_columns, constraint_name, lookup_cache, rely_clause) -%}
    {%- set constraint_name = dbt_constraints.sanitize_constraint_name((constraint_name or fk_table_relation.identifier ~ "_" ~ fk_column_names|join('_') ~ "_FK")) -%}

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
    {%- if dbt_constraints.unique_constraint_exists(pk_table_relation, pk_column_names, lookup_cache) -%}
        {#- Check if the table already has this foreign key -#}
        {%- if not dbt_constraints.foreign_key_exists(fk_table_relation, fk_column_names) -%}

            {%- if dbt_constraints.have_ownership_priv(fk_table_relation, verify_permissions, lookup_cache) and dbt_constraints.have_references_priv(pk_table_relation, verify_permissions, lookup_cache) -%}

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
{%- macro postgres__unique_constraint_exists(table_relation, column_names, lookup_cache) -%}
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
        {%- set constraint_map = dbt_constraints.get_results_group_by(constraint_list, "constraint_name") -%}
        {%- for c_name, c_rows in constraint_map.items() -%}
            {%- set columns = [] -%}
            {%- for row in c_rows -%}
                {%- do columns.append(row["column_name"]) -%}
            {%- endfor -%}
            {%- if dbt_constraints.column_list_matches(columns, column_names ) -%}
                {%- do log("Found PK/UK key: " ~ table_relation ~ " " ~ column_names, info=false) -%}
                {{ return(true) }}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}

    {#- If we get this far then the table does not have either constraint -#}
    {%- do log("No PK/UK key: " ~ table_relation ~ " " ~ column_names, info=false) -%}
    {{ return(false) }}
{%- endmacro -%}



{#- This macro is used in create macros to avoid duplicate FK constraints -#}
{%- macro postgres__foreign_key_exists(table_relation, column_names, lookup_cache) -%}
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
        {%- set constraint_map = dbt_constraints.get_results_group_by(constraint_list, "fk_name") -%}
        {%- for c_name, c_rows in constraint_map.items() -%}
            {%- set columns = [] -%}
            {%- for row in c_rows -%}
                {%- do columns.append(row["fk_column_name"]) -%}
            {%- endfor -%}
            {%- if dbt_constraints.column_list_matches(columns, column_names ) -%}
                {%- do log("Found FK key: " ~ table_relation ~ " " ~ column_names, info=false) -%}
                {{ return(true) }}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}

    {#- If we get this far then the table does not have this constraint -#}
    {%- do log("No FK key: " ~ table_relation ~ " " ~ column_names, info=false) -%}
    {{ return(false) }}
{%- endmacro -%}


{%- macro postgres__have_references_priv(table_relation, verify_permissions, lookup_cache) -%}
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


{%- macro postgres__have_ownership_priv(table_relation, verify_permissions, lookup_cache) -%}
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

{#- Release constraints before the build phase starts.

    postgres__drop_relation above does the same work. It runs on the worker thread
    that builds that model. A CASCADE drop of a constraint also locks each table
    whose foreign key needs that constraint. Two threads can then take the same
    locks in opposite order and deadlock.

    on-run-start calls this macro one time, from a single session, before the
    threads start. This removes the contention. The DDL and the set of dropped
    constraints stay the same. Only the time and the session count change. -#}
{% macro postgres__release_constraints_for_rebuild(rebuild_relations) -%}
    {%- for relation in rebuild_relations -%}
        {{ postgres__drop_referential_constraints(relation) }}
    {%- endfor -%}
{% endmacro %}



{#- PostgreSQL enforces every constraint type (issue #114).
    An adapter plugin that inherits from postgres (dbt-yellowbrick, dbt-materialize,
    dbt-risingwave) also dispatches to this macro, but its database may not enforce
    keys. Yellowbrick, for example, accepts keys and does not enforce them. Such a
    plugin gets no skipped tests here. It can override this macro to opt in. -#}
{%- macro postgres__adapter_enforced_constraint_types() -%}
    {%- if target.type == 'postgres' -%}
        {{ return(['not_null', 'primary_key', 'unique_key', 'foreign_key']) }}
    {%- endif -%}
    {{ return([]) }}
{%- endmacro -%}


{#- These lookups return only constraints that PostgreSQL enforces and has
    validated:
    - convalidated excludes a constraint added NOT VALID.
    - conenforced (PostgreSQL 18) excludes a constraint added NOT ENFORCED.
      to_jsonb reads it, so the query also runs on versions that lack it.
    - indisvalid excludes a unique index that a failed build left behind.
    - A table with classic inheritance children is excluded. A query on the
      parent reads the children, but the parent's constraint does not cover
      them. A partitioned table (relkind 'p') is safe, because its keys must
      include the partition key.
    - A key is excluded when one of its columns shares an upper-case name with
      another column of the table, such as "OrderKey" and orderkey. The tests
      compare column names without case, so they cannot tell the two apart.
    A deferrable constraint qualifies. dbt commits the model before a test runs,
    and the database checks a deferred constraint at that commit. -#}
{%- macro postgres__lookup_enforced_unique_keys(table_relation) -%}
    {%- set lookup_query -%}
    select c.conname as constraint_name
        , c.contype as constraint_type
        , upper(a.attname) as column_name
        , coalesce((to_jsonb(i) ->> 'indnullsnotdistinct')::boolean, false) as nulls_not_distinct
    from pg_constraint c
    join pg_class tbl on tbl.oid = c.conrelid
    join pg_namespace ns on ns.oid = tbl.relnamespace
    join pg_index i on i.indexrelid = c.conindid
    cross join lateral unnest(c.conkey) with ordinality as k(attnum, ord)
    join pg_attribute a on a.attrelid = tbl.oid and a.attnum = k.attnum
    where ns.nspname = '{{ table_relation.schema | replace("'", "''") }}'
        and tbl.relname = '{{ table_relation.identifier | replace("'", "''") }}'
        and c.contype in ('p', 'u')
        and c.convalidated
        and coalesce((to_jsonb(c) ->> 'conenforced')::boolean, true)
        and i.indisvalid
        and (tbl.relkind = 'p'
            or not exists (select 1 from pg_inherits h where h.inhparent = tbl.oid))
        and not exists (
            select 1 from unnest(c.conkey) ck(attnum)
            join pg_attribute x on x.attrelid = tbl.oid and x.attnum = ck.attnum
            join pg_attribute y on y.attrelid = tbl.oid and y.attnum > 0
                and not y.attisdropped and y.attnum <> x.attnum
                and upper(y.attname) = upper(x.attname) )
    order by c.conname, k.ord
    {%- endset -%}
    {%- do log("Lookup: " ~ lookup_query, info=false) -%}
    {{ return(dbt_constraints.keys_from_lookup_rows(run_query(lookup_query))) }}
{%- endmacro -%}


{#- attnotnull is also true for a NOT VALID not-null constraint, which
    PostgreSQL 18 stores as contype 'n'. Exclude a column that has one.
    Also exclude a column that shares an upper-case name with another column.
    A missing column only means its NOT NULL is not proven. -#}
{%- macro postgres__lookup_enforced_not_null_columns(table_relation) -%}
    {%- set lookup_query -%}
    select upper(a.attname) as column_name
    from pg_attribute a
    join pg_class tbl on tbl.oid = a.attrelid
    join pg_namespace ns on ns.oid = tbl.relnamespace
    where ns.nspname = '{{ table_relation.schema | replace("'", "''") }}'
        and tbl.relname = '{{ table_relation.identifier | replace("'", "''") }}'
        and a.attnum > 0
        and not a.attisdropped
        and a.attnotnull
        and not exists (
            select 1 from pg_constraint n
            where n.conrelid = tbl.oid
                and n.contype = 'n'
                and n.conkey = array[a.attnum]
                and not n.convalidated )
        and not exists (
            select 1 from pg_attribute y
            where y.attrelid = tbl.oid and y.attnum > 0
                and not y.attisdropped and y.attnum <> a.attnum
                and upper(y.attname) = upper(a.attname) )
    {%- endset -%}
    {%- do log("Lookup: " ~ lookup_query, info=false) -%}
    {{ return(run_query(lookup_query).columns["column_name"].values() | map('upper') | list) }}
{%- endmacro -%}


{%- macro postgres__lookup_enforced_foreign_keys(fk_relation, pk_relation) -%}
    {%- set lookup_query -%}
    select c.conname as constraint_name
        , upper(ca.attname) as column_name
        , upper(pa.attname) as pk_column_name
    from pg_constraint c
    join pg_class ct on ct.oid = c.conrelid
    join pg_namespace cn on cn.oid = ct.relnamespace
    join pg_class pt on pt.oid = c.confrelid
    join pg_namespace pn on pn.oid = pt.relnamespace
    cross join lateral unnest(c.conkey, c.confkey) with ordinality as k(child_attnum, parent_attnum, ord)
    join pg_attribute ca on ca.attrelid = ct.oid and ca.attnum = k.child_attnum
    join pg_attribute pa on pa.attrelid = pt.oid and pa.attnum = k.parent_attnum
    where cn.nspname = '{{ fk_relation.schema | replace("'", "''") }}'
        and ct.relname = '{{ fk_relation.identifier | replace("'", "''") }}'
        and pn.nspname = '{{ pk_relation.schema | replace("'", "''") }}'
        and pt.relname = '{{ pk_relation.identifier | replace("'", "''") }}'
        and c.contype = 'f'
        and c.convalidated
        and coalesce((to_jsonb(c) ->> 'conenforced')::boolean, true)
        and (ct.relkind = 'p'
            or not exists (select 1 from pg_inherits h where h.inhparent = ct.oid))
        and not exists (
            select 1 from unnest(c.conkey) ck(attnum)
            join pg_attribute x on x.attrelid = ct.oid and x.attnum = ck.attnum
            join pg_attribute y on y.attrelid = ct.oid and y.attnum > 0
                and not y.attisdropped and y.attnum <> x.attnum
                and upper(y.attname) = upper(x.attname) )
        and not exists (
            select 1 from unnest(c.confkey) pk(attnum)
            join pg_attribute x on x.attrelid = pt.oid and x.attnum = pk.attnum
            join pg_attribute y on y.attrelid = pt.oid and y.attnum > 0
                and not y.attisdropped and y.attnum <> x.attnum
                and upper(y.attname) = upper(x.attname) )
    order by c.conname, k.ord
    {%- endset -%}
    {%- do log("Lookup: " ~ lookup_query, info=false) -%}
    {{ return(dbt_constraints.keys_from_lookup_rows(run_query(lookup_query))) }}
{%- endmacro -%}
