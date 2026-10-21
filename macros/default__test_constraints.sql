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
{#- Wrap the model in a subselect. A test that sets the where config receives
    "(select * from x where ...) dbt_subquery", which already holds an alias.
    A direct "from {{model}} pk_test" then produces two aliases and invalid SQL. -#}
select validation_errors.* from (
    select
        {{prefixed_columns_list | join(', ')}}, count(*) as n_records
    from (select * from {{model}}) pk_test
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
{#- Wrap the model in a subselect. See the note in default__test_primary_key. -#}
select validation_errors.* from (
    select
        {{prefixed_columns_list | join(', ')}}, count(*) as n_records
    from (select * from {{model}}) uk_test
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
        from (select * from {{model}}) fk_child_inner
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
            {%- do column_list.append( adapter.quote(column) ) -%}
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
        {%- do prefixed_column_list.append( prefix_alias ~ '.' ~ column_list[x] ) -%}
    {%- endfor -%}
    {{ return(prefixed_column_list) }}
{%- endmacro -%}


{%- macro get_join_conditions(column_array_left, prefix_alias_left, column_array_right, prefix_alias_right, quote_columns=false) -%}
    {%- set column_list_left = dbt_constraints.get_prefixed_column_list(column_array_left, prefix_alias_left, quote_columns) -%}
    {%- set column_list_right = dbt_constraints.get_prefixed_column_list(column_array_right, prefix_alias_right, quote_columns) -%}

    {%- set join_conditions = [] -%}
    {%- for x in range(column_list_left|count) -%}
        {%- do join_conditions.append( column_list_left[x] ~ ' = ' ~ column_list_right[x] ) -%}
    {%- endfor -%}
    {{ return(join_conditions) }}
{%- endmacro -%}



{#- Skip a test when the database already enforces the constraint (issue #114).

   A test of a constraint that the database enforces and has validated cannot
   find a failing row. A large incremental model can spend longer on that test
   than on its load. Set the var `dbt_constraints_skip_tests_if_enforced` to true
   to skip such tests.

   A skipped test still produces its full SQL. skipped_test_sql adds a predicate
   that is never true, so the test returns no rows. The database can drop the
   scan because the predicate is a constant.

   Only a constraint that the database enforces AND has validated qualifies. A
   NOT VALID, NOVALIDATE, DISABLED, or RELY-only constraint does not. Each adapter
   reports what its database can enforce, and implements the catalog lookups. -#}

{#- Return the constraint types that this database enforces. -#}
{%- macro adapter_enforced_constraint_types() -%}
    {{ return(adapter.dispatch('adapter_enforced_constraint_types', 'dbt_constraints')()) }}
{%- endmacro -%}

{#- An adapter that does not say what it enforces never skips a test. -#}
{%- macro default__adapter_enforced_constraint_types() -%}
    {{ return([]) }}
{%- endmacro -%}


{#- Return the validated, enforced primary and unique keys on a table. Each item
   is a dict: name, type ('p' or 'u'), columns (list), and nulls_not_distinct
   (true when the key rejects repeated NULLs). -#}
{%- macro lookup_enforced_unique_keys(table_relation) -%}
    {{ return(adapter.dispatch('lookup_enforced_unique_keys', 'dbt_constraints')(table_relation)) }}
{%- endmacro -%}

{%- macro default__lookup_enforced_unique_keys(table_relation) -%}
    {{ return([]) }}
{%- endmacro -%}


{#- Return the upper-case names of the columns that have a validated, enforced
   NOT NULL. -#}
{%- macro lookup_enforced_not_null_columns(table_relation) -%}
    {{ return(adapter.dispatch('lookup_enforced_not_null_columns', 'dbt_constraints')(table_relation)) }}
{%- endmacro -%}

{%- macro default__lookup_enforced_not_null_columns(table_relation) -%}
    {{ return([]) }}
{%- endmacro -%}


{#- Return the validated, enforced foreign keys from fk_relation to pk_relation.
   Each item is a dict: name, columns (child columns), and pk_columns (the parent
   columns in the same order). -#}
{%- macro lookup_enforced_foreign_keys(fk_relation, pk_relation) -%}
    {{ return(adapter.dispatch('lookup_enforced_foreign_keys', 'dbt_constraints')(fk_relation, pk_relation)) }}
{%- endmacro -%}

{%- macro default__lookup_enforced_foreign_keys(fk_relation, pk_relation) -%}
    {{ return([]) }}
{%- endmacro -%}


{#- Turn the rows of a key lookup into the dicts the lookups above return. The
   rows must name their columns constraint_name and column_name, and list the
   columns of each constraint in position order. They can also carry
   constraint_type, nulls_not_distinct, and pk_column_name.

   query_results is a run_query result, or a dict with the same column_names and
   rows keys. Each key dict is complete before it is stored, and nothing changes
   it later. The cost is rows x keys, which is small for the keys of one table. -#}
{%- macro keys_from_lookup_rows(query_results) -%}
    {%- set has_type = "constraint_type" in query_results.column_names -%}
    {%- set has_nulls_not_distinct = "nulls_not_distinct" in query_results.column_names -%}
    {%- set has_pk_columns = "pk_column_name" in query_results.column_names -%}
    {%- set names = [] -%}
    {%- for row in query_results.rows -%}
        {%- if row["constraint_name"] | string not in names -%}
            {%- do names.append(row["constraint_name"] | string) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- set keys = [] -%}
    {%- for name in names -%}
        {%- set columns = [] -%}
        {%- set pk_columns = [] -%}
        {%- set ns = namespace(type="", nulls_not_distinct=false) -%}
        {%- for row in query_results.rows if row["constraint_name"] | string == name -%}
            {%- do columns.append(row["column_name"] | string | upper) -%}
            {%- if has_pk_columns -%}
                {%- do pk_columns.append(row["pk_column_name"] | string | upper) -%}
            {%- endif -%}
            {%- if has_type -%}
                {%- set ns.type = row["constraint_type"] | string | lower -%}
            {%- endif -%}
            {%- if has_nulls_not_distinct -%}
                {%- set ns.nulls_not_distinct = row["nulls_not_distinct"] | string | lower in ("true", "1", "y", "yes") -%}
            {%- endif -%}
        {%- endfor -%}
        {%- do keys.append({
            "name": name,
            "type": ns.type,
            "nulls_not_distinct": ns.nulls_not_distinct,
            "columns": columns,
            "pk_columns": pk_columns }) -%}
    {%- endfor -%}
    {{ return(keys) }}
{%- endmacro -%}


{#- Return the name of the constraint in the given keys that guarantees the test
   passes, or none. This macro runs no query. unique_keys and foreign_keys are
   the dicts that keys_from_lookup_rows returns. not_null_columns holds upper-case
   names.

   A unique constraint lets repeated NULLs through, but the unique_key test counts
   two NULL keys as a duplicate. So a unique constraint qualifies only when it
   rejects repeated NULLs, or when every key column has a validated NOT NULL. A
   primary key qualifies for both tests. A foreign key must join the same child
   and parent columns, in the same pairs. -#}
{%- macro match_enforced_constraint(kind, column_names, unique_keys=[], not_null_columns=[], pk_column_names=[], foreign_keys=[]) -%}
    {%- if kind in ('primary_key', 'unique_key') -%}
        {%- set ns = namespace(all_not_null=true) -%}
        {%- for column in column_names | map('upper') | map('trim', '"') if column not in not_null_columns -%}
            {%- set ns.all_not_null = false -%}
        {%- endfor -%}
        {%- for key in unique_keys if dbt_constraints.column_list_matches(key.columns, column_names) -%}
            {%- if key.type == 'p' or (kind == 'unique_key' and key.nulls_not_distinct) or ns.all_not_null -%}
                {{ return(key.name) }}
            {%- endif -%}
        {%- endfor -%}

    {%- elif kind == 'foreign_key' and column_names | count > 0
            and column_names | count == pk_column_names | count -%}
        {%- set test_pairs = [] -%}
        {%- for i in range(column_names | count) -%}
            {%- do test_pairs.append(column_names[i] | upper | trim('"') ~ '=' ~ pk_column_names[i] | upper | trim('"')) -%}
        {%- endfor -%}
        {%- for key in foreign_keys -%}
            {%- set key_pairs = [] -%}
            {%- for i in range(key.columns | count) if i < key.pk_columns | count -%}
                {%- do key_pairs.append(key.columns[i] | upper ~ '=' ~ key.pk_columns[i] | upper) -%}
            {%- endfor -%}
            {%- if key.columns | count == key.pk_columns | count
                    and dbt_constraints.column_list_matches(key_pairs, test_pairs) -%}
                {{ return(key.name) }}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

    {{ return(none) }}
{%- endmacro -%}


{#- Run the catalog lookups for one test, then match them. The NOT NULL lookup
   runs only when a unique key has the test's columns, which avoids a query for
   most tables. -#}
{%- macro enforced_constraint_for_test(kind, table_relation, column_names, pk_relation=none, pk_column_names=[]) -%}
    {%- if kind in ('primary_key', 'unique_key') -%}
        {%- set unique_keys = dbt_constraints.lookup_enforced_unique_keys(table_relation) -%}
        {#- A set inside a for loop does not outlive one pass, so the NOT NULL
            columns go in a namespace. -#}
        {%- set nn = namespace(columns=[]) -%}
        {%- for key in unique_keys if dbt_constraints.column_list_matches(key.columns, column_names) -%}
            {%- if loop.first -%}
                {%- set nn.columns = dbt_constraints.lookup_enforced_not_null_columns(table_relation) -%}
            {%- endif -%}
        {%- endfor -%}
        {{ return(dbt_constraints.match_enforced_constraint(kind, column_names,
            unique_keys=unique_keys, not_null_columns=nn.columns)) }}

    {%- elif kind == 'foreign_key' and pk_relation is not none -%}
        {{ return(dbt_constraints.match_enforced_constraint(kind, column_names,
            pk_column_names=pk_column_names,
            foreign_keys=dbt_constraints.lookup_enforced_foreign_keys(table_relation, pk_relation))) }}
    {%- endif -%}

    {{ return(none) }}
{%- endmacro -%}


{#- Return true when `relation` is a table relation rather than SQL text. A test
   with a `where` config receives "(select ... where ...) dbt_subquery" instead
   of a relation, and that test is never skipped. -#}
{%- macro is_table_relation_object(relation) -%}
    {{ return(relation is not none and relation is not string
        and relation.identifier is defined and relation.identifier
        and relation.schema is defined and relation.schema) }}
{%- endmacro -%}


{#- Return the test SQL, or the never-true form when the test can be skipped. -#}
{%- macro sql_or_skipped_for_enforced(kind, test_sql, model, column_names, pk_table=none, pk_column_names=[]) -%}
    {%- if execute
        and var('dbt_constraints_skip_tests_if_enforced', "false")|string|lower == "true"
        and kind in dbt_constraints.adapter_enforced_constraint_types()
        and dbt_constraints.is_table_relation_object(model)
        and (kind != 'foreign_key' or dbt_constraints.is_table_relation_object(pk_table)) -%}
        {%- set constraint_name = dbt_constraints.enforced_constraint_for_test(
            kind, model, column_names, pk_table, pk_column_names) -%}
        {%- if constraint_name -%}
            {%- do log("dbt Constraints: skipping validation of " ~ kind ~ " on " ~ model
                ~ " because the database enforces validated constraint " ~ constraint_name, info=true) -%}
            {{ return(dbt_constraints.skipped_test_sql(test_sql, kind, constraint_name)) }}
        {%- endif -%}
    {%- endif -%}
    {{ return(test_sql) }}
{%- endmacro -%}


{#- Wrap the full test SQL in a predicate that is never true. A database can
   override this to use its own form. -#}
{%- macro skipped_test_sql(test_sql, kind, constraint_name) -%}
    {{ return(adapter.dispatch('skipped_test_sql', 'dbt_constraints')(test_sql, kind, constraint_name)) }}
{%- endmacro -%}

{#- The comment carries only the test kind, a fixed word. The constraint name
   comes from the catalog and could close the comment, so it is logged instead. -#}
{%- macro default__skipped_test_sql(test_sql, kind, constraint_name) -%}
/* dbt_constraints: validation skipped, the database enforces a validated {{ kind }} constraint */
select * from (
{{ test_sql }}
) dbt_constraints_enforced
where 1 = 0
{%- endmacro -%}
