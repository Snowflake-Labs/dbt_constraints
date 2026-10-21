{#- Add an enforced constraint in a model post-hook, before the model's tests run.

    Purpose: exercise issue #114 inside the single preparation build.

    The package releases a table's constraints before it rebuilds the table, and it
    creates constraints in on-run-end, after the tests. So in a full build no test
    ever sees an enforced constraint. A post-hook runs right after the model
    materializes and before its tests, so a constraint that it adds is in place
    when the test is compiled. That is the state of an incremental model on its
    second run, which is what issue #114 describes.

    Arguments:
      kind             'primary_key', 'unique_key' or 'foreign_key', or one of:
                       'not_null'                   NOT NULL on each column
                       'unique_nulls_not_distinct'  PostgreSQL only: a unique key
                                                    that rejects repeated NULLs
                       'unusable_index'             Oracle only: mark the index of
                                                    constraint_name UNUSABLE
      column_names     list of columns on this model
      constraint_name  name of the constraint to add
      pk_relation      foreign key only: the parent relation, usually ref('...')
      pk_column_names  foreign key only: the parent columns, in the same order
      validated        false adds a constraint that the database has not checked
                       against existing rows: NOT VALID on PostgreSQL (foreign key
                       only), ENABLE NOVALIDATE on Oracle, DISABLED on Vertica.

    Only PostgreSQL, Oracle and Vertica enforce these constraints, so every other
    adapter gets a no-op. A kind that a database does not support is a no-op there.
-#}
{%- macro stage_enforced_constraint(kind, column_names, constraint_name, pk_relation=none, pk_column_names=[], validated=true) -%}
    {{ return(adapter.dispatch('stage_enforced_constraint', 'dbt_constraints_integration_tests')(
        kind, column_names, constraint_name, pk_relation, pk_column_names, validated)) }}
{%- endmacro -%}


{%- macro default__stage_enforced_constraint(kind, column_names, constraint_name, pk_relation, pk_column_names, validated) -%}
{%- endmacro -%}


{%- macro enforced_constraint_ddl(kind, column_names, constraint_name, pk_relation, pk_column_names) -%}
    {%- set columns_csv = column_names | join(", ") -%}
    {%- if kind == 'primary_key' -%}
        {%- set ddl = "ALTER TABLE " ~ this ~ " ADD CONSTRAINT " ~ constraint_name ~ " PRIMARY KEY (" ~ columns_csv ~ ")" -%}
    {%- elif kind == 'unique_key' -%}
        {%- set ddl = "ALTER TABLE " ~ this ~ " ADD CONSTRAINT " ~ constraint_name ~ " UNIQUE (" ~ columns_csv ~ ")" -%}
    {%- elif kind == 'foreign_key' -%}
        {%- set ddl = "ALTER TABLE " ~ this ~ " ADD CONSTRAINT " ~ constraint_name ~ " FOREIGN KEY (" ~ columns_csv
            ~ ") REFERENCES " ~ pk_relation ~ " (" ~ pk_column_names | join(", ") ~ ")" -%}
    {%- else -%}
        {{ exceptions.raise_compiler_error("stage_enforced_constraint: unknown kind " ~ kind) }}
    {%- endif -%}
    {{ return(ddl) }}
{%- endmacro -%}


{%- macro log_staged_constraint(kind, constraint_name, validated) -%}
    {%- do log("Test staging: added " ~ ("" if validated else "unvalidated ") ~ kind
        ~ " " ~ constraint_name ~ " on " ~ this.identifier, info=true) -%}
{%- endmacro -%}


{%- macro postgres__stage_enforced_constraint(kind, column_names, constraint_name, pk_relation, pk_column_names, validated) -%}
    {%- if execute -%}
        {%- if kind == 'not_null' -%}
            {%- for column in column_names -%}
                {%- do run_query("ALTER TABLE " ~ this ~ " ALTER COLUMN " ~ column ~ " SET NOT NULL") -%}
            {%- endfor -%}
        {%- elif kind == 'unique_nulls_not_distinct' -%}
            {%- do run_query("ALTER TABLE " ~ this ~ " ADD CONSTRAINT " ~ constraint_name
                ~ " UNIQUE NULLS NOT DISTINCT (" ~ column_names | join(", ") ~ ")") -%}
        {%- elif kind == 'unusable_index' -%}
            {{ return("") }}
        {%- elif not validated and kind != 'foreign_key' -%}
            {#- PostgreSQL accepts NOT VALID only on a foreign key, check or not-null
                constraint. An unvalidated key is left out, so its test runs. -#}
            {{ return("") }}
        {%- else -%}
            {%- set ddl = enforced_constraint_ddl(kind, column_names, constraint_name, pk_relation, pk_column_names) -%}
            {%- do run_query(ddl ~ ("" if validated else " NOT VALID")) -%}
        {%- endif -%}
        {%- do log_staged_constraint(kind, constraint_name, validated) -%}
    {%- endif -%}
{%- endmacro -%}


{%- macro oracle__stage_enforced_constraint(kind, column_names, constraint_name, pk_relation, pk_column_names, validated) -%}
    {%- if execute -%}
        {%- if kind == 'not_null' -%}
            {#- MODIFY fails with ORA-01442 on a column that is already NOT NULL. -#}
            {%- for column in column_names -%}
                {%- set nullable = run_query("SELECT nullable FROM user_tab_columns WHERE table_name = '"
                    ~ this.identifier | upper ~ "' AND column_name = '" ~ column | upper ~ "'") -%}
                {%- if nullable.rows | count > 0 and nullable.rows[0][0] == 'Y' -%}
                    {%- do run_query("ALTER TABLE " ~ this ~ " MODIFY (" ~ column ~ " NOT NULL)") -%}
                {%- endif -%}
            {%- endfor -%}
        {%- elif kind == 'unique_nulls_not_distinct' -%}
            {{ return("") }}
        {%- elif kind == 'unusable_index' -%}
            {%- set index_result = run_query("SELECT index_name FROM user_constraints WHERE constraint_name = '"
                ~ constraint_name | upper ~ "'") -%}
            {%- do run_query('ALTER INDEX "' ~ index_result.rows[0][0] ~ '" UNUSABLE') -%}
        {%- else -%}
            {%- set ddl = enforced_constraint_ddl(kind, column_names, constraint_name, pk_relation, pk_column_names) -%}
            {%- do run_query(ddl ~ ("" if validated else " ENABLE NOVALIDATE")) -%}
        {%- endif -%}
        {%- do log_staged_constraint(kind, constraint_name, validated) -%}
    {%- endif -%}
{%- endmacro -%}


{#- Vertica enforces a primary or unique key only while it is ENABLED, and never
    enforces a foreign key. A foreign key must reference a primary key. -#}
{%- macro vertica__stage_enforced_constraint(kind, column_names, constraint_name, pk_relation, pk_column_names, validated) -%}
    {%- if execute -%}
        {%- if kind == 'not_null' -%}
            {%- for column in column_names -%}
                {%- do run_query("ALTER TABLE " ~ this ~ " ALTER COLUMN " ~ column ~ " SET NOT NULL") -%}
            {%- endfor -%}
        {%- elif kind in ('unique_nulls_not_distinct', 'unusable_index') -%}
            {{ return("") }}
        {%- else -%}
            {%- set ddl = enforced_constraint_ddl(kind, column_names, constraint_name, pk_relation, pk_column_names) -%}
            {%- if kind != 'foreign_key' -%}
                {%- set ddl = ddl ~ (" ENABLED" if validated else " DISABLED") -%}
            {%- endif -%}
            {%- do run_query(ddl) -%}
        {%- endif -%}
        {%- do log_staged_constraint(kind, constraint_name, validated) -%}
    {%- endif -%}
{%- endmacro -%}
