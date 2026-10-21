{#- Add an enforced constraint in a model post-hook, before the model's tests run.

    Purpose: exercise issue #114 inside the single preparation build.

    The package releases a table's constraints before it rebuilds the table, and it
    creates constraints in on-run-end, after the tests. So in a full build no test
    ever sees an enforced constraint. A post-hook runs right after the model
    materializes and before its tests, so a constraint that it adds is in place
    when the test is compiled. That is the state of an incremental model on its
    second run, which is what issue #114 describes.

    Arguments:
      kind             'primary_key', 'unique_key' or 'foreign_key'
      column_names     list of columns on this model
      constraint_name  name of the constraint to add
      pk_relation      foreign key only: the parent relation, usually ref('...')
      pk_column_names  foreign key only: the parent columns, in the same order
      validated        false adds a constraint that the database enforces for new
                       rows but has not checked against existing rows:
                       NOT VALID on PostgreSQL, ENABLE NOVALIDATE on Oracle.

    Only PostgreSQL and Oracle enforce these constraints, so every other adapter
    gets a no-op.
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


{%- macro postgres__stage_enforced_constraint(kind, column_names, constraint_name, pk_relation, pk_column_names, validated) -%}
    {%- if execute -%}
        {%- set ddl = enforced_constraint_ddl(kind, column_names, constraint_name, pk_relation, pk_column_names) -%}
        {#- PostgreSQL accepts NOT VALID only on a foreign key, check or not-null constraint. -#}
        {%- do run_query(ddl ~ ("" if validated else " NOT VALID")) -%}
        {%- do log("Test staging: added " ~ ("" if validated else "unvalidated ") ~ kind
            ~ " " ~ constraint_name ~ " on " ~ this.identifier, info=true) -%}
    {%- endif -%}
{%- endmacro -%}


{%- macro oracle__stage_enforced_constraint(kind, column_names, constraint_name, pk_relation, pk_column_names, validated) -%}
    {%- if execute -%}
        {%- set ddl = enforced_constraint_ddl(kind, column_names, constraint_name, pk_relation, pk_column_names) -%}
        {%- do run_query(ddl ~ ("" if validated else " ENABLE NOVALIDATE")) -%}
        {%- do log("Test staging: added " ~ ("" if validated else "unvalidated ") ~ kind
            ~ " " ~ constraint_name ~ " on " ~ this.identifier, info=true) -%}
    {%- endif -%}
{%- endmacro -%}
