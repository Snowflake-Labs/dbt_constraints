{#- Pre-create a unique constraint with a deliberately WRONG rely flag.

    Purpose: exercise the issue #110 code path inside a SINGLE dbt build.

    The issue #110 bug was that the package stopped issuing
    ALTER TABLE ... MODIFY CONSTRAINT ... RELY/NORELY on a constraint that ALREADY
    existed. A normal first build cannot reach that path, because the package finds no
    constraint and takes the CREATE branch instead.

    A model post-hook runs immediately after the model materializes, and the package
    creates its constraints in an on-run-end hook, which runs later. So a post-hook that
    adds the constraint first leaves the package facing an EXISTING constraint. If that
    constraint carries the wrong flag for the model's data, the package must correct it,
    and it logs "Updating constraint: <name> <RELY|NORELY>".

    This means one build tests both directions and the alternate-name case, with no extra
    dbt invocation. Compare the older approach, which rebuilt the same table across
    several runs to reach each state.

    Arguments:
      column_names     list of columns for the unique constraint
      rely             'RELY' or 'NORELY', the WRONG value the package must correct
      constraint_name  optional. Pass a non-default name to check that the package finds
                       an existing constraint by its COLUMNS rather than by its name,
                       which is the second half of the 1.0.9 fix.

    RELY and NORELY are Snowflake-only, so every other adapter gets a no-op and the
    package simply creates the constraint as usual.
-#}
{%- macro stage_existing_constraint(column_names, rely, constraint_name=none) -%}
    {{ return(adapter.dispatch('stage_existing_constraint', 'dbt_constraints_integration_tests')(column_names, rely, constraint_name)) }}
{%- endmacro -%}


{#- No-op. Only Snowflake carries a rely flag on a constraint. -#}
{%- macro default__stage_existing_constraint(column_names, rely, constraint_name) -%}
{%- endmacro -%}


{%- macro snowflake__stage_existing_constraint(column_names, rely, constraint_name) -%}
    {%- if execute -%}
        {%- set name = constraint_name
            or (this.identifier ~ "_" ~ column_names | join("_") ~ "_UK") | upper -%}
        {%- set columns_csv = column_names | join(", ") -%}

        {#- No DROP first. These models are materialized='table', so each run replaces
            the table with CREATE OR REPLACE and the replacement carries no constraint.

            Do NOT add `DROP CONSTRAINT IF EXISTS` here as a guard. Snowflake rejects it:
            "syntax error ... unexpected 'EXISTS'". Snowflake has no IF EXISTS clause on
            DROP CONSTRAINT. -#}
        {%- do run_query(
            "ALTER TABLE " ~ this ~ " ADD CONSTRAINT " ~ name
            ~ " UNIQUE (" ~ columns_csv ~ ") " ~ rely
        ) -%}
        {%- do log(
            "Test staging: pre-created " ~ name ~ " as " ~ rely ~ " on " ~ this.identifier,
            info=true
        ) -%}
    {%- endif -%}
{%- endmacro -%}
