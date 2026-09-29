/*
    Issue #110 regression model — toggleable rely-flip target.

    When `var('issue_110_inject_dup', false)` is false (default), this model
    has unique o_orderkey values and the unique_key test passes, so the
    constraint is created with RELY.

    When the var is true, the model emits duplicate o_orderkey values so the
    unique_key test fails (severity=warn keeps the build green). The package
    must then ALTER ... MODIFY CONSTRAINT ... NORELY on the previously-RELY
    constraint. This is the exact path that regressed in 1.0.5
    (https://github.com/Snowflake-Labs/dbt_constraints/issues/110).
*/

{{ config(materialized='incremental', incremental_strategy='append') }}

{% set inject_dup = var('issue_110_inject_dup', false) %}

{% if is_incremental() %}
    {% if inject_dup %}
    -- Incremental run with the var: append a single duplicate of an
    -- existing o_orderkey so the unique_key test fails and the
    -- constraint must be flipped from RELY to NORELY.
    SELECT *
    FROM (
        SELECT O.*
        FROM {{ this }} O
        LIMIT 1
    )
    {% else %}
    -- Incremental run without the var: emit zero rows (no-op append).
    -- This lets us re-test that an unchanged unique-passing table
    -- causes the constraint to be flipped back to RELY.
    SELECT O.*
    FROM {{ this }} O
    WHERE 1=0
    {% endif %}
{% else %}
-- Full refresh: emit all rows from dim_orders. o_orderkey is unique
-- so the unique_key test passes and the constraint is created RELY.
SELECT O.*
FROM {{ ref('dim_orders') }} O
{% endif %}
