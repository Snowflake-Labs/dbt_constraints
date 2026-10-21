/*
    Issue #114: the tests compare column names without case, so a key on
    "OrderKey" must not prove a test on orderkey when the table has both.

    The primary key is on "OrderKey", which is unique. The unique_key test is on
    orderkey, which holds one value on every row. The test must not be skipped,
    and it warns. The model runs only on PostgreSQL and Oracle, the databases
    that enforce keys and allow both names. Vertica cannot create two columns
    whose names differ only in case.
*/

{{ config(
    materialized='table',
    enabled=(target.type in ('postgres', 'oracle')),
    post_hook=["{{ stage_enforced_constraint('primary_key', ['\"OrderKey\"'], 'I114_CASE_PK') }}"]
) }}

SELECT CAST(O.o_orderkey AS {{ dbt.type_int() }}) AS "OrderKey",
    CAST(1 AS {{ dbt.type_int() }}) AS orderkey
FROM {{ ref('dim_orders') }} O
