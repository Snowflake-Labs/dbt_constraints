/*
    Issue #114: a primary key that the database has not validated does not prove
    the primary_key test. The key is DISABLED on Vertica and ENABLE NOVALIDATE on
    Oracle. PostgreSQL cannot add an unvalidated key, so it adds none. The test
    runs everywhere and passes.
*/

{{ config(
    materialized='table',
    post_hook=["{{ stage_enforced_constraint('primary_key', ['o_orderkey'], 'I114_PK_NV', validated=false) }}"]
) }}

SELECT CAST(O.o_orderkey AS {{ dbt.type_int() }}) AS o_orderkey
FROM {{ ref('dim_orders') }} O
