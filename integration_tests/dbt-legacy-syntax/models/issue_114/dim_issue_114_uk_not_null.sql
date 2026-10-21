/*
    Issue #114: a unique key whose column has a validated NOT NULL proves both
    the unique_key and the primary_key test, so both are skipped.
*/

{{ config(
    materialized='table',
    post_hook=[
        "{{ stage_enforced_constraint('not_null', ['o_orderkey'], 'I114_UKNN_NN') }}",
        "{{ stage_enforced_constraint('unique_key', ['o_orderkey'], 'I114_UKNN') }}"]
) }}

SELECT CAST(O.o_orderkey AS {{ dbt.type_int() }}) AS o_orderkey
FROM {{ ref('dim_orders') }} O
