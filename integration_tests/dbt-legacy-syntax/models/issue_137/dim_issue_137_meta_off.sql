/*
    Issue #137: a test that sets dbt_constraints_enabled: false in config.meta
    must not create a constraint, even though config leaves the flag unset.
*/

{{ config(materialized='table') }}

SELECT CAST(O.o_orderkey AS {{ dbt.type_int() }}) AS o_orderkey
FROM {{ ref('dim_orders') }} O
