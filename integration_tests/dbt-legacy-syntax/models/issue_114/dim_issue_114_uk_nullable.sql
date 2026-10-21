/*
    Issue #114: a unique constraint does not prove the unique_key test.

    The data holds two rows with a NULL key. PostgreSQL and Oracle accept them
    under a UNIQUE constraint, but the unique_key test counts them as a
    duplicate. The column also allows NULL. So the test must NOT be skipped, and
    it must warn.
*/

{{ config(
    materialized='table',
    post_hook=["{{ stage_enforced_constraint('unique_key', ['o_orderkey'], 'I114_UK') }}"]
) }}

SELECT CAST(O.o_orderkey AS {{ dbt.type_int() }}) AS o_orderkey
FROM {{ ref('dim_orders') }} O
UNION ALL
SELECT CAST(NULL AS {{ dbt.type_int() }}) AS o_orderkey {{ 'FROM DUAL' if target.type == 'oracle' else '' }}
UNION ALL
SELECT CAST(NULL AS {{ dbt.type_int() }}) AS o_orderkey {{ 'FROM DUAL' if target.type == 'oracle' else '' }}
