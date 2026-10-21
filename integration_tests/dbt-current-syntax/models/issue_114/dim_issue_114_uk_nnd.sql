/*
    Issue #114: a UNIQUE NULLS NOT DISTINCT key rejects repeated NULLs, so it
    proves the unique_key test even on a nullable column. PostgreSQL 15 and
    later support it. The test is skipped on PostgreSQL and runs elsewhere. The
    data holds one NULL, so the test passes everywhere.
*/

{{ config(
    materialized='table',
    post_hook=["{{ stage_enforced_constraint('unique_nulls_not_distinct', ['o_orderkey'], 'I114_UK_NND') }}"]
) }}

SELECT CAST(O.o_orderkey AS {{ dbt.type_int() }}) AS o_orderkey
FROM {{ ref('dim_orders') }} O
UNION ALL
SELECT CAST(NULL AS {{ dbt.type_int() }}) AS o_orderkey {{ 'FROM DUAL' if target.type == 'oracle' else '' }}
