/*
    Issue #114: an Oracle primary key whose index is UNUSABLE does not prove the
    primary_key test. A direct-path load that finds duplicates leaves the index
    in this state while the constraint stays ENABLED VALIDATED. The second hook
    marks the index UNUSABLE on Oracle, so the test runs there. Elsewhere it is a
    no-op, and the test is skipped.
*/

{{ config(
    materialized='table',
    post_hook=[
        "{{ stage_enforced_constraint('primary_key', ['o_orderkey'], 'I114_PK_UNUSABLE') }}",
        "{{ stage_enforced_constraint('unusable_index', [], 'I114_PK_UNUSABLE') }}"]
) }}

SELECT CAST(O.o_orderkey AS {{ dbt.type_int() }}) AS o_orderkey
FROM {{ ref('dim_orders') }} O
