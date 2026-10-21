/*
    Issue #114: a validated, enforced primary key exists when the test runs.

    The post-hook adds the primary key on PostgreSQL and Oracle. With
    dbt_constraints_skip_tests_if_enforced on, the primary_key test must take the
    never-true form and log "skipping validation of primary_key".
    This model is also the parent of the issue #114 foreign keys.
*/

{{ config(
    materialized='table',
    post_hook=["{{ stage_enforced_constraint('primary_key', ['o_orderkey'], 'I114_PK') }}"]
) }}

SELECT CAST(O.o_orderkey AS {{ dbt.type_int() }}) AS o_orderkey
FROM {{ ref('dim_orders') }} O
