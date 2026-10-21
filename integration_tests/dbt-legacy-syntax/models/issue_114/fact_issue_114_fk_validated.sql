/*
    Issue #114: a validated, enforced foreign key exists when the test runs.

    Every row references dim_issue_114_pk_validated, and the post-hook adds the
    foreign key on PostgreSQL and Oracle. The foreign_key test must be skipped.
*/

{{ config(
    materialized='table',
    post_hook=["{{ stage_enforced_constraint('foreign_key', ['o_orderkey'], 'I114_FK', ref('dim_issue_114_pk_validated'), ['o_orderkey']) }}"]
) }}

SELECT CAST(P.o_orderkey AS {{ dbt.type_int() }}) AS o_orderkey
FROM {{ ref('dim_issue_114_pk_validated') }} P
