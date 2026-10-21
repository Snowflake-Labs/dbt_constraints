/*
    Issue #114: an unvalidated foreign key does not prove the foreign_key test.

    One row references a key that the parent lacks. The post-hook adds the
    foreign key NOT VALID on PostgreSQL and ENABLE NOVALIDATE on Oracle, which
    the database accepts over the orphan. The test must NOT be skipped, and it
    must warn.
*/

{{ config(
    materialized='table',
    post_hook=["{{ stage_enforced_constraint('foreign_key', ['o_orderkey'], 'I114_FK_NV', ref('dim_issue_114_pk_validated'), ['o_orderkey'], validated=false) }}"]
) }}

SELECT CAST(P.o_orderkey AS {{ dbt.type_int() }}) AS o_orderkey
FROM {{ ref('dim_issue_114_pk_validated') }} P
UNION ALL
SELECT CAST(-1 AS {{ dbt.type_int() }}) AS o_orderkey {{ 'FROM DUAL' if target.type == 'oracle' else '' }}
