/*
    Issue #114: the parent of fact_issue_114_fk_crossed, with a primary key on
    (k1, k2). Both models run only on the databases that enforce keys. Elsewhere
    the package would create the crossed test's foreign key in on-run-end, and
    Snowflake rejects a foreign key whose columns are not in primary key order.
*/

{{ config(
    materialized='table',
    enabled=(target.type in ('postgres', 'oracle', 'vertica')),
    post_hook=["{{ stage_enforced_constraint('primary_key', ['k1', 'k2'], 'I114_PK2') }}"]
) }}

SELECT CAST(1 AS {{ dbt.type_int() }}) AS k1, CAST(2 AS {{ dbt.type_int() }}) AS k2 {{ 'FROM DUAL' if target.type == 'oracle' else '' }}
