/*
    Issue #114: a foreign key must join the same column pairs as the test.

    The constraint is (c1, c2) REFERENCES parent (k1, k2). The test checks
    (c1, c2) against (k2, k1). The column sets match but the pairs do not, so the
    test must not be skipped. The row (1, 2) satisfies the constraint and breaks
    the test, so the test warns. The model runs only where keys are enforced;
    see dim_issue_114_fk_parent2.
*/

{{ config(
    materialized='table',
    enabled=(target.type in ('postgres', 'oracle', 'vertica')),
    post_hook=["{{ stage_enforced_constraint('foreign_key', ['c1', 'c2'], 'I114_FK_CROSSED', ref('dim_issue_114_fk_parent2'), ['k1', 'k2']) }}"]
) }}

SELECT CAST(1 AS {{ dbt.type_int() }}) AS c1, CAST(2 AS {{ dbt.type_int() }}) AS c2 {{ 'FROM DUAL' if target.type == 'oracle' else '' }}
