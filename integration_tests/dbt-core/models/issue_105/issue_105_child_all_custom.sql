{{
    config(
        materialized='table',
        database='custom_db',
        schema='test_schema_<env>',
        alias='all_custom_child<suffix>'
    )
}}

-- Child table with FK to parent with ALL customizations
-- This tests that FK creation works when both tables have custom database, schema, and alias
SELECT
    1 AS child_id,
    1 AS parent_id,
    'All Custom Child 1' AS description

UNION ALL

SELECT
    2 AS child_id,
    2 AS parent_id,
    'All Custom Child 2' AS description
