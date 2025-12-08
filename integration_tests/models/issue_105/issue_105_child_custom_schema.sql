{{
    config(
        materialized='table',
        schema='test_schema_<env>'
    )
}}

-- Child table with FK to parent table with custom schema
-- This tests that FK creation works when both tables have custom schemas
SELECT
    1 AS child_id,
    1 AS parent_id,
    'Child Record 1' AS description

UNION ALL

SELECT
    2 AS child_id,
    2 AS parent_id,
    'Child Record 2' AS description
