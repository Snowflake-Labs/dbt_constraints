{{
    config(
        materialized='table',
        alias='child_with_alias<suffix>'
    )
}}

-- Child table with FK to parent table with custom alias
-- This tests that FK creation works when both tables have custom aliases
SELECT
    1 AS child_id,
    1 AS parent_id,
    'Child with Alias 1' AS description

UNION ALL

SELECT
    2 AS child_id,
    2 AS parent_id,
    'Child with Alias 2' AS description
