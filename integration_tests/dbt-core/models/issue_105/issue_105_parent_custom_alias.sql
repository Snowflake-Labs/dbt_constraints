{{
    config(
        materialized='table',
        alias='parent_with_alias<suffix>'
    )
}}

-- Parent table with custom alias for testing issue #105
-- This tests that FK lookups respect generate_alias_name() transformations
SELECT
    1 AS id,
    'Parent with Alias 1' AS name

UNION ALL

SELECT
    2 AS id,
    'Parent with Alias 2' AS name
