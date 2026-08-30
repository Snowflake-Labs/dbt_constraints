{{
    config(
        materialized='table',
        schema='test_schema_<env>'
    )
}}

-- Parent table with custom schema for testing issue #105
-- This tests that FK lookups respect generate_schema_name() transformations
SELECT
    1 AS id,
    'Parent Record 1' AS name

UNION ALL

SELECT
    2 AS id,
    'Parent Record 2' AS name
