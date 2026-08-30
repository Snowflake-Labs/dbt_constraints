{{
    config(
        materialized='table',
        database='custom_db',
        schema='test_schema_<env>',
        alias='all_custom_parent<suffix>'
    )
}}

-- Parent table with ALL customizations for testing issue #105
-- This tests that FK lookups respect all generate_*_name() transformations simultaneously
SELECT
    1 AS id,
    'All Custom Parent 1' AS name

UNION ALL

SELECT
    2 AS id,
    'All Custom Parent 2' AS name
