{{
    config(
        materialized='table',
        database='custom_db'
    )
}}

-- Parent table with custom database for testing issue #105
-- This tests that FK lookups respect generate_database_name() transformations
SELECT
    1 AS id,
    'Parent Custom DB 1' AS name

UNION ALL

SELECT
    2 AS id,
    'Parent Custom DB 2' AS name
