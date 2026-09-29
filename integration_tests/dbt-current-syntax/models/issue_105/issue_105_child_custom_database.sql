{{
    config(
        materialized='table',
        database='custom_db'
    )
}}

-- Child table with FK to parent table with custom database
-- This tests that FK creation works when both tables have custom databases
SELECT
    1 AS child_id,
    1 AS parent_id,
    'Child Custom DB 1' AS description

UNION ALL

SELECT
    2 AS child_id,
    2 AS parent_id,
    'Child Custom DB 2' AS description
