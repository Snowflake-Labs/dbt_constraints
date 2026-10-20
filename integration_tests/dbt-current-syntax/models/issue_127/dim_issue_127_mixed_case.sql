/*
    Issue #127: correcting an existing constraint whose name has mixed case and a quote.

    The post-hook adds a NORELY unique constraint named "Issue_127_""Mixed""_Case_UK".
    Snowflake stores it as Issue_127_"Mixed"_Case_UK. The package reads that name from
    SHOW UNIQUE KEYS. It must quote the name to keep the case, and double the embedded
    quote characters.

    Data is UNIQUE, so the package must log
    "Updating constraint: Issue_127_"Mixed"_Case_UK RELY".
*/

{{ config(
    materialized='table',
    post_hook=["{{ stage_existing_constraint(['o_orderkey'], 'NORELY', '\"Issue_127_\"\"Mixed\"\"_Case_UK\"') }}"]
) }}

SELECT O.*
FROM {{ ref('dim_orders') }} O
