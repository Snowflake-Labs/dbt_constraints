/*
    Issue #110: correcting an existing constraint that has a NON-DEFAULT name.

    1.0.9 fixed two things. The first is that the package updates the rely flag on an
    existing constraint at all. The second is that it finds that constraint when its name
    differs from the one dbt would generate, because `unique_constraint_exists` matches on
    COLUMNS, not on the name.

    Data is UNIQUE, so the constraint should end as RELY. The post-hook pre-creates it as
    NORELY under the name ISSUE_110_HAND_NAMED_UK. The package must find it by its column
    and log "Updating constraint: ISSUE_110_HAND_NAMED_UK RELY", using the EXISTING name
    rather than creating a second constraint under its own generated name.
*/

{{ config(
    materialized='table',
    post_hook=["{{ stage_existing_constraint(['o_orderkey'], 'NORELY', 'ISSUE_110_HAND_NAMED_UK') }}"]
) }}

SELECT O.*
FROM {{ ref('dim_orders') }} O
