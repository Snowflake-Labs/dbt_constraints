/*
    Issue #110: the RELY -> NORELY correction, inside a single build.

    Data is DUPLICATED, so the unique_key test fails (severity=warn) and the constraint
    should end as NORELY. The post-hook pre-creates it as RELY, so the package must
    correct it and log "Updating constraint: ... NORELY".

    The duplicate is a plain UNION ALL rather than a LIMIT. LIMIT is not portable: Oracle
    needs FETCH FIRST, and this model still builds on every adapter.
*/

{{ config(
    materialized='table',
    post_hook=["{{ stage_existing_constraint(['o_orderkey'], 'RELY') }}"]
) }}

SELECT O.*
FROM {{ ref('dim_orders') }} O
UNION ALL
SELECT O.*
FROM {{ ref('dim_orders') }} O
