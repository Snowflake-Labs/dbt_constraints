/*
    Issue #110: the NORELY -> RELY correction, inside a single build.

    Data is UNIQUE, so the unique_key test passes and the constraint should end as RELY.
    The post-hook pre-creates it as NORELY, so the package must correct it and log
    "Updating constraint: ... RELY".

    See macros/stage_existing_constraint.sql for why a post-hook reaches this path.
*/

{{ config(
    materialized='table',
    post_hook=["{{ stage_existing_constraint(['o_orderkey'], 'NORELY') }}"]
) }}

SELECT O.*
FROM {{ ref('dim_orders') }} O
