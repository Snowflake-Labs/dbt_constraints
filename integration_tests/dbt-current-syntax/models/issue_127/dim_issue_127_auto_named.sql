/*
    Issue #127: correcting an existing constraint whose name Snowflake generated.

    The post-hook adds a NORELY unique constraint with no name. Snowflake names it
    SYS_CONSTRAINT_<uuid>. The uuid contains hyphens and lower-case letters, so the
    package must quote the name in ALTER TABLE ... MODIFY CONSTRAINT.

    Data is UNIQUE, so the package must log
    "Updating constraint: SYS_CONSTRAINT_<uuid> RELY". Without the quotes, Snowflake
    rejects the statement with "unexpected '-'" and the build fails.
*/

{{ config(
    materialized='table',
    post_hook=["{{ stage_existing_constraint(['o_orderkey'], 'NORELY', auto_name=true) }}"]
) }}

SELECT O.*
FROM {{ ref('dim_orders') }} O
