/*
    Simulate an incremental load of orders
*/

SELECT
  lineitem.*,
  coalesce(cast(l_orderkey as varchar(100)), '') || '~' || coalesce(cast(l_linenumber as varchar(100)), '') AS integration_id
FROM {{ ref('lineitem') }} lineitem
