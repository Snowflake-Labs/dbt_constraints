/*
    Simulate an incremental load of orders
*/

SELECT 
  lineitem.*,
  coalesce(l_orderkey::varchar, '') || '~' || coalesce(l_linenumber::varchar, '') AS integration_id
FROM {{ source('tpc_h', 'lineitem') }} lineitem

