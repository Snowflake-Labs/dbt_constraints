/*
    Simulate an incremental load of orders
*/

{{ config(
    materialized='incremental',
    unique_key='integration_id',
    on_schema_change='append_new_columns'
    )
}}

SELECT
  lineitem.*,
  cast(TO_CHAR(o_orderdate, 'YYYYMMDD') AS INTEGER) AS o_orderdate_key,
  coalesce(cast(l_orderkey as varchar(100)), '') || '~' || coalesce(cast(l_linenumber as varchar(100)), '') AS integration_id
FROM {{ source('tpc_h', 'lineitem') }} lineitem
JOIN {{ source('tpc_h', 'orders') }} orders ON l_orderkey = o_orderkey

{% if is_incremental() -%}

 -- this filter will only be applied on an incremental run
WHERE l_orderkey >=
      ( SELECT coalesce(MAX(l_orderkey), -1) FROM {{ this }} )

{% endif -%}
