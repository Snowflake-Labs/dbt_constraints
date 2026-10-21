/*
    Simulate an incremental load of orders

    dbt-vertica defaults to the append strategy, which ignores unique_key. The filter
    below loads the rows of the last order again, so append makes duplicates on Vertica.
    The merge strategy applies unique_key, as the default strategies of the other
    adapters do.
*/

{{ config(
    materialized='incremental',
    unique_key='integration_id',
    incremental_strategy=('merge' if target.type == 'vertica' else none),
    on_schema_change='append_new_columns'
    )
}}

SELECT
  lineitem.*,
  cast(TO_CHAR(o_orderdate, 'YYYYMMDD') AS INTEGER) AS o_orderdate_key,
  coalesce(cast(l_orderkey as varchar(100)), '') || '~' || coalesce(cast(l_linenumber as varchar(100)), '') AS integration_id
FROM {{ ref('lineitem') }} lineitem
JOIN {{ ref('orders') }} orders ON l_orderkey = o_orderkey

{% if is_incremental() -%}

 -- this filter will only be applied on an incremental run
WHERE l_orderkey >=
      ( SELECT coalesce(MAX(l_orderkey), -1) FROM {{ this }} )

{% endif -%}
