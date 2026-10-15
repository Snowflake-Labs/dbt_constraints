/*
 All Part Suppliers
 */
SELECT PS.*
FROM {{ source('tpc_h', 'source_partsupp') }} PS
