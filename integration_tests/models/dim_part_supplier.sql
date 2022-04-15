/*
 All Part Suppliers
 */
SELECT PS.*
FROM {{ source('tpc_h', 'partsupp') }} PS
