
/*
    Create an artificial duplication of the orders
*/

SELECT O.*
FROM {{ ref('dim_orders') }} O
UNION ALL
SELECT O.*
FROM {{ ref('dim_orders') }} O
