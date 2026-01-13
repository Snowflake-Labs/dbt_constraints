
/*
    Create an artificial subset of the orders
*/

SELECT *
FROM
{{ ref('dim_orders') }}

-- This line will cause a FK violation
WHERE MOD(o_orderkey, 2) = 0
