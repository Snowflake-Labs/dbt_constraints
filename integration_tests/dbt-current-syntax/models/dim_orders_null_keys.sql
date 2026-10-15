
/*
    Simulate missing PK values
*/

SELECT
    CASE WHEN MOD(o_orderkey, 10) = 0 THEN o_orderkey ELSE NULL END AS o_orderkey,
    o_orderkey_seq,
    o_custkey,
    o_orderstatus,
    o_totalprice,
    o_orderdate,
    o_order_priority,
    o_clerk,
    o_shippriority,
    o_comment
FROM
{{ ref('dim_orders') }}
