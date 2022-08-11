/*
 Test
dim_order_copy____________
*/
SELECT
  l_orderkey as l_____________________orderkey,
  l_linenumber as l___________________linenumber,
  l_partkey as l______________________partkey,
  l_suppkey l______________________suppkey,
  integration_id as l_______________integration_id
FROM
{{ ref('fact_order_line') }} O
