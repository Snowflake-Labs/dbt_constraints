{{ config(materialized = 'view') }}
/*
 All Customers
 */
SELECT *
FROM {{ ref('dim_customers') }}
