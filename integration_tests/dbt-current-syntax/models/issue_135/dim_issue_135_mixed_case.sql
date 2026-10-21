/*
    Issue #135: the package must quote a column when the test sets quote_columns.

    The column name OrderKey has mixed case, so it exists only as a quoted
    identifier. The primary_key test sets quote_columns: true, and the hook's
    global quote_columns stays false. Before the fix, the package quoted the test
    SQL but not the ALTER TABLE, which then named an ORDERKEY or orderkey column
    that does not exist.
*/

{{ config(materialized='table') }}

SELECT CAST(O.o_orderkey AS {{ dbt.type_int() }}) AS {{ adapter.quote('OrderKey') }}
FROM {{ ref('dim_orders') }} O
