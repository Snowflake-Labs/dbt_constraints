# dbt Constraints Integration Tests
This set of models and associated tests is designed to test all the supported tests as well as most unsupported scenarios.

## Environment
A set of TPC-H test data has been included as seeds to test the validity of primary keys, unique keys, and foreign keys.

## Running the tests
1. Set up a `dbt_constraints` profile in your ~/.dbt/profiles.yml to a Snowflake or PostgreSQL schema you can create tables and views in.
2. Execute `dbt seed`
3. Execute `dbt build`

A successful execution of the project should result in the following messages about constraints being created:
```
Creating primary key: fact_order_line_missing_orders_l_linenumber_l_orderkey_PK
Creating primary key: dim_customers_c_custkey_PK
Creating primary key: dim_part_p_partkey_PK
Creating primary key: fact_order_line_l_linenumber_l_orderkey_PK
Creating unique key: dim_customers_c_custkey_HASH_UK
Creating unique key: dim_customers_c_custkey_seq_UK
Creating unique key: dim_part_p_partkey_HASH_UK
Creating unique key: dim_part_p_partkey_seq_UK
Creating unique key: dim_part_supplier_ps_partkey_ps_suppkey_UK
Creating unique key: dim_orders_null_keys_o_orderkey_HASH_UK
Creating unique key: fact_order_line_missing_orders_integration_id_UK
Creating unique key: dim_orders_o_orderkey_UK
Creating unique key: fact_order_line_integration_id_UK
Creating foreign key: dim_orders_o_custkey_FK referencing dim_customers ['c_custkey']
Skipping fact_order_line_missing_orders_l_partkey_l_suppkey_FK because a PK/UK was not found on the PK table: "DFLIPPO_DEV"."DBT_DEMO"."dim_part_supplier_missing_con" ['ps_partkey', 'ps_suppkey']
Creating foreign key: fact_order_line_l_partkey_l_suppkey_FK referencing dim_part_supplier ['ps_partkey', 'ps_suppkey']
Creating foreign key: dim_orders_null_keys_o_custkey_FK referencing dim_customers ['c_custkey']
Creating foreign key: fact_order_line_l_orderkey_FK referencing dim_orders ['o_orderkey']
```

Also, 4 errors should be reported by models that have been designed to test failures with messages like the following:
```
Completed with 4 warnings:
Warning in test dbt_constraints_unique_key_dim_duplicate_orders_o_orderkey_HASH (models/schema.yml)
  Got 938 results, configured to warn if != 0
Warning in test dbt_constraints_primary_key_dim_duplicate_orders_o_orderkey (models/schema.yml)
  Got 938 results, configured to warn if != 0
Warning in test dbt_constraints_foreign_key_fact_order_line_missing_orders_l_orderkey__o_orderkey__ref_dim_missing_orders_ (models/schema.yml)
  Got 484 results, configured to warn if != 0
Warning in test dbt_constraints_primary_key_dim_orders_null_keys_o_orderkey (models/schema.yml)
  Got 1 result, configured to warn if != 0
```
