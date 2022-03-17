# dbt Constraints Package
This package generates database constraints based on the tests in a dbt project. It is compatible with Snowflake only. 

Although Snowflake doesn't enforce most constraints, the [query optimizer does consider primary key, unique key, and foreign key constraints](https://docs.snowflake.com/en/sql-reference/constraints-properties.html?#extended-constraint-properties) during query rewrite if the constraint is set to RELY. Since dbt can test that the data in the table complies with the constraints, we can use this package to create constraints with the RELY property to improve query performance.

## Please note
When you add this package, dbt will automatically begin to create unique keys for all your existing `unique` and `dbt_util.unique_combination_of_columns` tests and foreign keys for existing `relationship` tests. The package also provides three new tests (`primary_key`, `unique_key`, and `foreign_key`) that are a bit more flexible than the standard dbt tests. These tests can be used inline, out-of-line, and can support multiple columns when used in the `tests:` section of a model.

## Installation

1. Add this package to your `packages.yml` following [these instructions](https://docs.getdbt.com/docs/building-a-dbt-project/package-management/)

2. Run `dbt deps`.

3. Optionally add `primary_key`, `unique_key`, or `foreign_key` tests to your model like the following examples.
```yml
  - name: DIM_ORDER_LINES
    columns:
      # Single column inline constraints
      - name: OL_PK
        tests:
          - dbt_constraints.primary_key
      - name: OL_PK
        tests:
          - dbt_constraints.primary_key
      - name: OL_CUSTKEY
        tests:
          - dbt_constraints.foreign_key:
              pk_table_name: ref('DIM_CUSTOMERS')
              pk_column_name: C_CUSTKEY
    tests:
      # Single column constraints
      - dbt_constraints.primary_key:
          column_name: OL_PK
      - dbt_constraints.unique_key:
          column_names: OL_ORDERKEY
      - dbt_constraints.foreign_key:
          fk_column_name: OL_CUSTKEY
          pk_table_name: ref('DIM_CUSTOMERS')
          pk_column_name: C_CUSTKEY
      # Multiple column constraints
      - dbt_constraints.primary_key:
          column_names:
            - OL_PK_COLUMN_1
            - OL_PK_COLUMN_2
      - dbt_constraints.unique_key:
          column_names:
            - OL_UK_COLUMN_1
            - OL_UK_COLUMN_2
      - dbt_constraints.foreign_key:
          fk_column_names:
            - OL_FK_COLUMN_1
            - OL_FK_COLUMN_2
          pk_table_name: ref('DIM_CUSTOMERS')
          pk_column_names:
            - C_PK_COLUMN_1
            - C_PK_COLUMN_2

```

## DBT_CONSTRAINTS Limitations
Generally, if you don't meet a requirement, tests are still executed but the constraint is skipped rather than producing an error
    * All models involved in a constraint must be materialized as table, incremental, or snapshot
    * Constraints will not be created on sources, only models. You can use the PK/UK/FK tests with sources but constraints won't be generated.
    * All columns on constraints must be individual column names, not expressions. You can reference columns on a model that come from an expression.
    * Constraints are not created for failed tests
    * `primary_key`, `unique_key`, and `foreign_key` constraints are considered first and duplicate constraints are skipped. One exception is that you will get an error if you add two different `primary_key` tests to the same model.
    * Foreign keys require that the parent table have a primary key or unique key on the referenced columns. Unique keys generated from standard `unique` tests are sufficient.
    * The order of columns on a foreign key test must match betweek the FK columnns and PK columns
    * The `foreign_key` test will ignore any rows with a null column, even if only one of two columns in a compound key is null. If you also want to ensure FK columns are not null, you should add standard `not_null` tests to your model.
    * DBT_CONSTRAINTS expects constraints to follow a very specific, deterministic naming convention. If you get errors because DBT_CONSTRAINTS is trying to create duplicate constraints, you should drop any previous PK/UK/FK constraints and allow DBT_CONSTRAINTS to create the constraints. This is the naming convention:
    {table_name}_{alphabetically_sorted_col_1}_{alphabetically_sorted_col_2}_{alphabetically_sorted_col_3}_{PK, UK, or FK}