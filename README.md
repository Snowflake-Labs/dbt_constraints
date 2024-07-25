# dbt Constraints Package

This package generates database constraints based on the tests in a dbt project. It is currently compatible with Snowflake, PostgreSQL, Oracle, Redshift, and Vertica only.

## How the dbt Constraints Package differs from dbt's Model Contracts feature

This package focuses on automatically generating constraints based on the tests already in a user's dbt project. In most cases, merely adding the dbt Constraints package is all that is needed to generate constraints. dbt's recent [model contracts feature](https://docs.getdbt.com/docs/collaborate/govern/model-contracts) allows users to explicitly document constraints for models in yml. This package and the core feature are 100% compatible with one another and the dbt Constraints package will skip generating constraints already created by a model contract. However, the dbt Constraints package will also generate constraints for any tests that are not documented as model contracts. As described in the next section, dbt Constraints is also designed to provide join elimination on Snowflake.

## Why data engineers should add referential integrity constraints

The primary reason to add constraints to your database tables is that many tools including [DBeaver](https://dbeaver.io) and [Oracle SQL Developer Data Modeler](https://community.snowflake.com/s/article/How-To-Customizing-Oracle-SQL-Developer-Data-Modeler-SDDM-to-Support-Snowflake-Variant) can correctly reverse-engineer data model diagrams if there are primary keys, unique keys, and foreign keys on tables. Most BI tools will also add joins automatically between tables when you import tables that have foreign keys. This can both save time and avoid mistakes.

In addition, although Snowflake doesn't enforce most constraints, the [query optimizer can consider primary key, unique key, and foreign key constraints](https://docs.snowflake.com/en/sql-reference/constraints-properties.html?#extended-constraint-properties) during query rewrite if the constraint is set to RELY. Since dbt can test that the data in the table complies with the constraints, this package creates constraints on Snowflake with the RELY property to improve query performance. Some database query optimizers also consider not null constraints when building an execution plan.

Many databases including [Snowflake](https://docs.snowflake.com/en/user-guide/join-elimination.html), PostgreSQL, Oracle, SQL Server, MySQL, and DB2 can use referential integrity constraints to perform "[Join Elimination](https://blog.jooq.org/join-elimination-an-essential-optimiser-feature-for-advanced-sql-usage/)" to remove tables from an execution plan. This commonly occurs when you query a subset of columns from a view and some of the tables in the view are unnecessary. In addition, on databases that do not support join elimination, some [BI and visualization tools will also rewrite their queries](https://docs.snowflake.com/en/user-guide/table-considerations.html#referential-integrity-constraints) based on constraint information, producing the same effect.

Finally, although most columnar databases including Snowflake do not use or need indexes, most row-oriented databases including PostgreSQL and Oracle require indexes on their primary key columns in order to perform efficient joins between tables. A primary key or unique key constraint is typically enforced on databases using such indexes. Having dbt create the unique indexes automatically can slightly reduce the degree of performance tuning necessary for row-oriented databases. Row-oriented databases frequently also need indexes on foreign key columns but [that is something best added manually](https://docs.getdbt.com/reference/resource-configs/postgres-configs#indexes).

## Please note

When you add this package, dbt will automatically begin to create __unique keys__ for all your existing `unique` and `dbt_utils.unique_combination_of_columns` tests, __foreign keys__ for existing `relationship` tests, and __not null constraints__ for `not_null` tests. The package also provides three new tests (`primary_key`, `unique_key`, and `foreign_key`) that are a bit more flexible than the standard dbt tests. These tests can be used inline, out-of-line, and can support multiple columns when used in the `tests:` section of a model. The `primary_key` test will also cause a not null constraint to be created on each column.

### Disabling automatic constraint generation

The `dbt_constraints_enabled` variable can be set to `false` in your project to disable automatic constraint generation. By default dbt Constraints only creates constraints on models. To allow constraints on sources, you can set `dbt_constraints_sources_enabled` to `true`. The package will verify that you have sufficient database privileges to create constraints on sources.

```yml
vars:
  # The package can be temporarily disabled using this variable
  dbt_constraints_enabled: true

  # The package can also add constraints on sources if you have sufficient privileges
  dbt_constraints_sources_enabled: false

  # You can also be specific on which constraints are enabled for sources
  # You must also enable dbt_constraints_sources_enabled above
  dbt_constraints_sources_pk_enabled: true
  dbt_constraints_sources_uk_enabled: true
  dbt_constraints_sources_fk_enabled: true
  dbt_constraints_sources_nn_enabled: true
```

## Installation

1. Add this package to your `packages.yml` following [these instructions](https://docs.getdbt.com/docs/building-a-dbt-project/package-management/). Please check [this link for the latest released version](https://github.com/Snowflake-Labs/dbt_constraints/releases/latest).

```yml
packages:
  - package: Snowflake-Labs/dbt_constraints
    version: [">=1.0.0", "<1.1.0"]
# <see https://github.com/Snowflake-Labs/dbt_constraints/releases/latest> for the latest version tag.
# You can also pull the latest changes from Github with the following:
#  - git: "https://github.com/Snowflake-Labs/dbt_constraints.git"
#    revision: main
```

2. Run `dbt deps`.

3. Optionally add `primary_key`, `unique_key`, or `foreign_key` tests to your model like the following examples.

```yml
  - name: DIM_ORDER_LINES
    columns:
      # Single column inline constraints
      - name: OL_PK
        tests:
          - dbt_constraints.primary_key
      - name: OL_UK
        tests:
          - dbt_constraints.unique_key
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
          column_name: OL_ORDERKEY
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

### Dependencies and Requirements

* The package's macros depend on the results and graph object schemas of dbt >=1.0.0

* The package currently only includes macros for creating constraints in Snowflake, PostgreSQL, and Oracle. To add support for other databases, it is necessary to implement the following seven macros with the appropriate DDL & SQL for your database. Pull requests to contribute support for other databases are welcome. See the <ADAPTER_NAME>__create_constraints.sql files as examples.

```sql
<ADAPTER_NAME>__create_primary_key(table_model, column_names, verify_permissions, quote_columns=false, constraint_name=none, lookup_cache=none)
<ADAPTER_NAME>__create_unique_key(table_model, column_names, verify_permissions, quote_columns=false, constraint_name=none, lookup_cache=none)
<ADAPTER_NAME>__create_foreign_key(pk_model, pk_column_names, fk_model, fk_column_names, verify_permissions, quote_columns=false, constraint_name=none, lookup_cache=none)
<ADAPTER_NAME>__create_not_null(pk_model, pk_column_names, fk_model, fk_column_names, verify_permissions, quote_columns=false, lookup_cache=none)
<ADAPTER_NAME>__unique_constraint_exists(table_relation, column_names, lookup_cache=none)
<ADAPTER_NAME>__foreign_key_exists(table_relation, column_names, lookup_cache=none)
<ADAPTER_NAME>__have_references_priv(table_relation, verify_permissions, lookup_cache=none)
<ADAPTER_NAME>__have_ownership_priv(table_relation, verify_permissions, lookup_cache=none)
```

## RELY and NORELY Properties

Version 1.0.0 introduces the ability to create constraints with the RELY and NORELY properties on Snowflake. Executed tests with zero failures are created with the `RELY` property. Tests with any failures will generate `NORELY` constraints and constraints will be altered to `RELY` or `NORELY` based on subsequent executions of the test. When the `always_create_constraint` feature is enabled, it is now also possible to create `NORELY` constraints using `dbt run` and then have those constraints become RELY constraints using `dbt test`.

## Determining the Constraints to Generate

Version 1.0.0 introduces a more advanced set of criteria for selecting tests to turn into constraints.

* The test must be one of the following: `primary_key`, `unique_key`, `unique_combination_of_columns`, `unique`, `foreign_key`, `relationships`, or `not_null`
* The test executed and had zero failures (RELY) or the database has support for NORELY constraints
* The test executed (RELY/NORELY), we need the primary/unique key constraint for a foreign key, or we have the `always_create_constraint` parameter turned on.
* If you are using `build`, `run`, or `test` for only part of a project using the `--select` parameter, either the test or its model was selected to run, or the test is a primary/unique key that is needed for a selected foreign key. If a primary/unique key is created for a foreign key, and its test was not executed, the primary/unique key will be created as a NORELY constraint.
* All models involved in a constraint must **not** be a view or ephemeral materialization. Version 1.0.0 now allows custom materializations.
* If source constraints are enabled, the source must be a table. You must also have the `OWNERSHIP` table privilege to add a constraint. For foreign keys you also need the `REFERENCES` privilege on the parent table with the primary or unique key. The package will identify when you lack these privileges on Snowflake and PostgreSQL. Oracle does not provide an easy way to look up your effective privileges so it has an exception handler and will display Oracle's error messages.
* All columns on constraints must be individual column names, not expressions. You can reference columns on a model that come from an expression.
* `primary_key`, `unique_key`, and `foreign_key` tests are considered first and duplicate constraints are skipped. One exception is that you will get an error if you add two different `primary_key` tests to the same model.
* Foreign keys require that the parent table have a primary key or unique key on the referenced columns. Unique keys generated from standard `unique` tests are sufficient.
* The order of columns on a foreign key test must match between the FK columns and PK columns
* Referential constraints must apply to all the rows in a table so any tests with a `config: where:`, `config: warn_if:`, or `config: fail_calc:` property will be set as `NORELY` when creating constraints.

Additional notes:
* The `foreign_key` test will ignore any rows with a null column, even if only one of two columns in a compound key is null. If you also want to ensure FK columns are not null, you should add standard `not_null` tests to your model which will add not null constraints to the table.
* You may need to manually drop a primary key constraint from a table if you change the columns in the constraint. This is not necessary for table materializations or if you do a full-refresh of an incremental model.

## Advanced: `always_create_constraint` Property

There is an advanced option for Snowflake users to force a constraint to be generated even when the test was not executed. When this setting is in effect, constraints on Snowflake will have the `NORELY` property until the associated test is executed with zero failures. Snowflake does not support `NORELY` for not null constraints so those constraints will still be skipped. You activate this feature in your dbt_project.yml under the `models:` or `tests:` sections. You can set it to be true for your entire project or you can specify specific folders that should use this feature. You can also set this in a specific model's header.

__[Caveat Emptor](https://en.wikipedia.org/wiki/Caveat_emptor):__

* You will get an error if you try to force constraints to be generated that are enforced by your database. On Snowflake that is only a not_null constraint but on databases like Oracle, all the generated constraints are enforced. This is why, at present, only the Snowflake macros implement this feature.
* This feature can still cause unexpected query results on Snowflake due to [join elimination](https://docs.snowflake.com/en/user-guide/join-elimination). Although executing tests on Snowflake will correctly set the `RELY` or `NORELY` property based on whether the tests pass and fail, activating this feature and **skipping the execution of tests** will not cause a `RELY` constraint to become a `NORELY` constraint. A `RELY` constraint only becomes a `NORELY` constraint **if a test is executed** and has failures. If you create a `RELY` constraint by running `dbt build` and subsequently only execute `dbt run` without eventually following up with `dbt test`, you could have constraints that still have the `RELY` property but now have referential integrity issues. Snowflake users are encouraged to frequently or always execute their tests so that the `RELY` property is kept up to date.

These are examples from a dbt_project.yml using the feature in models or tests:

```yml
models:
  your_project_name:
    +always_create_constraint: true
tests:
  your_project_name:
    +always_create_constraint: true
```

This is an example from a model schema.yml using the feature. Setting the property in the `config:` section of a test does not work so you should set it in the model's `config:` section.

```yml
version: 2

models:
  - name: your_model_name
    config:
      always_create_constraint: true
```

This is an example of activating the feature in the header of a model:
```jinja
{{ config(always_create_constraint = true) }}
```

## Primary Maintainers

* Dan Flippo ([@sfc-gh-dflippo](https://github.com/sfc-gh-dflippo))

This is a community-developed package, not an official Snowflake offering. It comes with no support or warranty. However, feel free to raise a github issue if you find a bug or would like a new feature.

## Legal

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this package except in compliance with the License. You may obtain a copy of the License at: [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
