/*
    FK parity guard for dbt Fusion.

    Asserts that on Snowflake the package created at least the expected number of
    FOREIGN KEY constraints in the test schema. This catches a regression of the
    upstream Fusion bug
    (dbt-fusion#1575) where test_metadata.kwargs would lose the `to` / `field`
    arguments for parameterised generic tests, causing the package to silently
    skip every FK with the "missing from test parameters" log line.

    This project also runs against PostgreSQL and Oracle. The guard returns zero
    rows on any target that is not Snowflake. See the else branch below.

    Lower bound only: counts FKs created by THIS Fusion build's models.
    Run after `dbt build`. Fails (returns rows) if FK count < the lower bound.

    IMPORTANT: this test does nothing unless you pass the assert_fk_parity variable. The
    package creates constraints in an on-run-end hook, which runs AFTER the tests, so on a
    `--full-refresh` build the constraints do not exist yet while this test runs. Run it in
    a later command, the same way assert_source_constraints.sql is run:

        dbt build
        dbt build --vars '{assert_fk_parity: true}'

    The expected list is conservative — only constraints whose underlying
    `relationships` / `foreign_key` test definitions live in this project's
    schema.yml files. If you add new FK tests, bump this list.
*/

{% set expected_fks = [
    'DIM_ORDERS_O_CUSTKEY_FK',
    'DIM_ORDERS_NULL_KEYS_O_CUSTKEY_FK',
    'DIM_PART_SUPPLIER_PS_SUPPKEY_FK',
    'FACT_ORDER_LINE_L_ORDERKEY_FK',
    'FACT_ORDER_LINE_L_PARTKEY_L_SUPPKEY_FK',
    'ALL_CUSTOM_CHILD_TEST_PARENT_ID_FK',
    'CHILD_WITH_ALIAS_TEST_PARENT_ID_FK',
    'ISSUE_105_CHILD_CUSTOM_DATABASE_PARENT_ID_FK',
    'ISSUE_105_CHILD_CUSTOM_SCHEMA_PARENT_ID_FK',
] %}

{% if target.type == 'snowflake' and var('assert_fk_parity', false) %}

WITH expected AS (
    {% for c in expected_fks %}
    SELECT '{{ c }}' AS constraint_name{% if not loop.last %} UNION ALL{% endif %}
    {% endfor %}
),
actual AS (
    {#- Deliberately NOT filtered to target.schema. The expected list spans schemas and
        databases: the issue_105 models use custom generate_database_name and
        generate_schema_name macros, so ISSUE_105_CHILD_CUSTOM_DATABASE_PARENT_ID_FK and
        ALL_CUSTOM_CHILD_TEST_PARENT_ID_FK do not live in target.schema at all.

        The trade-off is that another schema in the same database can satisfy this count.
        Restricting it to target.schema makes the cross-schema constraints unfindable and
        the test fails for the wrong reason. -#}
    SELECT constraint_name
    FROM {{ target.database }}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS
    WHERE constraint_type = 'FOREIGN KEY'
)
SELECT 'MISSING_FK' AS issue, e.constraint_name
FROM expected e
LEFT JOIN actual a USING (constraint_name)
WHERE a.constraint_name IS NULL

{% else %}

-- Non-Snowflake target: skip (no INFORMATION_SCHEMA constraint visibility
-- guaranteed). Return zero rows.
SELECT 'SKIPPED' AS issue, '' AS constraint_name
WHERE 1 = 0

{% endif %}
