/*
    FK parity guard for dbt Fusion.

    Asserts that on Snowflake (the only target this Fusion project runs against),
    the package created at least the expected number of FOREIGN KEY constraints
    in the test schema. This catches a regression of the upstream Fusion bug
    (dbt-fusion#1575) where test_metadata.kwargs would lose the `to` / `field`
    arguments for parameterised generic tests, causing the package to silently
    skip every FK with the "missing from test parameters" log line.

    Lower bound only: counts FKs created by THIS Fusion build's models.
    Run after `dbt build`. Fails (returns rows) if FK count < the lower bound.

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

{% if target.type == 'snowflake' %}

WITH expected AS (
    {% for c in expected_fks %}
    SELECT '{{ c }}' AS constraint_name{% if not loop.last %} UNION ALL{% endif %}
    {% endfor %}
),
actual AS (
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
