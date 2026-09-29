/*
    Source constraint guard.

    This test shows that the package creates constraints on dbt SOURCES, not only
    on models. dbt_constraints_sources_enabled and the per-type
    dbt_constraints_sources_{pk,uk,fk,nn}_enabled variables control source support.
    This project sets all of them to true.

    This test guards a regression that made source support silently inert. The
    package read test dependencies from graph.nodes only. dbt holds sources in
    graph.sources. Each source dependency then resolved to nothing, and the package
    created no constraint. A test declared on a source also has a null
    attached_node, and the main loop discarded it.

    The lists below are a lower bound. They cover the single-column cases that have
    stable constraint names. Extra constraints do not fail this test. Add to the
    lists when you add source tests.

    Run this test after `dbt build`. It fails, and returns rows, if a constraint is
    absent or if a NOT NULL column is still nullable.

    IMPORTANT: this test does nothing unless you pass the assert_source_constraints
    variable. The package creates constraints in an on-run-end hook. That hook runs
    AFTER the tests, so the constraints do not exist during one `dbt build`. Run
    this test as a second step:

        dbt build
        dbt test --select assert_source_constraints \
            --vars '{assert_source_constraints: true}'

    This test runs on Snowflake and Postgres. Other adapters return zero rows. This
    project has no verified constraint catalog query for them.
    The package does not create a foreign key declared ON a source. Such a test has
    a null attached_node and two dependencies. The package cannot identify the child
    table. The list below therefore checks a foreign key on a model that points AT a
    source. This shows that the package resolves a source on the parent side of a
    relationship.
*/

{% set expected_constraints = [
    ('SOURCE_PART_P_PARTKEY_PK', 'PRIMARY KEY'),
    ('SOURCE_SUPPLIER_S_SUPPKEY_UK', 'UNIQUE'),
    ('SOURCE_ORDERS_O_ORDERKEY_UK', 'UNIQUE'),
    ('SOURCE_CUSTOMER_C_CUSTKEY_UK', 'UNIQUE'),
    ('SOURCE_PARTSUPP_PS_PARTKEY_PS_SUPPKEY_UK', 'UNIQUE'),
    ('SOURCE_LINEITEM_L_ORDERKEY_L_LINENUMBER_UK', 'UNIQUE'),
    ('DIM_PART_SUPPLIER_PS_SUPPKEY_FK', 'FOREIGN KEY'),
] %}

{#- information_schema.table_constraints holds no row for NOT NULL on Snowflake or
    on Postgres. Check the nullability of the column instead. -#}
{% set expected_not_null = [
    ('source_part', 'p_partkey'),
    ('source_partsupp', 'ps_partkey'),
    ('source_partsupp', 'ps_suppkey'),
    ('source_supplier', 's_suppkey'),
    ('source_supplier', 's_nationkey'),
    ('source_orders', 'o_orderkey'),
    ('source_orders', 'o_custkey'),
    ('source_customer', 'c_custkey'),
    ('source_customer', 'c_name'),
    ('source_customer', 'c_nationkey'),
    ('source_lineitem', 'l_orderkey'),
    ('source_lineitem', 'l_linenumber'),
] %}

{% set guard_enabled = var('assert_source_constraints', false) | string | lower == 'true' %}

{% if guard_enabled and target.type in ('snowflake', 'postgres') %}

{#- Postgres rejects a catalog prefix that is not the current database. Qualify the
    schema only on Snowflake. -#}
{%- if target.type == 'snowflake' -%}
    {%- set information_schema = target.database ~ '.INFORMATION_SCHEMA' -%}
{%- else -%}
    {%- set information_schema = 'information_schema' -%}
{%- endif -%}

with expected_constraints as (
    {% for constraint_name, constraint_type in expected_constraints %}
    select '{{ constraint_name }}' as constraint_name,
           '{{ constraint_type }}' as constraint_type
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

actual_constraints as (
    select upper(constraint_name) as constraint_name,
           upper(constraint_type) as constraint_type
    from {{ information_schema }}.table_constraints
    where upper(table_schema) = upper('{{ target.schema }}')
),

missing_constraints as (
    select 'MISSING_CONSTRAINT' as issue,
           e.constraint_type || ' ' || e.constraint_name as object_name
    from expected_constraints e
    left join actual_constraints a
        on upper(e.constraint_name) = a.constraint_name
        and upper(e.constraint_type) = a.constraint_type
    where a.constraint_name is null
),

expected_not_null as (
    {% for table_name, column_name in expected_not_null %}
    select '{{ table_name }}' as table_name,
           '{{ column_name }}' as column_name
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

actual_columns as (
    select upper(table_name) as table_name,
           upper(column_name) as column_name,
           upper(is_nullable) as is_nullable
    from {{ information_schema }}.columns
    where upper(table_schema) = upper('{{ target.schema }}')
),

missing_not_null as (
    select 'NULLABLE_COLUMN' as issue,
           e.table_name || '.' || e.column_name as object_name
    from expected_not_null e
    left join actual_columns a
        on upper(e.table_name) = a.table_name
        and upper(e.column_name) = a.column_name
    {# An absent column is also a failure. It shows that the source table was never
       created. The assertion would otherwise pass without any check. #}
    where a.column_name is null
        or a.is_nullable = 'YES'
)

select issue, object_name from missing_constraints
union all
select issue, object_name from missing_not_null

{% else %}

-- The guard is off, or the adapter is not supported. Return zero rows.
select 'SKIPPED' as issue, '' as object_name
where 1 = 0

{% endif %}
