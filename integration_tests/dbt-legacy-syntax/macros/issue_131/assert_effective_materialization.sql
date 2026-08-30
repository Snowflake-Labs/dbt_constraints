{#- Check the effective_materialization macro and the two decisions that use it.

   Run this operation with:
       dbt run-operation assert_effective_materialization

   The operation raises a compiler error if any case returns a wrong value.

   Issue #131. Two clauses read meta.materialized with a literal default of "other".
   Most nodes do not set meta.materialized, so both clauses were always true.
   One clause set verify_permissions to true for every model. The permission check
   then failed and the package skipped every constraint.
   The other clause stopped the view and ephemeral exclusion from working. -#}
{%- macro assert_effective_materialization() -%}

    {#- Each case holds the node, the expected materialization, and the two
       expected decisions. "excluded" means the package must skip the node.
       "verify" means the package must check permissions first. -#}
    {%- set cases = [
        {
            "name": "plain table with no meta",
            "node": {"config": {"materialized": "table"}},
            "expected": "table",
            "excluded": false,
            "verify": false
        },
        {
            "name": "incremental with no meta",
            "node": {"config": {"materialized": "incremental"}},
            "expected": "incremental",
            "excluded": false,
            "verify": false
        },
        {
            "name": "seed with no meta",
            "node": {"config": {"materialized": "seed"}},
            "expected": "seed",
            "excluded": false,
            "verify": false
        },
        {
            "name": "table with an empty meta dict",
            "node": {"config": {"materialized": "table", "meta": {}}},
            "expected": "table",
            "excluded": false,
            "verify": false
        },
        {
            "name": "view with no meta",
            "node": {"config": {"materialized": "view"}},
            "expected": "view",
            "excluded": true,
            "verify": true
        },
        {
            "name": "ephemeral with no meta",
            "node": {"config": {"materialized": "ephemeral"}},
            "expected": "ephemeral",
            "excluded": true,
            "verify": true
        },
        {
            "name": "dynamic table with no meta",
            "node": {"config": {"materialized": "dynamic_table"}},
            "expected": "dynamic_table",
            "excluded": true,
            "verify": true
        },
        {
            "name": "custom materialization with no meta",
            "node": {"config": {"materialized": "my_custom"}},
            "expected": "my_custom",
            "excluded": false,
            "verify": true
        },
        {
            "name": "meta declares a table over a custom materialization",
            "node": {"config": {"materialized": "my_custom", "meta": {"materialized": "table"}}},
            "expected": "table",
            "excluded": false,
            "verify": false
        },
        {
            "name": "meta declares a view over a table",
            "node": {"config": {"materialized": "table", "meta": {"materialized": "view"}}},
            "expected": "view",
            "excluded": true,
            "verify": true
        },
        {
            "name": "node with an empty config",
            "node": {"config": {}},
            "expected": "other",
            "excluded": false,
            "verify": true
        },
        {
            "name": "node that is none",
            "node": none,
            "expected": "other",
            "excluded": false,
            "verify": true
        }
    ] -%}

    {%- set failures = [] -%}

    {%- for case in cases -%}
        {%- set actual = dbt_constraints.effective_materialization(case["node"]) -%}

        {%- if actual != case["expected"] -%}
            {%- do failures.append(
                case["name"] ~ ": expected materialization " ~ case["expected"]
                ~ " but read " ~ actual) -%}
        {%- endif -%}

        {#- Repeat the two decisions exactly as create_constraints_by_type makes them. -#}
        {%- set actual_excluded = actual in ("view", "ephemeral", "dynamic_table") -%}
        {%- set actual_verify = actual not in ("table", "incremental", "snapshot", "seed") -%}

        {%- if actual_excluded != case["excluded"] -%}
            {%- do failures.append(
                case["name"] ~ ": expected excluded " ~ case["excluded"]
                ~ " but got " ~ actual_excluded) -%}
        {%- endif -%}

        {%- if actual_verify != case["verify"] -%}
            {%- do failures.append(
                case["name"] ~ ": expected verify_permissions " ~ case["verify"]
                ~ " but got " ~ actual_verify) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- if failures | length > 0 -%}
        {{ exceptions.raise_compiler_error(
            "assert_effective_materialization found "
            ~ failures | length ~ " failures: " ~ failures | join(" | ")) }}
    {%- endif -%}

    {{ log("assert_effective_materialization passed " ~ cases | length ~ " cases", info=true) }}

{%- endmacro -%}
