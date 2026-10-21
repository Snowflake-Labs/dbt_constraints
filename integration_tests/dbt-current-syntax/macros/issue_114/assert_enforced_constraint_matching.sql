{#- Check the issue #114 matching code on every target, inside the baseline build.

    on-run-start calls this macro. It feeds rows in the shape of a catalog lookup
    to keys_from_lookup_rows, and stubbed keys to match_enforced_constraint. So the
    same matching code runs under dbt-core, Fusion and dbt in Snowflake, which have
    no enforcing database to stage a constraint on. It also runs the skipped form
    of a test on the database, to prove that the database accepts its comment.

    The macro logs "assert_enforced_constraint_matching passed", or one
    "assert_enforced_constraint_matching failed: <case>" line for each failure.
    It does not raise, so a failure fails one pytest test and not the whole cell.
    tests/test_issue_114.py reads these lines.
-#}
{%- macro assert_enforced_constraint_matching() -%}
    {%- if execute -%}
        {%- set failures = [] -%}

        {#- PK1 rows are split by another key, to check the grouping. -#}
        {%- set unique_rows = {
            "column_names": ["constraint_name", "constraint_type", "column_name", "nulls_not_distinct"],
            "rows": [
                {"constraint_name": "PK1", "constraint_type": "P", "column_name": "a", "nulls_not_distinct": "N"},
                {"constraint_name": "UK_NULLABLE", "constraint_type": "U", "column_name": "c", "nulls_not_distinct": "false"},
                {"constraint_name": "PK1", "constraint_type": "P", "column_name": "b", "nulls_not_distinct": "N"},
                {"constraint_name": "UK_NND_Y", "constraint_type": "U", "column_name": "d", "nulls_not_distinct": "Y"},
                {"constraint_name": "UK_NND_1", "constraint_type": "U", "column_name": "e", "nulls_not_distinct": "1"},
                {"constraint_name": "UK_NND_TRUE", "constraint_type": "U", "column_name": "f", "nulls_not_distinct": "true"},
                {"constraint_name": "UK_NOT_NULL", "constraint_type": "U", "column_name": "g", "nulls_not_distinct": "0"}
            ] } -%}
        {%- set foreign_rows = {
            "column_names": ["constraint_name", "column_name", "pk_column_name"],
            "rows": [
                {"constraint_name": "FK_CROSS", "column_name": "c1", "pk_column_name": "k2"},
                {"constraint_name": "FK_CROSS", "column_name": "c2", "pk_column_name": "k1"},
                {"constraint_name": "FK_STRAIGHT", "column_name": "c3", "pk_column_name": "k1"},
                {"constraint_name": "FK_STRAIGHT", "column_name": "c4", "pk_column_name": "k2"}
            ] } -%}
        {%- set unique_keys = dbt_constraints.keys_from_lookup_rows(unique_rows) -%}
        {%- set foreign_keys = dbt_constraints.keys_from_lookup_rows(foreign_rows) -%}
        {%- set not_null_columns = ["A", "B", "G"] -%}

        {%- if unique_keys | count != 6 or foreign_keys | count != 2 -%}
            {%- do failures.append("key count " ~ unique_keys | count ~ "/" ~ foreign_keys | count) -%}
        {%- elif unique_keys[0].name != "PK1" or unique_keys[0].type != "p"
                or unique_keys[0].columns | join(",") != "A,B" -%}
            {%- do failures.append("grouping of PK1 rows") -%}
        {%- elif foreign_keys[0].columns | join(",") != "C1,C2"
                or foreign_keys[0].pk_columns | join(",") != "K2,K1" -%}
            {%- do failures.append("grouping of FK_CROSS rows") -%}
        {%- endif -%}

        {#- [description, kind, test columns, parent columns, expected match] -#}
        {%- set cases = [
            ["pk proves pk", "primary_key", ["a", "b"], [], "PK1"],
            ["pk proves uk in any order", "unique_key", ["b", "a"], [], "PK1"],
            ["quoted and upper-case names", "primary_key", ['"A"', "b"], [], "PK1"],
            ["fewer columns", "primary_key", ["a"], [], none],
            ["more columns", "primary_key", ["a", "b", "c"], [], none],
            ["nullable uk does not prove uk", "unique_key", ['"c"'], [], none],
            ["nullable uk does not prove pk", "primary_key", ["c"], [], none],
            ["nulls not distinct Y", "unique_key", ["d"], [], "UK_NND_Y"],
            ["nulls not distinct 1", "unique_key", ["e"], [], "UK_NND_1"],
            ["nulls not distinct true", "unique_key", ["f"], [], "UK_NND_TRUE"],
            ["nulls not distinct does not prove pk", "primary_key", ["d"], [], none],
            ["not null uk proves uk", "unique_key", ["g"], [], "UK_NOT_NULL"],
            ["not null uk proves pk", "primary_key", ["g"], [], "UK_NOT_NULL"],
            ["crossed fk pairs", "foreign_key", ["c1", "c2"], ["k1", "k2"], none],
            ["same fk pairs", "foreign_key", ["c1", "c2"], ["k2", "k1"], "FK_CROSS"],
            ["same fk pairs in another order", "foreign_key", ["c2", "c1"], ["k1", "k2"], "FK_CROSS"],
            ["straight fk", "foreign_key", ["c3", "c4"], ["k1", "k2"], "FK_STRAIGHT"],
            ["fk with fewer columns", "foreign_key", ["c3"], ["k1"], none]
        ] -%}
        {%- for case in cases -%}
            {%- set result = dbt_constraints.match_enforced_constraint(case[1], case[2],
                unique_keys=unique_keys, not_null_columns=not_null_columns,
                pk_column_names=case[3], foreign_keys=foreign_keys) -%}
            {%- if (result or none) != case[4] -%}
                {%- do failures.append(case[0] ~ " returned " ~ result) -%}
            {%- endif -%}
        {%- endfor -%}

        {#- The skipped form must run on this database and return no row. -#}
        {%- set inner_sql = "select 1 as x" ~ (" from dual" if target.type == "oracle" else "") -%}
        {%- set skipped_sql = dbt_constraints.skipped_test_sql(inner_sql, "primary_key", "*/ select") -%}
        {%- set count_result = run_query("select count(*) as failures from (\n" ~ skipped_sql ~ "\n) t") -%}
        {%- if count_result.rows[0][0] | int != 0 -%}
            {%- do failures.append("skipped test SQL returned rows") -%}
        {%- endif -%}

        {%- if failures | count == 0 -%}
            {%- do log("assert_enforced_constraint_matching passed", info=true) -%}
        {%- endif -%}
        {%- for failure in failures -%}
            {%- do log("assert_enforced_constraint_matching failed: " ~ failure, info=true) -%}
        {%- endfor -%}
    {%- endif -%}
{%- endmacro -%}
