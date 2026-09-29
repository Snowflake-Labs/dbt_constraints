{#- Run an ad hoc SQL statement and log each row.

   Used by the integration test harness to inspect database state through dbt's own
   connection, so the check runs against exactly the target the models were built on.

   Every row is logged with a fixed `QUERY RESULT:` prefix so a test can assert on the
   output rather than only on the exit code.

   Example:
     dbt run-operation query --args "{sql: SELECT COUNT(*) FROM my_table}"
-#}
{%- macro query(sql) -%}
    {%- if execute -%}
        {%- set result = run_query(sql) -%}
        {%- if result -%}
            {%- for row in result.rows -%}
                {%- do log("QUERY RESULT: " ~ (row | join(" | ")), info=true) -%}
            {%- endfor -%}
            {%- if result.rows | length == 0 -%}
                {%- do log("QUERY RESULT: (no rows)", info=true) -%}
            {%- endif -%}
        {%- endif -%}
    {%- endif -%}
{%- endmacro -%}
