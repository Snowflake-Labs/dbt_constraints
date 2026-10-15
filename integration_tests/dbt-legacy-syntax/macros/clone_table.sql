{%- macro clone_table(new_prefix) -%}
    {%- if flags.WHICH == 'seed' and execute -%}
        {{ return(adapter.dispatch('clone_table')(new_prefix)) }}
    {%- endif -%}
{%- endmacro -%}


{#- Drop the cloned tables in one pass, before the seeds run.

   Do not do this in the per-seed post-hook. The clones hold foreign keys to each
   other. A `drop table ... cascade` also locks each table whose foreign key needs
   the dropped table. Six concurrent seeds can then take the same locks in opposite
   order and deadlock. The single on-run-start session removes the contention.

   The `flags.WHICH == 'seed'` test matches clone_table. Without this test a
   `dbt build` drops the clones and does not create them again. Only `dbt seed`
   runs the post-hook that creates them. -#}
{%- macro drop_cloned_tables(new_prefix) -%}
    {%- if flags.WHICH == 'seed' and execute -%}
        {#- Drop only the clones that this run creates again. A drop of each clone
           during a partial seed leaves the other clones absent. -#}
        {%- for node_id in selected_resources -%}
            {%- set node = graph.nodes.get(node_id) -%}
            {%- if node and node.resource_type == "seed" -%}
                {%- set table_clone = api.Relation.create(
                        database = node.database,
                        schema = node.schema,
                        identifier = new_prefix ~ (node.alias or node.name) ) -%}
                {%- do log("Dropping table clone: " ~ table_clone, info=false) -%}
                {%- do adapter.dispatch('drop_cloned_table')(table_clone) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
{%- endmacro -%}

{%- macro default__drop_cloned_table(table_clone) -%}
    {%- do run_query("drop table if exists " ~ table_clone) -%}
{%- endmacro -%}

{%- macro postgres__drop_cloned_table(table_clone) -%}
    {%- do run_query("drop table if exists " ~ table_clone ~ " cascade") -%}
{%- endmacro -%}

{%- macro snowflake__drop_cloned_table(table_clone) -%}
    {#- Snowflake does not enforce constraints. A cascade is not necessary. The CTAS
       below replaces the clone. This macro does nothing. -#}
{%- endmacro -%}

{%- macro oracle__drop_cloned_table(table_clone) -%}
    {#- Uses all_tables, not dba_tables. dba_tables needs DBA privileges the test user
       does not have, and querying it fails with ORA-00942. all_tables shows every table
       the current user can see, which includes its own schema. -#}
    {%- set drop_statement -%}
DECLARE
tbl_count number;
sql_stmt long;

BEGIN
    SELECT COUNT(*) INTO tbl_count
    FROM all_tables
    WHERE owner = '{{ table_clone.schema | upper }}'
    AND table_name = '{{ table_clone.identifier | upper }}';

    IF(tbl_count <> 0)
        THEN
        sql_stmt:='DROP TABLE {{ table_clone }} CASCADE CONSTRAINTS';
        EXECUTE IMMEDIATE sql_stmt;
    END IF;
END;
    {%- endset -%}
    {%- do run_query(drop_statement) -%}
{%- endmacro -%}


{%- macro snowflake__clone_table(new_prefix) -%}
    {%- set table_clone = api.Relation.create(
            database = this.database,
            schema = this.schema,
            identifier = new_prefix ~ this.identifier ) -%}

    {#- Use CTAS, not CLONE. A Snowflake clone copies the constraints of the source
       table. The cloned table then holds a primary key and unique keys before the
       package runs. The package correctly skips them. The tests for source
       constraints then fail, but the behaviour is correct. CTAS copies only the
       data. This matches the postgres and oracle macros below. -#}
    {%- set clone_statement -%}
        create or replace table {{ table_clone }} as select * from {{ this }}
    {%- endset -%}
    {%- do log("Creating table copy: " ~ table_clone, info=false) -%}
    {%- do run_query(clone_statement) -%}

{%- endmacro -%}


{%- macro postgres__clone_table(new_prefix) -%}
    {%- set table_clone = api.Relation.create(
            database = this.database,
            schema = this.schema,
            identifier = new_prefix ~ this.identifier ) -%}

    {#- drop_cloned_tables drops the table one time at on-run-start. Do not drop it
       here. A drop for each seed ran six concurrent `drop ... cascade` statements
       against tables that a foreign key joins. Those statements deadlocked. A
       create takes no lock on another table. Threads can run this part in
       parallel. -#}
    {%- set clone_statement -%}
        create table {{ table_clone }} as select * from {{ this }}
    {%- endset -%}
    {%- do log("Creating table clone: " ~ table_clone, info=true) -%}
    {%- do run_query(clone_statement) -%}

{%- endmacro -%}


{%- macro oracle__clone_table(new_prefix) -%}
    {%- set table_clone = api.Relation.create(
            database = this.database,
            schema = this.schema,
            identifier = new_prefix ~ this.identifier ) -%}

    {#- drop_cloned_tables drops the table one time at on-run-start. Do not drop it
       here. This prevents concurrent DDL on clones that a foreign key joins. -#}
    {%- set clone_statement -%}
        create table {{ table_clone }} as select * from {{ this }}
    {%- endset -%}
    {%- do log("Creating table clone: " ~ table_clone, info=false) -%}
    {%- do run_query(clone_statement) -%}

{%- endmacro -%}
