{%- macro clone_table(new_prefix) -%}
    {%- if flags.WHICH == 'seed' and execute -%}
        {{ return(adapter.dispatch('clone_table')(new_prefix)) }}
    {%- endif -%}
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

    {#- Drop the table first. Each seed run then creates a clean table. An old clone
       holds the constraints from an earlier run. The constraint tests then pass
       without any work by the package. -#}
    {%- set drop_statement -%}
        drop table if exists {{ table_clone }} cascade
    {%- endset -%}
    {%- do log("Drop table if exists: " ~ table_clone, info=true) -%}
    {%- do run_query(drop_statement) -%}

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
        sql_stmt:='DROP TABLE {{ table_clone }}';
        EXECUTE IMMEDIATE sql_stmt;
    END IF;
END;
    {%- endset -%}
    {%- do log("Drop table if exists: " ~ table_clone, info=false) -%}
    {%- do run_query(drop_statement) -%}

    {%- set clone_statement -%}
        create table {{ table_clone }} as select * from {{ this }}
    {%- endset -%}
    {%- do log("Creating table clone: " ~ table_clone, info=false) -%}
    {%- do run_query(clone_statement) -%}

{%- endmacro -%}
