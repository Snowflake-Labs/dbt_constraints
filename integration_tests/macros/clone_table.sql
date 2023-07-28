{%- macro clone_table(new_prefix) -%}
    {{ return(adapter.dispatch('clone_table')(new_prefix)) }}
{%- endmacro -%}


{%- macro snowflake__clone_table(new_prefix) -%}
    {%- set table_clone = api.Relation.create(
            database = this.database,
            schema = this.schema,
            identifier = new_prefix ~ this.identifier ) -%}

    {%- set clone_statement -%}
    create or replace table {{table_clone}} clone {{this}}
    {%- endset -%}
    {%- do log("Creating table clone: " ~ table_clone, info=false) -%}
    {%- do run_query(clone_statement) -%}

{%- endmacro -%}


{%- macro postgres__clone_table(new_prefix) -%}
    {%- set table_clone = api.Relation.create(
            database = this.database,
            schema = this.schema,
            identifier = new_prefix ~ this.identifier ) -%}

    {%- set clone_statement -%}
    drop table if exists {{table_clone}}
    {%- endset -%}
    {%- do log("Drop table if exists: " ~ table_clone, info=false) -%}

    {%- set clone_statement -%}
    create table {{table_clone}} as select * from {{this}}
    {%- endset -%}
    {%- do log("Creating table clone: " ~ table_clone, info=false) -%}
    {%- do run_query(clone_statement) -%}

{%- endmacro -%}


{%- macro oracle__clone_table(new_prefix) -%}
    {%- set table_clone = api.Relation.create(
            database = this.database,
            schema = this.schema,
            identifier = new_prefix ~ this.identifier ) -%}

    {%- set clone_statement -%}
DECLARE
tbl_count number;
sql_stmt long;

BEGIN
    SELECT COUNT(*) INTO tbl_count
    FROM dba_tables
    WHERE owner = '{{table_clone.schema}}'
    AND table_name = '{{table_clone.identifier}}';

    IF(tbl_count <> 0)
        THEN
        sql_stmt:='DROP TABLE {{table_clone}}';
        EXECUTE IMMEDIATE sql_stmt;
    END IF;
END;
    {%- endset -%}
    {%- do log("Drop table if exists: " ~ table_clone, info=false) -%}

    {%- set clone_statement -%}
    create table {{table_clone}} as select * from {{this}}
    {%- endset -%}
    {%- do log("Creating table clone: " ~ table_clone, info=false) -%}
    {%- do run_query(clone_statement) -%}

{%- endmacro -%}


