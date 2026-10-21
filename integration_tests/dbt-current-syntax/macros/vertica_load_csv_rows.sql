{#- Override the dbt-vertica seed loader (dbt/include/vertica/macros/materializations/seeds/helpers.sql).

   Two changes from the adapter COPY:

   1. Remove `rejected data as table <schema>.seed_rejects`. Every seed in a schema uses
      that one table name. The first seed creates it. Each other seed then fails with
      "Object "seed_rejects" already exists", in parallel and in later runs. The clause
      has no use here: `abort on error` stops the load at the first bad row.

   2. Load each integer column through a text FILLER. agate infers `integer` for a
      column such as lineitem.l_quantity, whose values are all like "32.00". The other
      adapters INSERT bound values, which casts them. COPY parses the raw text and
      rejects "32.00" for an integer column. The cast `::numeric::integer` gives the
      same result as the INSERT path. nullif() turns a quoted empty field into NULL.

   A root-project macro takes precedence over the adapter macro of the same name. -#}
{%- macro vertica__load_csv_rows(model, agate_table) -%}
    {%- set column_override = model['config'].get('column_types', {}) -%}
    {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}
    {%- set copy_columns = [] -%}
    {%- for col_name in agate_table.column_names -%}
        {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
        {%- set type = column_override.get(col_name, inferred_type) -%}
        {%- set column = adapter.quote_seed_column(col_name | string, quote_seed_column) -%}
        {%- if type | lower in ('integer', 'int', 'bigint', 'int8', 'smallint', 'tinyint') -%}
            {%- set raw = adapter.quote('raw_' ~ loop.index0) -%}
            {%- do copy_columns.append(raw ~ ' filler varchar(64)') -%}
            {%- do copy_columns.append(column ~ ' as nullif(' ~ raw ~ ", '')::numeric::integer") -%}
        {%- else -%}
            {%- do copy_columns.append(column) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- set sql -%}
        copy {{ this.render() }}
        ({{ copy_columns | join(', ') }})
        from local '{{ agate_table.original_abspath }}'
        delimiter ','
        enclosed by '"'
        skip 1
        abort on error
    {%- endset -%}
    {{ return(sql) }}
{%- endmacro -%}
