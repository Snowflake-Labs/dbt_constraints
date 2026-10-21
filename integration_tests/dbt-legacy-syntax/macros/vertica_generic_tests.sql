{#- Vertica versions of the dbt-core generic tests unique and relationships
   (dbt/include/global_project/macros/generic_test_sql).

   The dbt-core macros write `where {{ column_name }} is not null`. Vertica gives
   IS NOT NULL a higher precedence than `||`. So for an expression key such as
   `a || '~' || b`, Vertica reads `a || '~' || (b is not null)` and fails with
   "Argument of WHERE must be type boolean". PostgreSQL gives `||` the higher
   precedence, so it does not have this problem.

   These copies only put parentheses around each expression. -#}
{% macro vertica__test_unique(model, column_name) %}

select
    ({{ column_name }}) as unique_field,
    count(*) as n_records

from {{ model }}
where ({{ column_name }}) is not null
group by ({{ column_name }})
having count(*) > 1

{% endmacro %}


{% macro vertica__test_relationships(model, column_name, to, field) %}

with child as (
    select ({{ column_name }}) as from_field
    from {{ model }}
    where ({{ column_name }}) is not null
),

parent as (
    select ({{ field }}) as to_field
    from {{ to }}
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null

{% endmacro %}
