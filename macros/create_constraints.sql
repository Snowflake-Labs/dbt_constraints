{#- Define three tests for PK, UK, and FK that can be overridden by DB implementations.
    These tests have overloaded parameter names to be as flexible as possible. -#}

{%- test primary_key(model,
        column_name=none, column_names=[],
        quote_columns=false, constraint_name=none) -%}

    {%- if column_names|count == 0 and column_name -%}
        {%- do column_names.append(column_name) -%}
    {%- endif -%}

    {{ return(adapter.dispatch('test_primary_key', 'dbt_constraints')(model, column_names, quote_columns)) }}

{%- endtest -%}


{%- test unique_key(model,
        column_name=none, column_names=[],
        quote_columns=false, constraint_name=none) -%}

    {%- if column_names|count == 0 and column_name -%}
        {%- do column_names.append(column_name) -%}
    {%- endif -%}

    {{ return(adapter.dispatch('test_unique_key', 'dbt_constraints')(model, column_names, quote_columns)) }}

{%- endtest -%}


{%- test foreign_key(model,
        column_name=none, fk_column_name=none, fk_column_names=[],
        pk_table_name=none, to=none,
        pk_column_name=none, pk_column_names=[], field=none,
        quote_columns=false, constraint_name=none) -%}

    {%- if pk_column_names|count == 0 and (pk_column_name or field) -%}
        {%- do pk_column_names.append( (pk_column_name or field) ) -%}
    {%- endif -%}
    {%- if fk_column_names|count == 0 and (fk_column_name or column_name) -%}
        {%- do fk_column_names.append( (fk_column_name or column_name) ) -%}
    {%- endif -%}
    {%- set pk_table_name = pk_table_name or to -%}

    {{ return(adapter.dispatch('test_foreign_key', 'dbt_constraints')(model, fk_column_names, pk_table_name, pk_column_names, quote_columns)) }}

{%- endtest -%}




{#- Define three create macros for PK, UK, and FK that can be overridden by DB implementations -#}

{%- macro create_primary_key(table_model, column_names, verify_permissions, quote_columns, constraint_name, lookup_cache, rely_clause) -%}
    {{ return(adapter.dispatch('create_primary_key', 'dbt_constraints')(table_model, column_names, verify_permissions, quote_columns, constraint_name, lookup_cache, rely_clause)) }}
{%- endmacro -%}


{%- macro create_unique_key(table_model, column_names, verify_permissions, quote_columns, constraint_name, lookup_cache, rely_clause) -%}
    {{ return(adapter.dispatch('create_unique_key', 'dbt_constraints')(table_model, column_names, verify_permissions, quote_columns, constraint_name, lookup_cache, rely_clause)) }}
{%- endmacro -%}


{%- macro create_foreign_key(pk_table_relation, pk_column_names, fk_table_relation, fk_column_names, verify_permissions, quote_columns, constraint_name, lookup_cache, rely_clause) -%}
    {{ return(adapter.dispatch('create_foreign_key', 'dbt_constraints')(pk_table_relation, pk_column_names, fk_table_relation, fk_column_names, verify_permissions, quote_columns, constraint_name, lookup_cache, rely_clause)) }}
{%- endmacro -%}


{%- macro create_not_null(table_relation, column_names, verify_permissions, quote_columns, lookup_cache, rely_clause) -%}
    {{ return(adapter.dispatch('create_not_null', 'dbt_constraints')(table_relation, column_names, verify_permissions, quote_columns, lookup_cache, rely_clause)) }}
{%- endmacro -%}


{#- Define two macros for detecting if PK, UK, and FK exist that can be overridden by DB implementations -#}

{%- macro unique_constraint_exists(table_relation, column_names, lookup_cache) -%}
    {{ return(adapter.dispatch('unique_constraint_exists', 'dbt_constraints')(table_relation, column_names, lookup_cache) ) }}
{%- endmacro -%}

{%- macro foreign_key_exists(table_relation, column_names, lookup_cache) -%}
    {{ return(adapter.dispatch('foreign_key_exists', 'dbt_constraints')(table_relation, column_names, lookup_cache)) }}
{%- endmacro -%}


{#- Define two macros for detecting if we have sufficient privileges that can be overridden by DB implementations -#}

{%- macro have_references_priv(table_relation, verify_permissions, lookup_cache) -%}
    {{ return(adapter.dispatch('have_references_priv', 'dbt_constraints')(table_relation, verify_permissions, lookup_cache) ) }}
{%- endmacro -%}

{%- macro have_ownership_priv(table_relation, verify_permissions, lookup_cache) -%}
    {{ return(adapter.dispatch('have_ownership_priv', 'dbt_constraints')(table_relation, verify_permissions, lookup_cache)) }}
{%- endmacro -%}


{#- Define macro for whether a DB implementation has implemented logic for RELY and NORELY constraints -#}

{%- macro adapter_supports_rely_norely(test_name) -%}
    {{ return(adapter.dispatch('adapter_supports_rely_norely', 'dbt_constraints')(test_name)) }}
{%- endmacro -%}

{#- By default, we assume DB implementations have NOT implemented logic for RELY and NORELY constraints -#}
{%- macro default__adapter_supports_rely_norely(test_name) -%}
    {{ return(false) }}
{%- endmacro -%}




{#- Override dbt's truncate_relation macro to allow us to create adapter specific versions that drop constraints -#}

{% macro truncate_relation(relation) -%}
  {{ return(adapter.dispatch('truncate_relation')(relation)) }}
{% endmacro %}

{#- Override dbt's drop_relation macro to allow us to create adapter specific versions that drop constraints -#}

{% macro drop_relation(relation) -%}
  {{ return(adapter.dispatch('drop_relation')(relation)) }}
{% endmacro %}



{#- This macro should be added to on-run-end to create constraints
    after all the models and tests have completed. You can pass a
    list of the tests that you want considered for constraints and
    a flag for whether columns should be quoted. The first macro
    primarily controls the order that constraints are created. -#}
{%- macro create_constraints(
        constraint_types=[
            'primary_key',
            'unique_key',
            'unique_combination_of_columns',
            'unique',
            'foreign_key',
            'relationships',
            'not_null'],
        quote_columns=false) -%}
    {%- if execute and var('dbt_constraints_enabled', false) and results -%}
        {%- do log("Running dbt Constraints", info=true) -%}

        {%- set lookup_cache = {
            "table_columns": { },
            "table_privileges": { },
            "unique_keys": { },
            "not_null_col": { },
            "foreign_keys": { } } -%}

        {%- if 'not_null' in constraint_types -%}
            {%- do dbt_constraints.create_constraints_by_type(['not_null'], quote_columns, lookup_cache) -%}
        {%- endif -%}
        {%- if 'primary_key' in constraint_types -%}
            {%- do dbt_constraints.create_constraints_by_type(['primary_key'], quote_columns, lookup_cache) -%}
        {%- endif -%}
        {%- if 'unique_key' in constraint_types -%}
            {%- do dbt_constraints.create_constraints_by_type(['unique_key'], quote_columns, lookup_cache) -%}
        {%- endif -%}
        {%- if 'unique_combination_of_columns' in constraint_types -%}
            {%- do dbt_constraints.create_constraints_by_type(['unique_combination_of_columns'], quote_columns, lookup_cache) -%}
        {%- endif -%}
        {%- if 'unique' in constraint_types -%}
            {%- do dbt_constraints.create_constraints_by_type(['unique'], quote_columns, lookup_cache) -%}
        {%- endif -%}
        {%- if 'foreign_key' in constraint_types -%}
            {%- do dbt_constraints.create_constraints_by_type(['foreign_key'], quote_columns, lookup_cache) -%}
        {%- endif -%}
        {%- if 'relationships' in constraint_types -%}
            {%- do dbt_constraints.create_constraints_by_type(['relationships'], quote_columns, lookup_cache) -%}
        {%- endif -%}

        {%- do log("Finished dbt Constraints", info=true) -%}
    {%- endif -%}

{%- endmacro -%}


{#- This macro checks if a test or its model is selected -#}
{%- macro test_selected(test_model) -%}

    {%- if test_model.unique_id in selected_resources -%}
        {{ return("TEST_SELECTED") }}
    {%- endif -%}
    {%- if test_model.attached_node in selected_resources -%} -%}
        {{ return("MODEL_SELECTED") }}
    {%- endif -%}

    {#- Check if a PK/UK should be created because it is referenced by a selected FK -#}
    {%- if test_model.test_metadata.name in ("primary_key", "unique_key", "unique_combination_of_columns", "unique") -%}
        {%- set pk_test_args = test_model.test_metadata.kwargs -%}
        {%- set pk_test_columns = [] -%}
        {%- if pk_test_args.column_names -%}
            {%- set pk_test_columns =  pk_test_args.column_names -%}
        {%- elif pk_test_args.combination_of_columns -%}
            {%- set pk_test_columns =  pk_test_args.combination_of_columns -%}
        {%- elif pk_test_args.column_name -%}
            {%- set pk_test_columns =  [pk_test_args.column_name] -%}
        {%- endif -%}
        {%- for fk_model in graph.nodes.values() | selectattr("resource_type", "equalto", "test")
                if  fk_model.test_metadata
                and fk_model.test_metadata.name in ("foreign_key", "relationships")
                and test_model.attached_node in fk_model.depends_on.nodes
                and ( (fk_model.unique_id and fk_model.unique_id in selected_resources)
                    or (fk_model.attached_node and fk_model.attached_node in selected_resources) ) -%}
            {%- set fk_test_args = fk_model.test_metadata.kwargs -%}
            {%- set fk_test_columns = [] -%}
            {%- if fk_test_args.pk_column_names -%}
                {%- set fk_test_columns =  fk_test_args.pk_column_names -%}
            {%- elif fk_test_args.pk_column_name -%}
                {%- set fk_test_columns =  [fk_test_args.pk_column_name] -%}
            {%- elif fk_test_args.field -%}
                {%- set fk_test_columns =  [fk_test_args.field] -%}
            {%- endif -%}
            {%- if column_list_matches(pk_test_columns, fk_test_columns) -%}
                {{ return("PK_UK_FOR_SELECTED_FK") }}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

    {{ return(none) }}
{%- endmacro -%}


{#- This macro that checks if a test has results and whether there were errors -#}
{%- macro lookup_should_rely(test_model) -%}
    {%- if test_model.config.where
            or test_model.config.warn_if != "!= 0"
            or test_model.config.fail_calc != "count(*)" -%}
        {#- Set NORELY if there is a condition on the test -#}
        {{ return('NORELY') }}
    {%- endif -%}

    {%- for res in results
        if res.node.config.materialized == "test"
        and res.node.unique_id == test_model.unique_id -%}
        {%- if res.failures == None -%}
            {#- Set '' if we do not know if there is a test failure -#}
            {{ return('') }}
        {%- elif res.failures > 0 -%}
            {#- Set NORELY if there is a test failure -#}
            {{ return('NORELY') }}
        {%- elif res.failures == 0 -%}
            {#- Set RELY if there are 0 failures -#}
            {{ return('RELY') }}
        {%- endif -%}
    {%- endfor -%}
    {{ return('') }}
{%- endmacro -%}


{#- This macro that checks if a test or its model has always_create_constraint set -#}
{%- macro should_always_create_constraint(test_model) -%}
    {%- if test_model.config.get("always_create_constraint", false) == true -%}
        {{ return(true) }}
    {%- endif -%}
    {%- for table_node in test_model.depends_on.nodes -%}
        {%- for node in graph.nodes.values() | selectattr("unique_id", "equalto", table_node)
            if node.config.get("always_create_constraint", false) == true -%}
            {{ return(true) }}
        {%- endfor -%}
    {%- endfor -%}

    {{ return(false) }}
{%- endmacro -%}


{#- This macro is called internally and passed which constraint types to create. -#}
{%- macro create_constraints_by_type(constraint_types, quote_columns, lookup_cache) -%}

    {#- Loop through the metadata and find all tests that match the constraint_types and have all the fields we check for tests -#}
    {%- for test_model in graph.nodes.values() | selectattr("resource_type", "equalto", "test")
            if test_model.test_metadata
            and test_model.test_metadata.kwargs
            and test_model.test_metadata.name
            and test_model.test_metadata.name is in( constraint_types )
            and test_model.unique_id
            and test_model.attached_node
            and test_model.depends_on
            and test_model.depends_on.nodes
            and test_model.config
            and test_model.config.enabled
            and test_model.config.get("dbt_constraints_enabled", true) -%}

        {%- set test_parameters = test_model.test_metadata.kwargs -%}
        {%- set test_name = test_model.test_metadata.name -%}
        {%- set selected = dbt_constraints.test_selected(test_model) -%}

        {#- We can shortcut additional tests if the constraint was not selected -#}
        {%- if selected is not none -%}
            {#- rely_clause clause will be RELY if a test passed, NORELY if it failed, and '' if it was skipped -#}
            {%- set rely_clause = dbt_constraints.lookup_should_rely(test_model) -%}
            {%- set always_create_constraint = dbt_constraints.should_always_create_constraint(test_model) -%}
        {%- else -%}
            {%- set rely_clause = '' -%}
            {%- set always_create_constraint = false -%}
        {%- endif -%}

        {#- Create constraints that:
            - Either the test or its model was selected to run, including PK/UK for FK
            - Passed the test (RELY) or the database supports NORELY constraints
            - We ran the test (RELY/NORELY) or we need the constraint for a FK
              or we have the always_create_constraint parameter turned on -#}
        {%- if selected is not none
            and ( rely_clause == 'RELY'
                  or dbt_constraints.adapter_supports_rely_norely(test_name) == true )
            and ( rely_clause in('RELY', 'NORELY')
                  or selected == "PK_UK_FOR_SELECTED_FK"
                  or always_create_constraint == true ) -%}

            {% set ns = namespace(verify_permissions=false) %}
            {%- set table_models = [] -%}

            {#- Find the table models that are referenced by this test. -#}
            {%- for table_node in test_model.depends_on.nodes -%}
                {%- for node in graph.nodes.values() | selectattr("unique_id", "equalto", table_node)
                    if node.config
                    and node.config.get("materialized", "other") not in ("view", "ephemeral", "dynamic_table")
                    and ( node.resource_type in ("model", "snapshot", "seed")
                        or ( node.resource_type == "source" and var('dbt_constraints_sources_enabled', false)
                            and ( ( var('dbt_constraints_sources_pk_enabled', false) and test_name in("primary_key") )
                                or ( var('dbt_constraints_sources_uk_enabled', false) and test_name in("unique_key", "unique_combination_of_columns", "unique") )
                                or ( var('dbt_constraints_sources_fk_enabled', false) and test_name in("foreign_key", "relationships") )
                                or ( var('dbt_constraints_sources_nn_enabled', false) and test_name in("not_null") ) )
                        ) ) -%}

                    {%- do node.update({'alias': node.alias or node.name }) -%}
                    {#- Append to our list of models for this test -#}
                    {%- do table_models.append(node) -%}
                    {%- if node.resource_type == "source"
                        or node.config.get("materialized", "other") not in ("table", "incremental", "snapshot", "seed") -%}
                        {#- If we are using a sources or custom materializations, we will need to verify permissions -#}
                        {%- set ns.verify_permissions = true -%}
                    {%- endif -%}

                {% endfor %}
            {% endfor %}

            {#- We only create PK/UK if there is one model referenced by the test
                and if all the columns exist as physical columns on the table -#}
            {%- if 1 == table_models|count
                and test_name in("primary_key", "unique_key", "unique_combination_of_columns", "unique") -%}

                {# Attempt to identify a parameter we can use for the column names #}
                {%- set column_names = [] -%}
                {%- if  test_parameters.column_names -%}
                    {%- set column_names =  test_parameters.column_names -%}
                {%- elif  test_parameters.combination_of_columns -%}
                    {%- set column_names =  test_parameters.combination_of_columns -%}
                {%- elif  test_parameters.column_name -%}
                    {%- set column_names =  [test_parameters.column_name] -%}
                {%- else  -%}
                    {{ exceptions.raise_compiler_error(
                    "`column_names` or `column_name` parameter missing for primary/unique key constraint on table: '" ~ table_models[0].name
                    ) }}
                {%- endif -%}

                {%- set table_relation = adapter.get_relation(
                    database=table_models[0].database,
                    schema=table_models[0].schema,
                    identifier=table_models[0].alias ) -%}
                {%- if table_relation and table_relation.is_table -%}
                    {%- if dbt_constraints.table_columns_all_exist(table_relation, column_names, lookup_cache) -%}
                        {%- if test_name == "primary_key" or (target.type == "bigquery" 
                            and test_name in("unique_key", "unique_combination_of_columns", "unique"))
                        -%}
                            {%- if dbt_constraints.adapter_supports_rely_norely("not_null") == true -%}
                                {%- do dbt_constraints.create_not_null(table_relation, column_names, ns.verify_permissions, quote_columns, lookup_cache, rely_clause) -%}
                            {%- endif -%}
                            {%- do dbt_constraints.create_primary_key(table_relation, column_names, ns.verify_permissions, quote_columns, test_parameters.constraint_name, lookup_cache, rely_clause) -%}
                        {%- else  -%}
                            {%- do dbt_constraints.create_unique_key(table_relation, column_names, ns.verify_permissions, quote_columns, test_parameters.constraint_name, lookup_cache, rely_clause) -%}
                        {%- endif -%}
                    {%- else  -%}
                        {%- do log("Skipping primary/unique key because a physical column name was not found on the table: " ~ table_models[0].name ~ " " ~ column_names, info=true) -%}
                    {%- endif -%}
                {%- else  -%}
                    {%- do log("Skipping primary/unique key because the table was not found in the database: " ~ table_models[0].name, info=true) -%}
                {%- endif -%}

            {#- We only create FK if there are two models referenced by the test
                and if all the columns exist as physical columns on the tables -#}
            {%- elif 2 == table_models|count
                and test_name in( "foreign_key", "relationships") -%}

                {%- set fk_model = table_models | selectattr("unique_id", "equalto", test_model.attached_node) | first -%}
                {%- set pk_model = table_models | rejectattr("unique_id", "equalto", test_model.attached_node) | first -%}

                {%- if fk_model and pk_model -%}

                    {%- set fk_table_relation = adapter.get_relation(
                        database=fk_model.database,
                        schema=fk_model.schema,
                        identifier=fk_model.alias) -%}

                    {%- set pk_table_relation = adapter.get_relation(
                        database=pk_model.database,
                        schema=pk_model.schema,
                        identifier=pk_model.alias) -%}

                    {%- if fk_table_relation and pk_table_relation and fk_table_relation.is_table and pk_table_relation.is_table-%}
                        {# Attempt to identify parameters we can use for the column names #}
                        {%- set pk_column_names = [] -%}
                        {%- if  test_parameters.pk_column_names -%}
                            {%- set pk_column_names = test_parameters.pk_column_names -%}
                        {%- elif  test_parameters.field -%}
                            {%- set pk_column_names = [test_parameters.field] -%}
                        {%- elif test_parameters.pk_column_name -%}
                            {%- set pk_column_names = [test_parameters.pk_column_name] -%}
                        {%- else -%}
                            {{ exceptions.raise_compiler_error(
                            "`pk_column_names`, `pk_column_name`, or `field` parameter missing for foreign key constraint on table: '" ~ fk_model.name ~ " " ~ test_parameters
                            ) }}
                        {%- endif -%}

                        {%- set fk_column_names = [] -%}
                        {%- if  test_parameters.fk_column_names -%}
                            {%- set fk_column_names = test_parameters.fk_column_names -%}
                        {%- elif test_parameters.column_name -%}
                            {%- set fk_column_names = [test_parameters.column_name] -%}
                        {%- elif test_parameters.fk_column_name -%}
                            {%- set fk_column_names = [test_parameters.fk_column_name] -%}
                        {%- else -%}
                            {{ exceptions.raise_compiler_error(
                            "`fk_column_names`, `fk_column_name`, or `column_name` parameter missing for foreign key constraint on table: '" ~ fk_model.name ~ " " ~ test_parameters
                            ) }}
                        {%- endif -%}

                        {%- if not dbt_constraints.table_columns_all_exist(pk_table_relation, pk_column_names, lookup_cache) -%}
                            {%- do log("Skipping foreign key because a physical column was not found on the pk table: " ~ pk_model.name ~ " " ~ pk_column_names, info=true) -%}
                        {%- elif not dbt_constraints.table_columns_all_exist(fk_table_relation, fk_column_names, lookup_cache) -%}
                            {%- do log("Skipping foreign key because a physical column was not found on the fk table: " ~ fk_model.name ~ " " ~ fk_column_names, info=true) -%}
                        {%- else  -%}
                            {%- do dbt_constraints.create_foreign_key(pk_table_relation, pk_column_names, fk_table_relation, fk_column_names, ns.verify_permissions, quote_columns, test_parameters.constraint_name, lookup_cache, rely_clause) -%}
                        {%- endif -%}
                    {%- else  -%}
                        {%- if fk_model == None or not fk_table_relation.is_table -%}
                            {%- do log("Skipping foreign key to " ~ pk_model.alias ~ " because the child table was not found in the database: " ~ fk_model.alias, info=true) -%}
                        {%- endif -%}
                        {%- if pk_model == None or not pk_model.is_table -%}
                            {%- do log("Skipping foreign key on " ~ fk_model.alias ~ " because the parent table was not found in the database: " ~ pk_model.alias, info=true) -%}
                        {%- endif -%}
                    {%- endif -%}

                {%- else  -%}
                    {%- do log("Skipping foreign key because a we couldn't find the child table: model=" ~ test_model.attached_node ~ " or source", info=true) -%}
                {%- endif -%}

            {#- We only create NN if there is one model referenced by the test
                and if all the columns exist as physical columns on the table -#}
            {%- elif 1 == table_models|count
                and test_name in("not_null") -%}

                {# Attempt to identify a parameter we can use for the column names #}
                {%- set column_names = [] -%}
                {%- if  test_parameters.column_names -%}
                    {%- set column_names =  test_parameters.column_names -%}
                {%- elif  test_parameters.combination_of_columns -%}
                    {%- set column_names =  test_parameters.combination_of_columns -%}
                {%- elif  test_parameters.column_name -%}
                    {%- set column_names =  [test_parameters.column_name] -%}
                {%- else  -%}
                    {{ exceptions.raise_compiler_error(
                    "`column_names` or `column_name` parameter missing for not null constraint on table: '" ~ table_models[0].name
                    ) }}
                {%- endif -%}

                {%- set table_relation = adapter.get_relation(
                    database=table_models[0].database,
                    schema=table_models[0].schema,
                    identifier=table_models[0].alias ) -%}

                {%- if table_relation and table_relation.is_table -%}
                    {%- if dbt_constraints.table_columns_all_exist(table_relation, column_names, lookup_cache) -%}
                        {%- do dbt_constraints.create_not_null(table_relation, column_names, ns.verify_permissions, quote_columns, lookup_cache, rely_clause) -%}
                    {%- else  -%}
                        {%- do log("Skipping not null constraint because a physical column name was not found on the table: " ~ table_models[0].name ~ " " ~ column_names, info=true) -%}
                    {%- endif -%}
                {%- else  -%}
                    {%- do log("Skipping not null constraint because the table was not found in the database: " ~ table_models[0].name, info=true) -%}
                {%- endif -%}

            {%- endif -%}
        {%- endif -%}


    {%- endfor -%}

{%- endmacro -%}



{# This macro tests that all the column names passed to the macro can be found on the table, ignoring case #}
{%- macro table_columns_all_exist(table_relation, column_list, lookup_cache) -%}
    {%- set tab_column_list = dbt_constraints.lookup_table_columns(table_relation, lookup_cache) -%}

    {%- for column in column_list|map('upper') if column not in tab_column_list -%}
        {{ return(false) }}
    {%- endfor -%}
    {{ return(true) }}

{%- endmacro -%}


{%- macro lookup_table_columns(table_relation, lookup_cache) -%}
    {{ return(adapter.dispatch('lookup_table_columns', 'dbt_constraints')(table_relation, lookup_cache)) }}
{%- endmacro -%}


{%- macro default__lookup_table_columns(table_relation, lookup_cache) -%}
    {%- if table_relation not in lookup_cache.table_columns -%}
        {%- set tab_Columns = adapter.get_columns_in_relation(table_relation) -%}

        {%- set tab_column_list = [] -%}
        {%- for column in tab_Columns -%}
            {{ tab_column_list.append(column.name|upper) }}
        {%- endfor -%}
        {%- do lookup_cache.table_columns.update({ table_relation: tab_column_list }) -%}
    {%- endif -%}
    {{ return(lookup_cache.table_columns[table_relation]) }}
{%- endmacro -%}


{# This macro allows us to compare two sets of columns to see if they are the same, ignoring case #}
{%- macro column_list_matches(listA, listB) -%}
    {# Test if A is empty or the lists are not the same size #}
    {%- if listA | count > 0 and listA | count == listB | count  -%}
        {# Fail if there are any columns in A that are not in B #}
        {%- for valueFromA in listA|map('upper') -%}
            {%- if valueFromA|upper not in listB| map('upper')  -%}
                {{ return(false) }}
            {%- endif -%}
        {% endfor %}
        {# Since we know the count is the same, A must equal B #}
        {{ return(true) }}
    {%- else -%}
        {{ return(false) }}
    {%- endif -%}
{%- endmacro -%}
