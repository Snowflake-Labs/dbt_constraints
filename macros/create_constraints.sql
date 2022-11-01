{#- Define three tests for PK, UK, and FK that can be overridden by DB implementations.
    These tests have overloaded parameter names to be as flexible as possible. -#}

{%- test primary_key(model,
        column_name=none, column_names=[],
        quote_columns=false) -%}

    {%- if column_names|count == 0 and column_name -%}
        {%- do column_names.append(column_name) -%}
    {%- endif -%}

    {{ return(adapter.dispatch('test_primary_key', 'dbt_constraints')(model, column_names, quote_columns)) }}

{%- endtest -%}


{%- test unique_key(model,
        column_name=none, column_names=[],
        quote_columns=false) -%}

    {%- if column_names|count == 0 and column_name -%}
        {%- do column_names.append(column_name) -%}
    {%- endif -%}

    {{ return(adapter.dispatch('test_unique_key', 'dbt_constraints')(model, column_names, quote_columns)) }}

{%- endtest -%}


{%- test foreign_key(model,
        column_name=none, fk_column_name=none, fk_column_names=[],
        pk_table_name=none, to=none,
        pk_column_name=none, pk_column_names=[], field=none,
        quote_columns=false) -%}

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

{%- macro create_primary_key(table_model, column_names, verify_permissions, quote_columns=false) -%}
    {{ return(adapter.dispatch('create_primary_key', 'dbt_constraints')(table_model, column_names, verify_permissions, quote_columns)) }}
{%- endmacro -%}


{%- macro create_unique_key(table_model, column_names, verify_permissions, quote_columns=false) -%}
    {{ return(adapter.dispatch('create_unique_key', 'dbt_constraints')(table_model, column_names, verify_permissions, quote_columns)) }}
{%- endmacro -%}


{%- macro create_foreign_key(pk_model, pk_column_names, fk_model, fk_column_names, verify_permissions, quote_columns=false) -%}
    {{ return(adapter.dispatch('create_foreign_key', 'dbt_constraints')(pk_model, pk_column_names, fk_model, fk_column_names, verify_permissions, quote_columns)) }}
{%- endmacro -%}


{%- macro create_not_null(table_model, column_names, verify_permissions, quote_columns=false) -%}
    {{ return(adapter.dispatch('create_not_null', 'dbt_constraints')(table_model, column_names, verify_permissions, quote_columns)) }}
{%- endmacro -%}


{#- Define two macros for detecting if PK, UK, and FK exist that can be overridden by DB implementations -#}

{%- macro unique_constraint_exists(table_relation, column_names) -%}
    {{ return(adapter.dispatch('unique_constraint_exists', 'dbt_constraints')(table_relation, column_names) ) }}
{%- endmacro -%}

{%- macro foreign_key_exists(table_relation, column_names) -%}
    {{ return(adapter.dispatch('foreign_key_exists', 'dbt_constraints')(table_relation, column_names)) }}
{%- endmacro -%}


{#- Define two macros for detecting if we have sufficient privileges that can be overridden by DB implementations -#}

{%- macro have_references_priv(table_relation, verify_permissions) -%}
    {{ return(adapter.dispatch('have_references_priv', 'dbt_constraints')(table_relation, verify_permissions) ) }}
{%- endmacro -%}

{%- macro have_ownership_priv(table_relation, verify_permissions) -%}
    {{ return(adapter.dispatch('have_ownership_priv', 'dbt_constraints')(table_relation, verify_permissions)) }}
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
    {%- if execute and var('dbt_constraints_enabled', false) -%}
        {%- do log("Running dbt Constraints", info=true) -%}

        {%- if 'not_null' in constraint_types -%}
            {%- do dbt_constraints.create_constraints_by_type(['not_null'], quote_columns) -%}
        {%- endif -%}
        {%- if 'primary_key' in constraint_types -%}
            {%- do dbt_constraints.create_constraints_by_type(['primary_key'], quote_columns) -%}
        {%- endif -%}
        {%- if 'unique_key' in constraint_types -%}
            {%- do dbt_constraints.create_constraints_by_type(['unique_key'], quote_columns) -%}
        {%- endif -%}
        {%- if 'unique_combination_of_columns' in constraint_types -%}
            {%- do dbt_constraints.create_constraints_by_type(['unique_combination_of_columns'], quote_columns) -%}
        {%- endif -%}
        {%- if 'unique' in constraint_types -%}
            {%- do dbt_constraints.create_constraints_by_type(['unique'], quote_columns) -%}
        {%- endif -%}
        {%- if 'foreign_key' in constraint_types -%}
            {%- do dbt_constraints.create_constraints_by_type(['foreign_key'], quote_columns) -%}
        {%- endif -%}
        {%- if 'relationships' in constraint_types -%}
            {%- do dbt_constraints.create_constraints_by_type(['relationships'], quote_columns) -%}
        {%- endif -%}

        {%- do log("Finished dbt Constraints", info=true) -%}
    {%- endif -%}

{%- endmacro -%}




{#- This macro is called internally and passed which constraint types to create. -#}
{%- macro create_constraints_by_type(constraint_types, quote_columns) -%}

    {#- Loop through the results and find all tests that passed and match the constraint_types -#}
    {#- Issue #2: added condition that the where config must be empty -#}
    {%- for res in results
        if res.status == "pass"
            and res.node.config.materialized == "test"
            and res.node.test_metadata
            and res.node.test_metadata.name is in( constraint_types )
            and res.node.config.where is none -%}

        {%- set test_model = res.node -%}
        {%- set test_parameters = test_model.test_metadata.kwargs -%}
        {% set ns = namespace(verify_permissions=false) %}

        {#- Find the table models that are referenced by this test.
            These models must be physical tables and cannot be sources -#}
        {%- set table_models = [] -%}
        {%- for node in graph.nodes.values() | selectattr("unique_id", "in", test_model.depends_on.nodes)
                if node.resource_type in ( ( "model", "snapshot") )
                    if node.config.materialized in( ("table", "incremental", "snapshot") ) -%}

                        {#- Append to our list of models &or snapshots for this test -#}
                        {%- do table_models.append(node) -%}

        {% endfor %}

        {#- Check if we allow constraints on sources overall and for this specific type of constraint -#}
        {%- if var('dbt_constraints_sources_enabled', false) and (
                ( var('dbt_constraints_sources_pk_enabled', false) and test_model.test_metadata.name in("primary_key") )
             or ( var('dbt_constraints_sources_uk_enabled', false) and test_model.test_metadata.name in("unique_key", "unique_combination_of_columns", "unique") )
             or ( var('dbt_constraints_sources_fk_enabled', false) and test_model.test_metadata.name in("foreign_key", "relationships") )
             or ( var('dbt_constraints_sources_nn_enabled', false) and test_model.test_metadata.name in("not_null") )
            ) -%}
            {%- for node in graph.sources.values()
                | selectattr("resource_type", "equalto", "source")
                | selectattr("unique_id", "in", test_model.depends_on.nodes) -%}

                    {%- do node.update({'alias': node.alias or node.name }) -%}
                    {#- Append to our list of models for this test -#}
                    {%- do table_models.append(node) -%}
                    {#- If we are using a sources, we will need to verify permissions -#}
                    {%- set ns.verify_permissions = true -%}

            {%- endfor -%}
        {%- endif -%}


        {#- We only create PK/UK if there is one model referenced by the test
            and if all the columns exist as physical columns on the table -#}
        {%- if 1 == table_models|count
            and test_model.test_metadata.name in("primary_key", "unique_key", "unique_combination_of_columns", "unique") -%}

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

            {%- set table_relation = api.Relation.create(
                database=table_models[0].database,
                schema=table_models[0].schema,
                identifier=table_models[0].alias ) -%}
            {%- if dbt_constraints.table_columns_all_exist(table_relation, column_names) -%}
                {%- if test_model.test_metadata.name == "primary_key" -%}
                    {%- do dbt_constraints.create_not_null(table_relation, column_names, ns.verify_permissions, quote_columns) -%}
                    {%- do dbt_constraints.create_primary_key(table_relation, column_names, ns.verify_permissions, quote_columns) -%}
                {%- else  -%}
                    {%- do dbt_constraints.create_unique_key(table_relation, column_names, ns.verify_permissions, quote_columns) -%}
                {%- endif -%}
            {%- else  -%}
                {%- do log("Skipping primary/unique key because a physical column name was not found on the table: " ~ table_models[0].name ~ " " ~ column_names, info=true) -%}
            {%- endif -%}

        {#- We only create FK if there are two models referenced by the test
            and if all the columns exist as physical columns on the tables -#}
        {%- elif 2 == table_models|count
            and test_model.test_metadata.name in( "foreign_key", "relationships") -%}

            {%- set fk_model = none -%}
            {%- set pk_model = none -%}
            {%- set fk_model_names = modules.re.findall( "(models|snapshots)\W+(\w+)" , test_model.file_key_name)  -%}
            {%- set fk_source_names = modules.re.findall( "source\W+(\w+)\W+(\w+)" , test_parameters.model)  -%}

            {%- if 1 == fk_model_names | count -%}
                {%- set fk_model = table_models | selectattr("name", "equalto", fk_model_names[0][1]) | first -%}
                {%- set pk_model = table_models | rejectattr("name", "equalto", fk_model_names[0][1]) | first -%}
            {%- elif 1 == fk_source_names | count  -%}
                {%- if table_models[0].source_name == fk_source_names[0][0] and table_models[0].name == fk_source_names[0][1] -%}
                    {%- set fk_model = table_models[0] -%}
                    {%- set pk_model = table_models[1] -%}
                {%- else  -%}
                    {%- set fk_model = table_models[1] -%}
                    {%- set pk_model = table_models[0] -%}
                {%- endif -%}
            {%- endif -%}
            {# {%- set fk_model_name = test_model.file_key_name |replace("models.", "") -%} #}

            {%- if fk_model and pk_model -%}

                {%- set fk_table_relation = api.Relation.create(
                    database=fk_model.database,
                    schema=fk_model.schema,
                    identifier=fk_model.alias) -%}

                {%- set pk_table_relation = api.Relation.create(
                    database=pk_model.database,
                    schema=pk_model.schema,
                    identifier=pk_model.alias) -%}

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

                {%- if not dbt_constraints.table_columns_all_exist(pk_table_relation, pk_column_names) -%}
                    {%- do log("Skipping foreign key because a physical column was not found on the pk table: " ~ pk_model.name ~ " " ~ pk_column_names, info=true) -%}
                {%- elif not dbt_constraints.table_columns_all_exist(fk_table_relation, fk_column_names) -%}
                    {%- do log("Skipping foreign key because a physical column was not found on the fk table: " ~ fk_model.name ~ " " ~ fk_column_names, info=true) -%}
                {%- else  -%}
                    {%- do dbt_constraints.create_foreign_key(pk_table_relation, pk_column_names, fk_table_relation, fk_column_names, ns.verify_permissions, quote_columns) -%}
                {%- endif -%}
            {%- else  -%}
                {%- do log("Skipping foreign key because a we couldn't find the child table: model=" ~ fk_model_names ~ " or source=" ~ fk_source_names, info=true) -%}
            {%- endif -%}

        {#- We only create NN if there is one model referenced by the test
            and if all the columns exist as physical columns on the table -#}
        {%- elif 1 == table_models|count
            and test_model.test_metadata.name in("not_null") -%}

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

            {%- set table_relation = api.Relation.create(
                database=table_models[0].database,
                schema=table_models[0].schema,
                identifier=table_models[0].alias ) -%}

            {%- if dbt_constraints.table_columns_all_exist(table_relation, column_names) -%}
                {%- do dbt_constraints.create_not_null(table_relation, column_names, ns.verify_permissions, quote_columns) -%}
            {%- else  -%}
                {%- do log("Skipping not null constraint because a physical column name was not found on the table: " ~ table_models[0].name ~ " " ~ column_names, info=true) -%}
            {%- endif -%}

        {%- endif -%}

    {%- endfor -%}

{%- endmacro -%}



{# This macro tests that all the column names passed to the macro can be found on the table, ignoring case #}
{%- macro table_columns_all_exist(table_relation, column_list) -%}
    {%- set tab_Columns = adapter.get_columns_in_relation(table_relation) -%}

    {%- set tab_column_list = [] -%}
    {%- for column in tab_Columns -%}
        {{ tab_column_list.append(column.name|upper) }}
    {%- endfor -%}

    {%- for column in column_list|map('upper') if column not in tab_column_list -%}
        {{ return(false) }}
    {%- endfor -%}
    {{ return(true) }}

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
