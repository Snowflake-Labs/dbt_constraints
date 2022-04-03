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




{#- Define three create macros for PK, UK, and FK that
    can be overridden by DB implementations -#}

{%- macro create_primary_key(table_model, column_names, quote_columns=false) -%}
    {{ return(adapter.dispatch('create_primary_key', 'dbt_constraints')(table_model, column_names, quote_columns)) }}
{%- endmacro -%}


{%- macro create_unique_key(table_model, column_names, quote_columns=false) -%}
    {{ return(adapter.dispatch('create_unique_key', 'dbt_constraints')(table_model, column_names, quote_columns)) }}
{%- endmacro -%}


{%- macro create_foreign_key(test_model, pk_model, pk_column_names, fk_model, fk_column_names, quote_columns=false) -%}
    {{ return(adapter.dispatch('create_foreign_key', 'dbt_constraints')(test_model, pk_model, pk_column_names, fk_model, fk_column_names, quote_columns)) }}
{%- endmacro -%}



{#- Define two macros for detecting if PK, UK, and FK exist that
    can be overridden by DB implementations -#}

{%- macro unique_constraint_exists(table_relation, column_names) -%}
    {{ return(adapter.dispatch('unique_constraint_exists', 'dbt_constraints')(table_relation, column_names) ) }}
{%- endmacro -%}

{%- macro foreign_key_exists(table_relation, column_names) -%}
    {{ return(adapter.dispatch('foreign_key_exists', 'dbt_constraints')(table_relation, column_names)) }}
{%- endmacro -%}




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
            'relationships'],
        quote_columns=false) -%}
    {%- if execute and var('dbt_constraints_enabled', false) -%}
     
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
 
    {%- endif -%}

{%- endmacro -%}




{#- This macro is called internally and passed which constraint types to create. -#}
{%- macro create_constraints_by_type(constraint_types, quote_columns) -%}

    {#- Loop through the results and find all tests that passed and match the constraint_types -#}
    {%- for res in results
        if res.status == "pass" 
            and res.node.config.materialized == "test" 
            and res.node.test_metadata.name is in( constraint_types ) -%}
        
        {%- set test_model = res.node -%}
        {%- set test_parameters = test_model.test_metadata.kwargs -%}

        {#- Find the table models that are referenced by this test.
            These models must be physical tables and cannot be sources -#}
        {%- set table_models = [] -%}
        {%- for node in graph.nodes.values()
            | selectattr("resource_type", "equalto", "model")
            | selectattr("unique_id", "in", test_model.depends_on.nodes)
            if node.config.materialized in( ("table", "incremental", "snapshot") ) -%}

                {#- Append to our list of models for this test -#} 
                {%- do table_models.append(node) -%}

        {% endfor %}

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
            
            {%- set table_relation = adapter.get_relation(
                database=table_models[0].database,
                schema=table_models[0].schema,
                identifier=table_models[0].name) -%}
            {%- if dbt_constraints.table_columns_all_exist(table_relation, column_names) -%}
                {%- if test_model.test_metadata.name == "primary_key" -%}
                    {%- do dbt_constraints.create_primary_key(table_relation, column_names, quote_columns) -%}
                {%- else  -%}
                    {%- do dbt_constraints.create_unique_key(table_relation, column_names, quote_columns) -%}
                {%- endif -%}
            {%- else  -%}
                {%- do log("Skipping primary/unique key because a physical column name was not found on the table: " ~ table_models[0].name ~ " " ~ column_names ~ " in " ~ table_models[0].columns.values(), info=true) -%}
            {%- endif -%}

        {#- We only create FK if there are two models referenced by the test
            and if all the columns exist as physical columns on the tables -#} 
        {%- elif 2 == table_models|count 
            and test_model.test_metadata.name in( "foreign_key", "relationships") -%}
            
            {%- set fk_model_name = test_model.file_key_name |replace("models.", "") -%}

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
                "`pk_column_names`, `pk_column_name`, or `field` parameter missing for foreign key constraint on table: '" ~ fk_model_name ~ " " ~ test_parameters
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
                "`fk_column_names`, `fk_column_name`, or `column_name` parameter missing for foreign key constraint on table: '" ~ fk_model_name ~ " " ~ test_parameters
                ) }}
            {%- endif -%}
            
            {# The easiest way to identify which is the parent and child table is to compare the name to this constraint's file_key_name #}
            
            {%- set fk_model = table_models | selectattr("name", "equalto", fk_model_name)|first -%}
            {%- set fk_table_relation = adapter.get_relation(
                database=fk_model.database,
                schema=fk_model.schema,
                identifier=fk_model.name) -%}
            {%- set pk_model = table_models | rejectattr("name", "equalto", fk_model_name)|first -%}
            {%- set pk_table_relation = adapter.get_relation(
                database=pk_model.database,
                schema=pk_model.schema,
                identifier=pk_model.name) -%}

            {%- if not dbt_constraints.table_columns_all_exist(pk_table_relation, pk_column_names) -%}
                {%- do log("Skipping foreign key because a physical column was not found on the pk table: " ~ pk_column_names ~ " in " ~ pk_model.name, info=true) -%}
            {%- elif not dbt_constraints.table_columns_all_exist(fk_table_relation, fk_column_names) -%}
                {%- do log("Skipping foreign key because a physical column was not found on the fk table: " ~ fk_column_names ~ " in " ~ fk_model.name, info=true) -%}
            {%- else  -%}
                {%- do dbt_constraints.create_foreign_key(test_model, pk_table_relation, pk_column_names, fk_table_relation, fk_column_names, quote_columns) -%} 
            {%- endif -%}

        {%- endif -%}

    {%- endfor -%}

{%- endmacro -%}



{# This macro tests that all the column names passed to the macro can be found on the table, ignoring case #}
{%- macro table_columns_all_exist(table_relation, column_list) -%}

    {%- set tab_column_list = adapter.get_columns_in_relation(table_relation)|map('upper') -%}
    {%- for column in column_list|map('upper') if not column not in tab_column_list -%}
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
