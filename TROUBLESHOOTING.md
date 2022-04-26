# Troubleshooting dbt Constraints

As it executes, dbt Constraints will log a number of messages when it cannot create a constraint. 
A list of the messages is provided below with additional details on how to address the message.

## Messages that are displayed on the front-end of the command line interface and in the dbt log

```
Skipping primary/unique key because a physical column name was not found on the table: {TABLE NAME} {TABLE COLUMNS}
Skipping foreign key because a physical column was not found on the pk table: {PK TABLE NAME} {PK TABLE COLUMNS}
Skipping foreign key because a physical column was not found on the fk table: {FK TABLE NAME} {FK TABLE COLUMNS}
```
- These error messages typically occur when a column is misspelled or if the test uses an expression instead of a column name. 
- One solution can be adding the expression as an additional column in your model so that you can reference it in your constraint.


```
"Skipping foreign key because a we couldn't find the child table: model={FK TABLE NAME} or source={PK TABLE NAME}
```
- This is only expected to occur when a foreign key constraint is made with a source and dbt Constraints can't parse the reference to a source table.
- The package is looking for something that looks like: `source("source_name", "table_name")` or `source('source_name', 'table_name')`
- You may need to replace any dynamic variables with strings for the source name or table name.


```
Skipping {CONSTRAINT NAME} because of insufficient privileges: {FK TABLE NAME} referencing {PK TABLE NAME}
Skipping {CONSTRAINT NAME} because of insufficient privileges: {TABLE NAME}
```
- You must have OWNERSHIP on the child FK table and you must have REFERENCES on the parent PK table.
- For primary keys and unique keys, you need ownership on the table.
- These errors most frequently apply to sources.
- This can also indicate that one of your tables is actually a view


```
Skipping {CONSTRAINT NAME} because a PK/UK was not found on the PK table: {PK TABLE NAME} {PK TABLE COLUMNS}
```
- You either need to manually create a primary key/unique key or you need to add a test to the parent table and allow the package to create the constraint.
- The package creates constraints in the order of primary keys, unique keys, foreign keys to allow parent constraints to be referenced by foreign keys.


## Messages that are only displayed in the dbt log:

```
Skipping {CONSTRAINT NAME} because PK/UK already exists: {TABLE NAME} {TABLE COLUMNS}
Skipping {CONSTRAINT NAME} because FK already exists: {FK TABLE NAME} {FK TABLE COLUMNS}
```
- Indicates duplicate constraints or that a constraint was already added to an incremental / snapshot table on a previous run
- Typically not an issue
