# Current Syntax Integration Tests

This project holds the integration tests for `dbt_constraints` that use the
**current generic test syntax**. Test arguments sit under an `arguments:`
property.

This is the project that most cells run. The sibling project
[`../dbt-legacy-syntax`](../dbt-legacy-syntax) holds the same models and tests in
the older syntax, for dbt-core versions before 1.10.5.

## Which engines run this project

The test harness picks a project by dbt version, not by adapter. dbt-core added
the `arguments:` property for generic tests in 1.10.5. Every engine at that
version **or later** runs this project, including recent dbt-core 1.x.

| Cell | Engine | Runs this project |
|---|---|---|
| `postgres` | dbt-core 1.11.x | Yes |
| `oracle` | dbt-core 1.12.x | Yes |
| `dpos_core` | dbt-core 1.11.x in Snowflake | Yes |
| `fusion` | dbt Fusion 2.x | Yes |
| `core2` | dbt-core 2.x | Yes |
| `dpos_fusion` | dbt Fusion in Snowflake | Yes |
| `snowflake` | dbt-core 1.5.12 | No |

The harness makes this choice in `get_project_dir` in
`../automated_tests/conftest.py`. `CURRENT_SYNTAX_MIN_VERSION` holds the cutover
version.

Because this project now runs against PostgreSQL and Oracle as well as
Snowflake, keep every model and macro portable across those adapters.

## Adding coverage

Add new test coverage to **both** projects. Use the `arguments:` form here and
the older form in the legacy-syntax project. Only the `schema.yml` files differ
between the two projects. Every other file must match.

## Configuration

- **Test arguments**: under an `arguments:` property
- **`always_create_constraint`**: inside a `+meta:` block. dbt Fusion reads this
  property from `meta` only. dbt-core reads it from either place.
- **Flags**: sets `require_generic_test_arguments_property: true`, which makes
  dbt reject the older argument form in this project

## Usage

```bash
cd integration_tests/dbt-current-syntax
dbt deps
dbt seed --full-refresh
dbt run
dbt test
```

## Differences from the legacy-syntax project

1. **Test arguments** sit under `arguments:`.

   ```yaml
   - relationships:
       arguments:
         to: ref('parent')
         field: parent_id
   ```

2. **`always_create_constraint`** sits inside `meta`.

   ```yaml
   tests:
     +meta:
       always_create_constraint: true
   ```

3. **Flags**: this project sets the argument property flag.

   ```yaml
   flags:
     require_generic_test_arguments_property: true
   ```

## dbt Fusion compatibility

Full PK / UK / FK / NN parity with dbt-core needs **dbt Fusion >=
`2.0.0-preview.176`**. That release shipped the upstream fix for
[dbt-fusion#1575](https://github.com/dbt-labs/dbt-fusion/issues/1575)
("test_metadata.kwargs missing custom arguments (values, to, field, etc.) in
manifest for parameterised generic tests"). The package needs that metadata to
create `relationships` and `foreign_key` constraints.

| Fusion version | PK / UK / NN | FK |
|---|---|---|
| `>= 2.0.0-preview.176` | Created | Created |
| `< 2.0.0-preview.176` | Created | Skipped |

An older Fusion version skips the foreign key and logs "Skipping foreign key on
... because pk_column_name/field is missing from test parameters". The package
degrades gracefully. PK, UK, and NN constraints are unaffected.

`tests/assert_fk_parity.sql` guards against a regression of this bug. That test
checks Snowflake only. It returns no rows on other adapters.
