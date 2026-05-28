# dbt Fusion Integration Tests

This directory contains integration tests for `dbt_constraints` with **dbt Fusion (v2.x)**.

## Configuration

- **Test format**: Uses the new YAML format with `arguments:` wrapper (required by Fusion)
- **Configuration**: `always_create_constraint` must be in `meta` block
- **Tested with**: dbt Fusion `2.0.0-preview.178`
- **Minimum version for full FK support**: dbt Fusion `2.0.0-preview.176` (see Compatibility below)

## Usage

```bash
cd integration_tests/dbt-fusion
dbt deps
dbt seed --full-refresh
dbt run
dbt test
```

## Differences from dbt-core

The key differences from the `dbt-core` project:

1. **Test Arguments**: Parameters MUST be wrapped in `arguments:` block
   ```yaml
   # dbt Fusion format (required)
   - relationships:
       arguments:
         to: ref('parent')
         field: parent_id
   ```

2. **Configuration**: `always_create_constraint` must be in `meta`
   ```yaml
   # dbt Fusion format (required)
   tests:
     +meta:
       always_create_constraint: true
   ```

3. **Flags**: Requires `require_generic_test_arguments_property: true`
   ```yaml
   flags:
     require_generic_test_arguments_property: true
   ```

## Compatibility

Full PK / UK / FK / NN parity with dbt-core requires **dbt Fusion >= `2.0.0-preview.176`**.
That release shipped the upstream fix for [dbt-fusion#1575](https://github.com/dbt-labs/dbt-fusion/issues/1575)
("test_metadata.kwargs missing custom arguments (values, to, field, etc.) in
manifest for parameterised generic tests"), which is the metadata `dbt_constraints`
needs to drive `relationships` / `foreign_key` constraints.

| Fusion version | PK / UK / NN | FK |
|---|---|---|
| `>= 2.0.0-preview.176` (incl. `preview.178`) | Created | Created |
| `<  2.0.0-preview.176` | Created | Skipped with an info-level log message ("Skipping foreign key on ... because pk_column_name/field is missing from test parameters") because the `to` / `field` arguments are not exposed by older Fusion to the package |

The package always degrades gracefully on older Fusion versions; PK / UK / NN
constraints are unaffected, only FK creation is skipped.
