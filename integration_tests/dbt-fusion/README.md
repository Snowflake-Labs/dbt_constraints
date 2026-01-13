# dbt Fusion Integration Tests

This directory contains integration tests for `dbt_constraints` with **dbt Fusion (v2.x)**.

## Configuration

- **Test format**: Uses the new YAML format with `arguments:` wrapper (required by Fusion)
- **Configuration**: `always_create_constraint` must be in `meta` block
- **Compatibility**: Designed for dbt Fusion 2.0.0-preview.x

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

## Known Issues

⚠️ **Test Metadata Compatibility Issue**

dbt Fusion currently has a known issue with how it exposes test metadata to Jinja macros:

- **Problem**: Test arguments are not included in `test_metadata.kwargs`
- **Impact**: The `dbt_constraints` package cannot access parameters like `pk_column_name`, `pk_table_name`, etc.
- **Status**: Tests will parse correctly but constraint creation may fail

This issue is being tracked and will be resolved in future Fusion releases.
