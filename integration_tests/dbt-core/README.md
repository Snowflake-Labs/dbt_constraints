# dbt-core Integration Tests

This directory contains integration tests for `dbt_constraints` with **dbt-core (v1.x)**.

## Configuration

- **Test format**: Uses the original YAML format without `arguments:` wrapper
- **Configuration**: `always_create_constraint` can be used directly (not required in `meta`)
- **Compatibility**: Designed for dbt-core 1.5 through 1.11

## Usage

```bash
cd integration_tests/dbt-core
dbt deps
dbt seed --full-refresh
dbt run
dbt test
```

## Differences from dbt-fusion

The key differences from the `dbt-fusion` project:

1. **Test Arguments**: Parameters are at the top level, not wrapped in `arguments:`
   ```yaml
   # dbt-core format
   - relationships:
       to: ref('parent')
       field: parent_id
   ```

2. **Configuration**: `always_create_constraint` works at the top level
   ```yaml
   # dbt-core format
   tests:
     +always_create_constraint: true
   ```

3. **Flags**: Does not require `require_generic_test_arguments_property`
