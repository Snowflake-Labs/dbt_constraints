# Legacy Syntax Integration Tests

This project holds the integration tests for `dbt_constraints` that use the
**older generic test syntax**. Test arguments sit directly under the test name.

The sibling project [`../dbt-current-syntax`](../dbt-current-syntax) holds the
same models and tests in the current syntax.

## Which engines run this project

The test harness picks a project by dbt version, not by adapter. dbt-core added
the `arguments:` property for generic tests in 1.10.5. Every engine **before**
that version runs this project.

| Cell | Engine | Runs this project |
|---|---|---|
| `snowflake` | dbt-core 1.5.12 | Yes |
| `postgres`, `oracle`, `dpos_core` | dbt-core 1.10.5 and later | No |
| `fusion`, `core2`, `dpos_fusion` | dbt 2.x, dbt Fusion | No |

The harness makes this choice in `get_project_dir` in
`../automated_tests/conftest.py`. `CURRENT_SYNTAX_MIN_VERSION` holds the cutover
version.

## Do not modernise this project

Keep the older syntax here. This project is the only proof that the package still
works for users on dbt-core before 1.10.5. The package supports both forms, and
this project covers the older one.

Add new test coverage to **both** projects. Use the older form here and the
`arguments:` form in the current-syntax project.

## Configuration

- **Test arguments**: directly under the test name, with no `arguments:` property
- **`always_create_constraint`**: read directly from config, not from `meta`
- **Flags**: does not set `require_generic_test_arguments_property`

## Usage

```bash
cd integration_tests/dbt-legacy-syntax
dbt deps
dbt seed --full-refresh
dbt run
dbt test
```

## Differences from the current-syntax project

1. **Test arguments** sit directly under the test name.

   ```yaml
   - relationships:
       to: ref('parent')
       field: parent_id
   ```

2. **`always_create_constraint`** sits directly in config.

   ```yaml
   tests:
     +always_create_constraint: true
   ```

3. **Flags**: this project sets no `require_generic_test_arguments_property`
   flag.

Every other file matches the current-syntax project. Only the `schema.yml` files
differ.
