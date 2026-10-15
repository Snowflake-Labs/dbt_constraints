# Issue #105 Test Macros

This directory contains custom dbt macros used for testing the fix to issue #105.

## Issue #105 Overview

**Problem:** Foreign key creation didn't respect customized properties of referenced models when using custom `generate_*_name()` macros.

**Fix:** Modified `macros/create_constraints.sql` to prioritize processed values over raw config values.

## Macros

These macros transform model properties during compilation to test that foreign key lookups work correctly with customized names:

### `generate_schema_name.sql`

Transforms schema names by replacing `<env>` placeholders with the `DBT_TEST_ENV` environment variable.

**Example:**

```yaml
config:
  schema: 'test_schema_<env>'
```

With `DBT_TEST_ENV=dev` → Compiles to: `test_schema_dev`

**Usage in test models:**

- `issue_105_parent_custom_schema.sql`
- `issue_105_child_custom_schema.sql`
- `issue_105_*_all_custom.sql`

### `generate_alias_name.sql`

Transforms alias names by replacing `<suffix>` placeholders with the `DBT_TEST_SUFFIX` environment variable.

**Example:**

```yaml
config:
  alias: 'parent_with_alias<suffix>'
```

With `DBT_TEST_SUFFIX=_test` → Compiles to: `parent_with_alias_test`

**Usage in test models:**

- `issue_105_parent_custom_alias.sql`
- `issue_105_child_custom_alias.sql`
- `issue_105_*_all_custom.sql`

### `generate_database_name.sql`

Transforms database names, particularly handling the `custom_db` value for testing.

**Example:**

```yaml
config:
  database: 'custom_db'
```

With `DBT_TEST_DB_PREFIX=user_` → Compiles to: `user_{target.database}`

**Usage in test models:**

- `issue_105_parent_custom_database.sql`
- `issue_105_child_custom_database.sql`
- `issue_105_*_all_custom.sql`

## Environment Variables

Control macro behavior with these environment variables:

```bash
# Schema name suffix (default: 'dev')
export DBT_TEST_ENV=dev

# Alias name suffix (default: '_test')
export DBT_TEST_SUFFIX=_test

# Database name prefix (default: '{user}_')
export DBT_TEST_DB_PREFIX=myuser_
```

## How It Tests Issue #105

Before the fix, when a foreign key referenced a parent table with custom names:

1. **The Bug:** Code used `pk_model.config.schema` (raw value: `test_schema_<env>`)
2. **Symptoms:** Cache miss errors, SQL compilation errors with `<` characters
3. **Result:** Foreign key constraints failed to create

After the fix:

1. **The Fix:** Code uses `pk_model.schema` (processed value: `test_schema_dev`)
2. **Result:** Foreign key lookups work correctly
3. **Verified by:** Tests in `automated_tests/test_issue_105.py`

## Testing

Run tests that use these macros:

```bash
# All issue #105 tests
cd integration_tests
make test-issue-105

# Quick verification
make verify-fix

# Specific test
pytest automated_tests/test_issue_105.py -v
```

## Related Files

- **Test Models:** `../models/issue_105/*.sql`
- **Test Config:** `../models/issue_105/schema.yml`
- **Pytest Tests:** `../automated_tests/test_issue_105.py`
- **Fix:** `../../macros/create_constraints.sql` (lines 413-415)
