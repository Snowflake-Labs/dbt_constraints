# Integration Tests

This directory contains integration tests for the `dbt_constraints` package, split into two separate dbt projects to support both dbt-core and dbt Fusion.

## Directory Structure

```
integration_tests/
├── dbt-core/              # dbt-core (v1.x) project
│   ├── data/              # Seed data
│   ├── models/            # Test models
│   ├── macros/            # Test macros
│   ├── dbt_project.yml    # dbt-core compatible config
│   ├── packages.yml       # Package dependencies
│   └── profiles.yml       # Connection profiles
├── dbt-fusion/            # dbt Fusion (v2.x) project
│   ├── data/              # Seed data (with arguments: wrapper)
│   ├── models/            # Test models (with arguments: wrapper)
│   ├── macros/            # Test macros
│   ├── dbt_project.yml    # Fusion compatible config
│   ├── packages.yml       # Package dependencies
│   └── profiles.yml       # Connection profiles
├── automated_tests/       # Pytest-based automation
│   ├── tests/             # Test files
│   ├── conftest.py        # Test configuration
│   └── docker/            # Docker infrastructure
├── .env                   # Snowflake credentials (not in git)
└── .dockerenv/            # Docker-specific profiles
```

## Why Two Projects?

dbt Fusion (v2.x) enforces stricter YAML formatting than dbt-core (v1.x):

### Key Differences

| Feature | dbt-core | dbt Fusion |
|---------|----------|------------|
| Test arguments | Top-level | Must be in `arguments:` block |
| Configuration | `+always_create_constraint` | Must be in `+meta:` block |
| Flag required | No | `require_generic_test_arguments_property: true` |

### Example

**dbt-core format:**
```yaml
- relationships:
    to: ref('parent')
    field: parent_id
```

**dbt Fusion format:**
```yaml
- relationships:
    arguments:
      to: ref('parent')
      field: parent_id
```

## Running Tests Manually

### dbt-core

```bash
cd integration_tests/dbt-core
dbt deps
dbt seed --full-refresh
dbt run
dbt test
```

### dbt Fusion

```bash
cd integration_tests/dbt-fusion
dbt deps
dbt seed --full-refresh  # Known to fail - see Fusion compatibility issue
dbt run
dbt test
```

## Automated Tests

The automated test suite automatically uses the correct project based on the database being tested:

```bash
cd integration_tests/automated_tests

# Test with dbt-core (Postgres example)
pytest --database postgres

# Test with dbt Fusion
pytest --database fusion

# Run all tests
pytest
```

## Known Issues

### dbt Fusion Compatibility

⚠️ **Test Metadata Access Issue**

dbt Fusion currently has a compatibility issue with the `dbt_constraints` package:

- **Problem**: Test arguments are not included in `test_metadata.kwargs`
- **Impact**: Constraint creation fails because parameters like `pk_column_name` cannot be accessed
- **Status**: Being investigated

The Fusion project is structured correctly with the required YAML format, but constraints cannot be created until this metadata access issue is resolved.

## Git History

The files in `dbt-core/` were moved using `git mv` to preserve git history. The `dbt-fusion/` project was copied from `dbt-core/` and modified with Fusion-compatible YAML formatting.
