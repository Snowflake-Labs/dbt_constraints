# Integration Tests

This directory holds the integration tests for `dbt_constraints`.
The tests use two separate dbt projects.
The test harness picks a project by dbt version.

## Directory Structure

```
integration_tests/
├── dbt-legacy-syntax/        # For dbt-core before 1.10.5
│   ├── data/                 # Seed data and seed tests
│   ├── models/               # Test models and schema.yml
│   ├── macros/               # Test macros
│   ├── dbt_project.yml       # Project config
│   ├── packages.yml          # Package dependencies
│   └── profiles.yml          # Connection profiles
├── dbt-current-syntax/       # For dbt-core 1.10.5 and later
│   ├── data/                 # Seed data and seed tests
│   ├── models/               # Test models and schema.yml
│   ├── macros/               # Test macros
│   ├── dbt_project.yml       # Project config
│   ├── packages.yml          # Package dependencies
│   └── profiles.yml          # Connection profiles
├── automated_tests/          # Pytest-based automation
│   ├── tests/                # Test files
│   ├── conftest.py           # Test configuration
│   ├── config/               # dbt version matrix
│   └── docker/               # Docker infrastructure
├── .env                      # Snowflake credentials (not in git)
└── .dockerenv/               # Docker-specific profiles
```

Each project has its own README.
Read `dbt-legacy-syntax/README.md` and `dbt-current-syntax/README.md` for the full detail.

## Why Two Projects

The harness selects a project by dbt version, not by adapter.
dbt-core added the `arguments:` property for generic tests in 1.10.5.

- A dbt version of 1.10.5 or later runs `dbt-current-syntax`.
- A dbt version earlier than 1.10.5 runs `dbt-legacy-syntax`.

`get_project_dir` in `automated_tests/conftest.py` makes this choice.
`CURRENT_SYNTAX_MIN_VERSION` holds the cutover version.

## Project Selection Matrix

| Cell | Engine | Project |
|---|---|---|
| `snowflake` | dbt-core 1.5.12 | `dbt-legacy-syntax` |
| `postgres` | dbt-core 1.11.x | `dbt-current-syntax` |
| `oracle` | dbt-core 1.12.x | `dbt-current-syntax` |
| `dpos_core` | dbt-core in Snowflake | `dbt-current-syntax` |
| `fusion` | dbt Fusion 2.x | `dbt-current-syntax` |
| `core2` | dbt-core 2.x | `dbt-current-syntax` |
| `dpos_fusion` | dbt Fusion in Snowflake | `dbt-current-syntax` |

The current-syntax project is the primary project.
It also runs on PostgreSQL and Oracle.
Keep every model and macro portable across those adapters.

## Differences Between the Projects

The two projects hold the same models, seeds, and macros.
Only the YAML that declares generic tests differs.

The current-syntax project:
- nests test arguments under `arguments:`. The legacy project places them directly under the test name.
- sets `always_create_constraint` inside a `+meta:` block. The legacy project sets it directly in config.
- sets `flags: require_generic_test_arguments_property: true`. The legacy project sets no flag.

The two project READMEs show the YAML examples for each form.

## Adding Coverage

Add new test coverage to **both** projects.
Use the matching syntax in each project.
Do not modernise the legacy project.
It is the only proof that the package still supports dbt-core before 1.10.5.

## Running Tests Manually

### dbt-legacy-syntax

```bash
cd integration_tests/dbt-legacy-syntax
dbt deps
dbt seed --full-refresh
dbt run
dbt test
```

### dbt-current-syntax

```bash
cd integration_tests/dbt-current-syntax
dbt deps
dbt seed --full-refresh
dbt run
dbt test
```

## Automated Tests

The automated suite selects the project for you.
It reads the cell's dbt version.

```bash
cd integration_tests/automated_tests

# Test a specific database
python3 -m pytest --database postgres

# Test with dbt Fusion
python3 -m pytest --database fusion

# Run all tests
python3 -m pytest
```

## Git History

The project directories were renamed with `git mv`.
This preserves the git history.
