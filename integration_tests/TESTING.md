# Integration Testing Guide

Comprehensive pytest-based testing framework for dbt_constraints across multiple database platforms and dbt versions.

## Directory Structure

```
integration_tests/
├── automated_tests/          # pytest-based automated tests
│   ├── tests/               # Test files
│   │   ├── test_dbt_versions.py
│   │   ├── test_issue_105.py
│   │   └── test_constraints.py
│   ├── conftest.py          # pytest configuration & fixtures
│   ├── docker/              # Docker build & compose files
│   │   ├── build/           # dbt runner Dockerfile
│   │   └── compose/         # Database containers
│   ├── config/              # Test configuration
│   │   └── test-versions.json
│   ├── pytest.ini           # pytest settings
│   ├── requirements-test.txt
│   ├── README.md            # Detailed documentation
│   └── ARCHITECTURE.md      # Technical architecture
├── models/                   # dbt test models
│   └── issue_105/           # Issue-specific models
├── macros/                   # Test macros
│   └── issue_105/           # Issue-specific macros
├── tests/                    # dbt singular tests
└── .env                      # Credentials (gitignored)
```

## Quick Start

```bash
cd integration_tests/automated_tests

# Install dependencies
uv pip install -r requirements-test.txt

# Test PostgreSQL with dbt 1.11.14 (fastest)
python3 -m pytest --database postgres --dbt-version 1.11.14

# Test all databases and versions
python3 -m pytest

# Fast mode (quick validation only)
python3 -m pytest --fast --database postgres --dbt-version 1.11.14
```

Use `python3 -m pytest`, not a bare `pytest`. This directory contains an
`__init__.py`, so pytest puts its parent on `sys.path` and `import dbt_venv` fails.
`python3 -m` puts the working directory on `sys.path` and the import resolves.

## Testing Snowflake

Snowflake requires credentials in `integration_tests/.env`:

```bash
# Create .env file
cat > integration_tests/.env << 'EOF'
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USER=your-username
SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/your/key.p8
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=your-passphrase
SNOWFLAKE_ROLE=your-role
SNOWFLAKE_DATABASE=your-database
SNOWFLAKE_WAREHOUSE=your-warehouse
SNOWFLAKE_SCHEMA=dbt_constraints_test
# Only the in-database cells need this. It must name a connections.toml entry for
# the SAME account as SNOWFLAKE_ACCOUNT above.
SNOWFLAKE_CONNECTION_NAME=your-snow-cli-connection
EOF

# Test Snowflake with a dbt client on this machine
cd automated_tests
python3 -m pytest --database snowflake --dbt-version 1.5.12
```

## Testing inside Snowflake (dbt Projects on Snowflake)

Two cells run dbt inside Snowflake instead of on this machine. Snowflake supplies the
engine, so no dbt client installs locally for the run.

```bash
python3 -m pytest --database dpos_core     # dbt Core 1.11.11, in Snowflake
python3 -m pytest --database dpos_fusion   # dbt Fusion 2.0.0-preview.186, in Snowflake
```

These cells need `SNOWFLAKE_CONNECTION_NAME` in `.env` as well as the `SNOWFLAKE_*`
values. The `snow` CLI reads its credentials from `connections.toml`, so no secret
passes through the test harness for this path. There is no default connection name: one
would deploy to whichever account `connections.toml` marks as `default`. The cells skip
with an explanation when the variable is absent.

What happens on the first test of a session:

1. The harness copies the project and the package under test into
   `automated_tests/.dpos-stage/<cell>/` and runs `dbt deps` there. Snowflake rejects a
   package path that leaves the project root, so `- local: ../../` cannot be deployed
   as it stands.
2. `snow dbt deploy` creates a new version of `DBT_CONSTRAINTS_DPOS_CORE` or
   `DBT_CONSTRAINTS_DPOS_FUSION` in `SNOWFLAKE_DATABASE.SNOWFLAKE_SCHEMA`.
3. Each test command runs as `snow dbt execute`.

Each cell writes to its own schema, so the three groups never collide:

| Cells | Schema |
|---|---|
| host cells | `dbt_constraints_test` |
| `dpos_core` | `dbt_constraints_dpos_core` |
| `dpos_fusion` | `dbt_constraints_dpos_fusion` |

These cells read `dbt_projects_profiles.yml` and `env.yml` in the project directory.
Standard dbt ignores both filenames, so your local `profiles.yml` workflow is unchanged.

Some commands cannot run in this mode and skip with a reason: `dbt clean` and `dbt debug`
are unsupported, and `dbt deps` against a deployed object does nothing because the object
is immutable.

Check what an account supports with `SELECT SYSTEM$SUPPORTED_DBT_VERSIONS();`.

## Testing Issue #105

Issue #105: Foreign key creation didn't respect custom database/schema/alias properties.

```bash
cd integration_tests/automated_tests

# Test Issue #105 regression (all scenarios)
python3 -m pytest tests/test_issue_105.py --database postgres --dbt-version 1.11.14 -v

# Quick regression check
python3 -m pytest tests/test_issue_105.py::test_issue_105_regression --database postgres --dbt-version 1.11.14
```

## Database Support

| Database   | Status | Auth Method | Notes |
|------------|--------|-------------|-------|
| PostgreSQL | ✅     | Auto-generated | Fast, recommended for CI |
| Oracle     | ✅     | Auto-generated | Slow startup (~5 min) |
| SQL Server | ❌     | n/a | Not supported: the package has no sqlserver__create_constraints macro |
| Snowflake  | ✅     | Private key (`.env`) | Cloud service, no container needed |
| dbt Fusion | ✅     | Private key (`.env`) | dbt v2 engine, runs on Snowflake |
| dbt Core 2 | ✅     | Private key (`.env`) | dbt v2 engine, runs on Snowflake |
| dpos_core  | ✅     | snow CLI (`connections.toml`) | dbt Core runs INSIDE Snowflake |
| dpos_fusion| ✅     | snow CLI (`connections.toml`) | dbt Fusion runs INSIDE Snowflake |

**Note**: Local databases (PostgreSQL, Oracle, SQL Server) use randomly generated credentials per test session.

## dbt v2 Engines

The matrix has two dbt v2 engines. They are different products. Do not confuse them.

- **fusion** (package `dbt`): dbt Fusion. It is the Rust engine. It adds SQL comprehension, LSP features and `dbt lint` on top of dbt Core 2.0.
- **core2** (package `dbt-core` 2.x): dbt Core 2.0. It is the Apache 2.0 licensed foundation behind Fusion. It does not include SQL comprehension, LSP or `dbt lint`.

Both are single self-contained wheels. Neither needs an adapter package or dbt-adapters. Both run against Snowflake. Both use the `integration_tests/dbt-fusion` project directory.

```bash
python3 -m pytest --database fusion --dbt-version 2.0.0rc212
python3 -m pytest --database core2 --dbt-version 2.0.0b2
```

Both run a dbt client on this machine. The `dpos_fusion` cell runs the same Fusion engine
inside Snowflake instead. dbt Core 2.0 betas are not a dbt Projects on Snowflake engine,
so `core2` has no in-database counterpart.

### dbt Version Pinning

All versions are exact patch pins, not minors. `dbt_venv.py` pins `dbt-core` exactly and
pins the adapter to the matching minor only. Adapter patch numbers are independent of
core patch numbers. All installs use uv.

The in-database cells use the Snowflake version string, which differs from the PyPI one.
Snowflake reports `2.0.0-preview.186` where PyPI publishes `2.0.0rc186`. They are the
same engine build.

> WARNING: Never run an unpinned `uv pip install dbt`. It resolves to version 1.0.0.40.21, the old dbt Cloud CLI. That tool is completely different and installs without error. Only `2.0.0rcNNN` releases of the `dbt` package are Fusion. Exact pins are mandatory.

## Manual dbt Testing

For quick manual verification:

```bash
cd integration_tests

# Run all tests
dbt test

# Full build with refresh
dbt build --full-refresh

# Test specific models
dbt test --select issue_105*
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Integration Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        database: [postgres, oracle, sqlserver]

    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          cd integration_tests/automated_tests
          uv pip install -r requirements-test.txt

      - name: Run tests
        run: |
          cd integration_tests/automated_tests
          python3 -m pytest --database ${{ matrix.database }}
```

### Parallel Testing

```yaml
strategy:
  matrix:
    database: [postgres, oracle, sqlserver]
    dbt-version: ['1.8.10', '1.9.11']
  max-parallel: 6
```

## Common Commands

```bash
cd integration_tests/automated_tests

# Single database, all versions
python3 -m pytest --database postgres

# Single database, specific version
python3 -m pytest --database postgres --dbt-version 1.11.14

# Specific test file
python3 -m pytest tests/test_issue_105.py --database postgres

# Verbose output with logs
python3 -m pytest -v --log-cli-level=DEBUG --database postgres

# Fast validation mode
python3 -m pytest --fast --database postgres
```

## Performance

- **Full test suite**: ~18 minutes (all databases, all versions)
- **Fast mode** (`--fast`): ~3 minutes (validation only)
- **Single database/version**: ~2-5 minutes

The full suite now runs 18 cells, not 10. postgres, oracle and sqlserver run 3 versions each. The snowflake, fusion and core2 cells add more. Total runtime is higher than before.

## More Information

See [`automated_tests/README.md`](automated_tests/README.md) for comprehensive documentation including:

- Advanced usage & options
- Troubleshooting guide
- Development tips
- Architecture details

---

**Last Updated**: 2026-08-29
**Framework**: pytest + pytest-docker + Docker Compose
