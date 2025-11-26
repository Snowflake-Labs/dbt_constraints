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
pip install -r requirements-test.txt

# Test PostgreSQL with dbt 1.9.0 (fastest)
pytest --database postgres --dbt-version 1.9.0

# Test all databases and versions
pytest

# Fast mode (quick validation only)
pytest --fast --database postgres --dbt-version 1.9.0
```

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
EOF

# Test Snowflake
cd automated_tests
pytest --database snowflake --dbt-version 1.9.0
```

## Testing Issue #105

Issue #105: Foreign key creation didn't respect custom database/schema/alias properties.

```bash
cd integration_tests/automated_tests

# Test Issue #105 regression (all scenarios)
pytest tests/test_issue_105.py --database postgres --dbt-version 1.9.0 -v

# Quick regression check
pytest tests/test_issue_105.py::test_issue_105_regression --database postgres --dbt-version 1.9.0
```

## Database Support

| Database   | Status | Auth Method | Notes |
|------------|--------|-------------|-------|
| PostgreSQL | ✅     | Auto-generated | Fast, recommended for CI |
| Oracle     | ✅     | Auto-generated | Slow startup (~5 min) |
| SQL Server | ✅     | Auto-generated | Experimental |
| Snowflake  | ✅     | Private key (`.env`) | Cloud service, no container needed |

**Note**: Local databases (PostgreSQL, Oracle, SQL Server) use randomly generated credentials per test session.

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
          pip install -r requirements-test.txt

      - name: Run tests
        run: |
          cd integration_tests/automated_tests
          pytest --database ${{ matrix.database }}
```

### Parallel Testing

```yaml
strategy:
  matrix:
    database: [postgres, oracle, sqlserver]
    dbt-version: ['1.8.0', '1.9.0']
  max-parallel: 6
```

## Common Commands

```bash
cd integration_tests/automated_tests

# Single database, all versions
pytest --database postgres

# Single database, specific version
pytest --database postgres --dbt-version 1.9.0

# Specific test file
pytest tests/test_issue_105.py --database postgres

# Verbose output with logs
pytest -v --log-cli-level=DEBUG --database postgres

# Fast validation mode
pytest --fast --database postgres
```

## Performance

- **Full test suite**: ~18 minutes (all databases, all versions)
- **Fast mode** (`--fast`): ~3 minutes (validation only)
- **Single database/version**: ~2-5 minutes

## More Information

See [`automated_tests/README.md`](automated_tests/README.md) for comprehensive documentation including:

- Advanced usage & options
- Troubleshooting guide
- Development tips
- Architecture details

---

**Last Updated**: 2024-11-26
**Framework**: pytest + pytest-docker + Docker Compose
