# dbt_constraints Automated Testing

Comprehensive testing framework for `dbt_constraints` across multiple databases and dbt versions using pytest and Docker.

> **For technical architecture and implementation details**, see [ARCHITECTURE.md](./ARCHITECTURE.md)

## Quick Start

```bash
# Test PostgreSQL with dbt 1.9.0 (fastest)
pytest --database postgres --dbt-version 1.9.0

# Test all PostgreSQL versions
pytest --database postgres

# Test Snowflake (requires SNOWFLAKE_* env vars)
pytest --database snowflake --dbt-version 1.9.0

# Test all databases and versions
pytest

# Fast mode (quick validation with dbt debug only)
pytest --fast --database postgres --dbt-version 1.9.0
```

## How It Works

- **Database containers** start once per session (PostgreSQL, Oracle, SQL Server)
- **dbt runner containers** are created per test with specific versions
- **Tests** are automatically parameterized from `config/test-versions.json`
- **Credentials** are randomly generated per session for security

## Usage

### Run Specific Tests

```bash
# Single database + version
pytest --database postgres --dbt-version 1.9.0

# Single database, all versions
pytest --database postgres

# Specific test file
pytest tests/test_constraints.py --database postgres

# Specific test function
pytest tests/test_dbt_versions.py::test_dbt_workflow --database postgres --dbt-version 1.9.0

# Fast mode (minimal validation)
pytest --fast --database postgres
```

### Advanced Options

```bash
# Verbose output
pytest -v --log-cli-level=DEBUG --database postgres

# Use markers (postgres, oracle, sqlserver, snowflake, fast, slow)
pytest -m postgres

# Capture output
pytest --database postgres 2>&1 | tee test_output.log
```

## Configuration

### Snowflake (Cloud Database)

Set environment variables before testing:

```bash
export SNOWFLAKE_ACCOUNT="your-account"
export SNOWFLAKE_USER="your-username"
export SNOWFLAKE_PASSWORD="your-password"
export SNOWFLAKE_ROLE="your-role"
export SNOWFLAKE_DATABASE="your-database"
export SNOWFLAKE_WAREHOUSE="your-warehouse"
```

### Version Matrix

Edit `config/test-versions.json` to control which dbt versions are tested:

```json
{
  "dbt_versions": {
    "postgres": ["1.8.0", "1.9.0"],
    "oracle": ["1.8.0", "1.9.0"],
    "sqlserver": ["1.8.0", "1.9.0"],
    "snowflake": ["1.5.0", "1.8.0", "1.9.0", "1.10.0"]
  }
}
```

## Extending

- **Add a new test**: Create file in `tests/` using the `run_dbt` fixture
- **Add a dbt version**: Edit `config/test-versions.json`
- **Add a database or customize framework**: See [ARCHITECTURE.md](./ARCHITECTURE.md)

## Performance

**Full test suite**: ~18 minutes (all databases, all versions)
**Fast mode** (`--fast`): ~3 minutes (validation only)

## CI/CD Integration

### GitHub Actions

```yaml
name: Test dbt_constraints
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

Run databases in parallel for faster CI:

```yaml
strategy:
  matrix:
    database: [postgres, oracle, sqlserver]
    dbt-version: ['1.8.0', '1.9.0']
  max-parallel: 6
```

## Troubleshooting

### Tests hang at database startup

Database containers use healthchecks. Check logs:

```bash
docker logs dbt-test-db-postgres
docker logs dbt-test-db-oracle
docker logs dbt-test-db-sqlserver
```

### Container conflicts

Clean up all containers:

```bash
docker compose -f docker/compose/postgres-db.yml -p dbt-test-db down -v
docker compose -f docker/compose/oracle-db.yml -p dbt-test-db down -v
docker compose -f docker/compose/sqlserver-db.yml -p dbt-test-db down -v
```

### dbt version not building

Check Dockerfile build args:

```bash
cd ../..
docker build -f integration_tests/automated_tests/docker/build/Dockerfile \
  --build-arg DBT_ADAPTER=postgres \
  --build-arg DBT_VERSION=1.9.0 \
  -t test-dbt .
```

### Network errors

Verify shared network exists:

```bash
docker network ls | grep dbt-test-db-network
```

## Development

```bash
# Run single test
pytest tests/test_dbt_versions.py::test_dbt_workflow[postgres-1.9.0] -v

# Debug with pdb
pytest --pdb --database postgres

# Inspect database
docker exec -it dbt-test-db-postgres psql -U <random_user> -d <random_db>
```

## Support

For issues:

1. Check logs: `pytest -v --log-cli-level=DEBUG`
2. Verify Docker: `docker ps`, `docker logs <container>`
3. Check compose files in `docker/compose/`
4. Open issue with full test output

## References

- **pytest-docker**: <https://github.com/avast/pytest-docker>
- **dbt Documentation**: <https://docs.getdbt.com>
- **Docker Compose**: <https://docs.docker.com/compose/>

---

**Last Updated**: 2025-11-26
**Framework Version**: 1.0
