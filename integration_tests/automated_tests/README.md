# dbt_constraints Automated Testing

Comprehensive testing framework for `dbt_constraints` across multiple databases and dbt versions using pytest and Docker.

> **For technical architecture and implementation details**, see [ARCHITECTURE.md](./ARCHITECTURE.md)

## Quick Start

```bash
# Test PostgreSQL with dbt 1.11.14 (fastest)
pytest --database postgres --dbt-version 1.11.14

# Test all PostgreSQL versions
pytest --database postgres

# Test Snowflake (requires SNOWFLAKE_* env vars)
pytest --database snowflake --dbt-version 1.11.11

# Test all databases and versions
pytest

# Fast mode (quick validation with dbt debug only)
pytest --fast --database postgres --dbt-version 1.11.14
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
pytest --database postgres --dbt-version 1.11.14

# Single database, all versions
pytest --database postgres

# Specific test file
pytest tests/test_constraints.py --database postgres

# Specific test function
pytest tests/test_dbt_versions.py::test_dbt_workflow --database postgres --dbt-version 1.11.14

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
    "postgres": ["1.8.10", "1.9.11", "1.11.14"],
    "oracle": ["1.8.10", "1.9.11", "1.12.3"],
    "sqlserver": ["1.8.10", "1.9.11", "1.11.14"],
    "snowflake": ["1.5.12", "1.8.10", "1.9.4", "1.10.15", "1.11.11", "1.12.3"],
    "fusion": ["2.0.0rc186", "2.0.0rc212"],
    "core2": ["2.0.0b2"]
  }
}
```

Every version is an exact patch pin. The matrix has 18 cells. The config file explains the pinning policy. Read `config/test-versions.json` for the authoritative matrix.

## dbt v2 Engines

The matrix has two dbt v2 engines. They are different products. Do not confuse them.

- **fusion** (package `dbt`): dbt Fusion. It is the Rust engine. It adds SQL comprehension, LSP features and `dbt lint` on top of dbt Core 2.0.
- **core2** (package `dbt-core` 2.x): dbt Core 2.0. It is the Apache 2.0 licensed foundation behind Fusion. It does not include SQL comprehension, LSP or `dbt lint`.

Both are single self-contained wheels. Neither needs an adapter package or dbt-adapters. Both run against Snowflake. Both use the `integration_tests/dbt-fusion` project directory. Fusion runs through `docker/compose/fusion-runner.yml`. core2 runs through `docker/compose/core2-runner.yml`. Both build `docker/build/Dockerfile.v2`.

```bash
pytest --database fusion --dbt-version 2.0.0rc212
pytest --database core2 --dbt-version 2.0.0b2
```

## Snowflake Alignment

The snowflake cells align with the versions dbt Projects on Snowflake (DPOS) supports.

- 1.9.4, 1.10.15 and 1.11.11 are the exact patches Snowflake supports. DPOS pins exact patches, not minors.
- The authoritative check is: `SELECT SYSTEM$SUPPORTED_DBT_VERSIONS();`
- Reference doc: https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-dbt-core-versions
- 1.5.12 and 1.8.10 are the legacy floor. 1.12.3 is the newest dbt Labs release.
- Where a Snowflake-supported patch exists for a minor, use it instead of that minor's newest patch. Testing what Snowflake actually runs is the point.
- DPOS applies to the snowflake adapter only. postgres, oracle and sqlserver track dbt Labs releases. Adapter availability caps them: dbt-postgres tops out at 1.11.0 and dbt-sqlserver at 1.11.1. Neither can test 1.12.x. dbt-oracle reaches 1.12.0.

## Known Gaps

- Snowflake also supports dbt Fusion 2.0.0-preview.175. The PyPI `dbt` package rc series starts at rc178. preview.175 cannot be installed with uv, so it is not covered.
- 2.0.0rc212 is the newest `dbt` release on PyPI. It matches the dbt Labs `stable` channel, not `latest`. CDN latest (preview.218) is not on PyPI. Channel source: https://public.cdn.getdbt.com/fs/versions.json
- install.sh is gone. No `latest` or `stable` channel auto-tracking remains. Bump all pins manually.

> WARNING: Never run an unpinned `uv pip install dbt`. It resolves to version 1.0.0.40.21, the old dbt Cloud CLI. That tool is completely different and installs without error. Only `2.0.0rcNNN` releases of the `dbt` package are Fusion. Exact pins are mandatory.

## Version Pinning

All package installs use uv.

- The Dockerfile pins `dbt-core==<version>` exactly.
- The Dockerfile pins the adapter to the matching minor only. Adapter patch numbers are independent of core patch numbers.
- dbt-core 1.11.11 exists, but dbt-snowflake 1.11.11 does not. The newest dbt-snowflake 1.11.x is 1.11.6.
- Docker images run `uv pip install --system`. Local test deps run `uv pip install -r requirements-test.txt`.
- Fusion installs from PyPI, not from the install.sh shell script.
- Dockerfile.v2 serves all dbt v2 cells with two build args: `DBT_PACKAGE` and `DBT_VERSION`.

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
          uv pip install -r requirements-test.txt

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
    dbt-version: ['1.8.10', '1.9.11']
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
  --build-arg DBT_VERSION=1.11.14 \
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
pytest tests/test_dbt_versions.py::test_dbt_workflow[postgres-1.11.14] -v

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

**Last Updated**: 2026-08-29
**Framework Version**: 1.0
