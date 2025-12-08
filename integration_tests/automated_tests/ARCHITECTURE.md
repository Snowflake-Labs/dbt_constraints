# Testing Framework Architecture

> **For usage instructions, quick start, and configuration**, see [README.md](./README.md)

## Overview

Modern pytest-based testing framework for `dbt_constraints` using Docker containers managed by `pytest-docker`.

This document provides technical details about the framework's internal architecture, design decisions, and implementation.

## Design Principles

1. **Separation of Concerns**: Database containers separate from dbt runners
2. **Efficiency**: Databases start once (session), runners per test (function)
3. **Security**: Randomized credentials prevent hardcoded secrets
4. **Flexibility**: Easy to add databases, versions, or tests
5. **Standards**: Uses pytest conventions and pytest-docker plugin
6. **Organization**: Clear folder structure for maintainability

## Directory Structure

```
automated_tests/
├── config/
│   └── test-versions.json          # Version matrix
├── docker/
│   ├── build/
│   │   ├── Dockerfile              # Multi-stage dbt runner
│   │   └── run_dbt_tests.sh        # dbt workflow script
│   └── compose/
│       ├── *-db.yml                # Database services
│       └── *-runner.yml            # dbt runner services
├── tests/
│   ├── test_dbt_versions.py        # Version matrix tests
│   ├── test_constraints.py         # Feature tests
│   └── test_issue_105.py           # Issue tests
├── conftest.py                     # Pytest fixtures
├── pytest.ini                      # Pytest config
└── requirements-test.txt           # Dependencies
```

## Architecture Diagram

```
┌──────────────────────────────────────────────────┐
│                   Pytest Session                 │
│                                                  │
│  ┌────────────────────────────────────────────┐  │
│  │  Session Fixtures (start_databases)        │  │
│  │                                            │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  │  │
│  │  │PostgreSQL│  │  Oracle  │  │SQLServer │  │  │
│  │  │Container │  │Container │  │Container │  │  │
│  │  │(healthy) │  │(healthy) │  │(healthy) │  │  │
│  │  └──────────┘  └──────────┘  └──────────┘  │  │
│  │       ▲              ▲              ▲      │  │
│  │       │              │              │      │  │
│  │   Shared Network: dbt-test-db-network      │  │
│  └────────────────────────────────────────────┘  │
│                                                  │
│  ┌────────────────────────────────────────────┐  │
│  │  Function Fixtures (per test)              │  │
│  │                                            │  │
│  │  Test: postgres-1.8.0                      │  │
│  │  ┌──────────────────────────┐              │  │
│  │  │ dbt-postgres:1.8.0 runner│──connects──▶ │  │
│  │  │ (ephemeral container)    │      PG      │  │
│  │  └──────────────────────────┘              │  │
│  │                                            │  │
│  │  Test: postgres-1.9.0                      │  │
│  │  ┌──────────────────────────┐              │  │
│  │  │ dbt-postgres:1.9.0 runner│──connects──▶ │  │
│  │  │ (ephemeral container)    │      PG      │  │
│  │  └──────────────────────────┘              │  │
│  │                                            │  │
│  │  Test: oracle-1.8.0                        │  │
│  │  ┌──────────────────────────┐              │  │
│  │  │ dbt-oracle:1.8.0 runner  │──connects──▶ │  │
│  │  │ (ephemeral container)    │      ORA     │  │
│  │  └──────────────────────────┘              │  │
│  └────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────┘
```

## Component Lifecycle

### Session Start

1. **pytest collection** reads `config/test-versions.json`
2. **Test parameterization** creates test matrix (database × version)
3. **db_connection_config fixture** (session scope):
   - Generates cryptographically secure random credentials:
     - PostgreSQL: random user, database, password (24 chars)
     - Oracle: random user, password (24 chars)
     - SQL Server: random password for `sa` user (24 chars)
   - Sets credentials as environment variables
   - Returns credentials dict for reference
4. **start_databases fixture** (session scope):
   - Reads database filter from CLI (`--database`)
   - For each database:
     - `docker compose up -d` using `docker/compose/*-db.yml` with random credentials
     - Waits for healthcheck to pass (60s for PostgreSQL, 120s for Oracle/SQL Server)
     - Shares network: `dbt-test-db-network`
   - Returns list of started databases

### Per Test

1. **Build dbt runner** with specific version:

   ```bash
   docker compose build \
     --build-arg DBT_VERSION=1.9.0 \
     -f docker/compose/postgres-runner.yml
   ```

2. **run_dbt fixture** provides function to execute commands:

   ```python
   result = run_dbt("dbt build --full-refresh")
   ```

3. **Test execution**:
   - Runner connects to database via shared network
   - Executes dbt workflow
   - Asserts on results

4. **Cleanup**:
   - `docker compose down -v` for runner
   - Database container persists

### Session End

1. **Teardown databases**:
   - For each started database:
     - `docker compose down -v`
     - Network cleanup

## File Organization

```
automated_tests/
├── config/                     # Configuration
│   └── test-versions.json      # Version matrix
│
├── docker/                     # Docker resources
│   ├── build/                  # Container images
│   │   └── Dockerfile          # dbt runner image
│   │
│   └── compose/                # Service definitions
│       ├── *-db.yml            # Database containers
│       └── *-runner.yml        # dbt runners
│
├── tests/                      # Test files
│   ├── test_dbt_versions.py    # Matrix tests
│   ├── test_constraints.py     # Feature tests
│   └── test_issue_105.py       # Issue-specific tests
│
├── conftest.py                 # Pytest fixtures & config
├── pytest.ini                  # Pytest settings
├── requirements-test.txt       # Python dependencies
└── README.md                   # Usage documentation
```

## Pytest Configuration

### Settings (`pytest.ini`)

```ini
[pytest]
testpaths = tests
timeout = 600                    # 10 minutes per test
log_cli_level = INFO
python_files = test_*.py
python_classes = Test*
python_functions = test_*
```

### Markers

- `postgres` - PostgreSQL-specific tests
- `oracle` - Oracle-specific tests
- `sqlserver` - SQL Server-specific tests
- `snowflake` - Snowflake-specific tests
- `fast` - Quick validation tests
- `slow` - Long-running tests

### CLI Arguments

- `--database <name>` - Filter by database
- `--dbt-version <version>` - Filter by dbt version
- `--fast` - Run minimal validation only

## Key Fixtures

### Session Scope

| Fixture | Purpose | Returns |
|---------|---------|---------|
| `docker_compose_command` | Docker CLI command | `"docker compose"` |
| `database_project_name` | Shared DB project name | `"dbt-test-db"` |
| `database_compose_files` | Map DB→compose file | `{"postgres": ["..."]}` |
| `db_connection_config` | Generate random creds | `{"POSTGRES_USER": "...", ...}` |
| `start_databases` | Start & wait for DBs | `["postgres", "oracle"]` |

### Function Scope

| Fixture | Purpose | Returns |
|---------|---------|---------|
| `database` | Current DB under test | `"postgres"` (param) |
| `dbt_version` | Current dbt version | `"1.9.0"` (param) |
| `runner_project_name` | Unique runner name | `"dbt-test-postgres-190"` |
| `runner_compose_file` | Runner compose file | `"docker/compose/postgres-runner.yml"` |
| `dbt_env` | Environment vars | `{"DBT_TARGET": "postgres", ...}` |
| `run_dbt` | Execute dbt commands | `function(command) → result` |

## Parameterization Flow

```python
# 1. pytest_generate_tests hook reads config
{
  "dbt_versions": {
    "postgres": ["1.8.0", "1.9.0"],
    "oracle": ["1.8.0"]
  }
}

# 2. Creates parameter combinations
[
  ("postgres", "1.8.0"),
  ("postgres", "1.9.0"),
  ("oracle", "1.8.0")
]

# 3. pytest.mark.parametrize injects into fixtures
def test_dbt_workflow(database, dbt_version, run_dbt):
    # database="postgres", dbt_version="1.8.0"
    # database="postgres", dbt_version="1.9.0"
    # database="oracle", dbt_version="1.8.0"
```

## Network Architecture

```
┌──────────────────────────────────────────────┐
│  Docker Network: dbt-test-db-network         │
│                                              │
│  ┌──────────────┐                            │
│  │  postgres    │  Container:                │
│  │  :5432       │  dbt-test-db-postgres      │
│  └──────────────┘                            │
│         ▲                                    │
│         │ connects via                       │
│         │ hostname: dbt-test-db-postgres     │
│         │                                    │
│  ┌──────────────┐                            │
│  │ dbt-postgres │  Project:                  │
│  │ runner       │  dbt-test-postgres-190     │
│  └──────────────┘  (ephemeral)               │
│                                              │
│  Environment: (randomized per session)       │
│    POSTGRES_HOST=dbt-test-db-postgres        │
│    POSTGRES_PORT=5432                        │
│    POSTGRES_USER=pg_user_9edb2671 (random)   │
│    POSTGRES_PASSWORD=Xk9#mL2$... (random)    │
│    ...                                       │
└──────────────────────────────────────────────┘
```

## Version Isolation

Each test gets its own runner container:

```
test_dbt_workflow[postgres-1.8.0]
├── build: dbt-postgres:1.8.0
├── run: dbt clean, deps, seed, build
└── down: cleanup container

test_dbt_workflow[postgres-1.9.0]
├── build: dbt-postgres:1.9.0 (different image)
├── run: dbt clean, deps, seed, build
└── down: cleanup container

Database persists throughout ✓
```

## Adding New Components

### New Database

1. **Create compose files**:

   ```yaml
   # docker/compose/newdb-db.yml
   services:
     newdb:
       image: newdb:latest
       healthcheck: ...

   # docker/compose/newdb-runner.yml
   services:
     dbt-newdb:
       build:
         args:
           DBT_ADAPTER: newdb
   ```

2. **Update conftest.py**:

   ```python
   def database_compose_files(request):
       return {
           ...
           "newdb": [str(COMPOSE_DIR / "newdb-db.yml")],
       }
   ```

3. **Update config**:

   ```json
   {
     "dbt_versions": {
       "newdb": ["1.9.0"]
     }
   }
   ```

### New dbt Version

Just update `config/test-versions.json`:

```json
{
  "dbt_versions": {
    "postgres": ["1.8.0", "1.9.0", "1.10.0"]
  }
}
```

### New Test

Create in `tests/` directory:

```python
def test_my_feature(database, dbt_version, run_dbt):
    """Test automatically runs for all DB×version combinations."""
    result = run_dbt("dbt run --select my_model")
    assert result.returncode == 0
```

## Security Features

### Randomized Credentials

Every test session generates unique, cryptographically secure credentials:

```python
# Generated per session
POSTGRES_USER="pg_user_9edb2671"      # random 8-char suffix
POSTGRES_DB="pg_db_fcd2f26e"          # random 8-char suffix
POSTGRES_PASSWORD="Xk9#mL2$pQ..."     # random 24-char password

ORACLE_USER="ora_user_7675a547"       # random 8-char suffix
ORACLE_PASSWORD="Ry5&nW8@vT..."       # random 24-char password

SQLSERVER_PASSWORD="Pk3#mV7$zX..."    # random 24-char password (sa user)
```

**Benefits:**

- ✅ No hardcoded credentials in code or configs
- ✅ Each test run is isolated with unique credentials
- ✅ Prevents credential leakage between test runs
- ✅ Enhances security for CI/CD environments
- ✅ Reduces risk of credential conflicts

**Implementation:**

- `generate_secure_password()`: Uses `secrets` module for cryptographic randomness
- `generate_db_identifier()`: Creates unique identifiers with random suffixes
- `db_connection_config` fixture: Session-scoped, runs once per test session

### Database Health Checks

Each database has optimized health checks with environment variable support:

**PostgreSQL:**

```yaml
healthcheck:
  test: ["CMD-SHELL", "pg_isready -U $$POSTGRES_USER"]
  interval: 5s, timeout: 5s, retries: 12, start_period: 10s
```

**Oracle:**

```yaml
healthcheck:
  test: ["CMD", "/opt/oracle/checkDBStatus.sh"]
  interval: 10s, timeout: 5s, retries: 30, start_period: 60s
```

**SQL Server:**

```yaml
healthcheck:
  test: ["CMD-SHELL", "/opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P \"$$SA_PASSWORD\" -Q 'SELECT 1'"]
  interval: 10s, timeout: 5s, retries: 20, start_period: 60s
```

**Note:** SQL Server uses `CMD-SHELL` to enable environment variable expansion (`$$SA_PASSWORD`).

## Docker Build Optimization

### Multi-Stage Dockerfile

The dbt runner uses a **multi-stage build** for faster iteration:

```dockerfile
# Stage 1: Base image with common dependencies (cached)
FROM python:3.11 as base
RUN apt-get update && apt-get upgrade -y
RUN apt-get install git build-essential libpq-dev unixodbc-dev
RUN install ODBC Driver 18 for SQL Server
# ... (rarely changes, cached)

# Stage 2: Final image with version-specific dbt (rebuilt per version)
FROM base as final
ARG DBT_ADAPTER=postgres
ARG DBT_VERSION=1.9.0
RUN pip install dbt-core==${DBT_VERSION} dbt-${DBT_ADAPTER}==${DBT_VERSION}
# ... (changes per test, but base is cached)
```

**Benefits:**

- 🚀 Base stage cached across all dbt versions
- 🚀 Only final stage rebuilds when changing versions
- 🚀 Significantly faster builds (seconds vs minutes)
- 🚀 Reduced Docker layer downloads

## Testing Philosophy

### What We Test

✅ **Core dbt Workflow**

- `dbt clean`, `dbt deps`, `dbt seed`, `dbt build`
- Full refresh and incremental modes
- Cross-version compatibility

✅ **Constraint Creation**

- Primary keys, foreign keys, unique, not null
- Check constraints (where supported)
- Custom naming macros (issue #105)

✅ **Cross-Database Compatibility**

- Same dbt project works on all adapters
- Constraints created correctly per platform

### What We Don't Test

❌ Database-specific features outside dbt_constraints
❌ dbt core functionality (trust dbt's own tests)
❌ Adapter-specific bugs (report to adapter repos)

## Performance Optimization

### Session vs Function Scope

**Without session-scoped databases:**

- Start PostgreSQL: 15s × 6 tests = 90s
- Start Oracle: 60s × 6 tests = 360s
- **Total: 450s overhead**

**With session-scoped databases:**

- Start PostgreSQL: 15s × 1 = 15s
- Start Oracle: 60s × 1 = 60s
- **Total: 75s overhead**

**Savings: 375 seconds (6.25 minutes)**

### Performance Metrics

**Full Test Suite:**

| Database   | Per Version | All Versions (2) |
|-----------|-------------|------------------|
| PostgreSQL | ~2 min     | ~4 min           |
| SQL Server | ~3 min     | ~6 min           |
| Oracle     | ~4 min     | ~8 min           |

**Total: ~18 minutes** (all databases, all versions)

**Fast Mode** (`--fast` flag):

| Database   | Time    | What it tests |
|-----------|---------|---------------|
| PostgreSQL | ~30 sec | `dbt debug` only |
| SQL Server | ~45 sec | `dbt debug` only |
| Oracle     | ~90 sec | `dbt debug` only |

**Total: ~3 minutes** (validation only)

### Why Function-Scoped Runners?

- **Isolation**: Each test gets clean dbt environment
- **Version-specific**: Build args inject correct dbt version
- **Fast cleanup**: No state leakage between tests
- **Parallel-ready**: Can run tests in parallel with xdist

## Framework Features

| Feature | Implementation |
|---------|----------------|
| **Orchestration** | pytest-docker plugin |
| **Database Lifecycle** | Session-scoped (shared) |
| **Runner Lifecycle** | Function-scoped (isolated) |
| **Test Discovery** | Automatic via pytest |
| **Parallel Support** | pytest-xdist compatible |
| **Fast Mode** | `--fast` flag |
| **Security** | Randomized credentials |
| **Build Optimization** | Multi-stage Docker |

## Benefits

1. **Standards-Based**: Uses pytest idioms, pytest-docker plugin
2. **Efficient**: Databases start once, runners are ephemeral
3. **Secure**: Randomized credentials for every test session
4. **Optimized**: Multi-stage Docker builds cache common dependencies
5. **Organized**: Clear folder structure
6. **Maintainable**: Easy to understand and modify
7. **Extensible**: Simple to add databases/versions/tests
8. **Fast**: Session-scoped DBs save 6+ minutes per run
9. **Flexible**: CLI filters, markers, parallel execution
10. **Documented**: Separate user and developer documentation

---

**Last Updated**: 2025-11-26
**Framework Version**: 1.0 (Initial Release)
