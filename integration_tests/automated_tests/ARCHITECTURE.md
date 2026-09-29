# Testing Framework Architecture

> **For usage instructions, quick start, and configuration**, see [README.md](./README.md)

## Overview

A pytest framework for `dbt_constraints`. Database servers run as Docker containers,
managed by `pytest-docker`. The dbt client runs in one of two places, depending on the
matrix cell: a cached uv venv on the host, or inside Snowflake through dbt Projects on
Snowflake.

This document provides technical details about the framework's internal architecture,
design decisions, and implementation. `../AGENTS.md` is the shorter operating reference.

## Design Principles

1. **Separation of Concerns**: Database containers separate from the dbt client
2. **Efficiency**: Databases start once per session; each venv is cached between runs
3. **Security**: Randomized credentials prevent hardcoded secrets
4. **Flexibility**: Easy to add databases, versions, or tests
5. **Standards**: Uses pytest conventions and the pytest-docker plugin
6. **Organization**: Clear folder structure for maintainability

## Directory Structure

```
automated_tests/
├── config/
│   └── test-versions.json          # Version matrix, single source of truth
├── docker/
│   └── compose/
│       └── *-db.yml                # Database services
├── scripts/
│   └── run_dbt_tests.sh            # Host-only dbt workflow script
├── tests/
│   ├── test_dbt_versions.py        # Version matrix tests
│   ├── test_constraints.py         # Feature tests
│   ├── test_source_constraints.py  # Constraints on sources, catalog-verified
│   ├── test_fusion_compatibility.py
│   ├── test_issue_105.py           # Issue tests
│   └── test_issue_110.py
├── .dpos-stage/                    # Generated. Deployable copies, gitignored
├── conftest.py                     # Pytest fixtures and the runner dispatch
├── dbt_venv.py                     # Per-cell uv virtual environments
├── dpos_stage.py                   # Builds a deployable copy of a project
├── dpos_runner.py                  # Translates dbt commands into snow CLI calls
├── pytest.ini                      # Pytest config
└── requirements-test.txt           # Dependencies
```

The runner containers are gone. `docker/build/`, `Dockerfile`, `Dockerfile.v2` and
`*-runner.yml` no longer exist. `docker/` holds database services only.

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
│  │  Test: postgres-1.8.10                     │  │
│  │  ┌──────────────────────────┐              │  │
│  │  │ dbt-postgres:1.8.10 runne│──connects──▶ │  │
│  │  │ (ephemeral container)    │      PG      │  │
│  │  └──────────────────────────┘              │  │
│  │                                            │  │
│  │  Test: postgres-1.11.14                    │  │
│  │  ┌──────────────────────────┐              │  │
│  │  │ dbt-postgres:1.11.14 runn│──connects──▶ │  │
│  │  │ (ephemeral container)    │      PG      │  │
│  │  └──────────────────────────┘              │  │
│  │                                            │  │
│  │  Test: oracle-1.8.10                       │  │
│  │  ┌──────────────────────────┐              │  │
│  │  │ dbt-oracle:1.8.10 runner │──connects──▶ │  │
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
     --build-arg DBT_VERSION=1.11.14 \
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
│   │   ├── Dockerfile          # dbt 1.x runner image
│   │   └── Dockerfile.v2       # dbt v2 runner image
│   │
│   └── compose/                # Service definitions
│       ├── *-db.yml            # Database containers
│       ├── fusion-runner.yml   # Fusion runner (Dockerfile.v2)
│       ├── core2-runner.yml    # Core 2 runner (Dockerfile.v2)
│       └── *-runner.yml        # dbt 1.x runners
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
| `dbt_version` | Current dbt version | `"1.11.14"` (param) |
| `runner_project_name` | Unique runner name | `"dbt-test-postgres-190"` |
| `runner_compose_file` | Runner compose file | `"docker/compose/postgres-runner.yml"` |
| `dbt_env` | Environment vars | `{"DBT_TARGET": "postgres", ...}` |
| `run_dbt` | Execute dbt commands | `function(command) → result` |

## Parameterization Flow

```python
# 1. pytest_generate_tests hook reads config
{
  "dbt_versions": {
    "postgres": ["1.8.10", "1.9.11"],
    "oracle": ["1.8.10"]
  }
}

# 2. Creates parameter combinations
[
  ("postgres", "1.8.10"),
  ("postgres", "1.9.11"),
  ("oracle", "1.8.10")
]

# 3. pytest.mark.parametrize injects into fixtures
def test_dbt_workflow(database, dbt_version, run_dbt):
    # database="postgres", dbt_version="1.8.10"
    # database="postgres", dbt_version="1.9.11"
    # database="oracle", dbt_version="1.8.10"
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
test_dbt_workflow[postgres-1.8.10]
├── build: dbt-postgres:1.8.10
├── run: dbt clean, deps, seed, build
└── down: cleanup container

test_dbt_workflow[postgres-1.9.11]
├── build: dbt-postgres:1.9.11 (different image)
├── run: dbt clean, deps, seed, build
└── down: cleanup container

Database persists throughout ✓
```

## dbt v2 Engines

The matrix has two dbt v2 engines. They are different products. Do not confuse them.

- **fusion** (package `dbt`): dbt Fusion. It is the Rust engine. It adds SQL comprehension, LSP features and `dbt lint` on top of dbt Core 2.0.
- **core2** (package `dbt-core` 2.x): dbt Core 2.0. It is the Apache 2.0 licensed foundation behind Fusion. It does not include SQL comprehension, LSP or `dbt lint`.

Both are single self-contained wheels. Neither needs an adapter package or dbt-adapters. Both run against Snowflake. Both parse the `integration_tests/dbt-fusion` project directory. Each installs into its own uv venv; see `dbt_venv.py`.

conftest.py defines module constants that replace scattered `== "fusion"` comparisons:

```python
V2_ENGINE_TARGETS = ("fusion", "core2")
DPOS_DATABASES = ("dpos_core", "dpos_fusion")
FUSION_PROJECT_TARGETS = (*V2_ENGINE_TARGETS, "dpos_fusion")
CLOUD_DATABASES = ("snowflake", *V2_ENGINE_TARGETS, *DPOS_DATABASES)
```

## In-database cells

`dpos_core` and `dpos_fusion` run dbt inside Snowflake. `conftest.run_dbt` dispatches on
the database key: a host cell shells out to the venv's `dbt`, and an in-database cell
shells out to `snow dbt execute`. Both return a `subprocess.CompletedProcess`, so the
assertions are identical.

`dpos_stage.py` exists because Snowflake rejects `- local: ../../`. It copies the project
and the repository root into `.dpos-stage/<cell>/`, rewrites `packages.yml`, and runs
`dbt deps` so `dbt_packages/` ships inside the object. A staged `dbt_packages/` also
removes the need for an external access integration.

`dpos_runner.py` owns the command translation and the platform limits: unsupported
commands (`clean`, `debug`), unsupported flags, and the session-context flags. See
`../AGENTS.md` for the full rule list.

## Snowflake Alignment

The snowflake and dpos_* cells align with the versions dbt Projects on Snowflake (DPOS)
supports.

- The authoritative check is: `SELECT SYSTEM$SUPPORTED_DBT_VERSIONS();`
- As of 2026-08-29 it returns dbt Core 1.9.4, 1.10.15, 1.11.11 and dbt Fusion
  2.0.0-preview, 2.0.0-preview.175, 2.0.0-preview.186. DPOS pins exact patches.
- 1.11.11 and 2.0.0-preview.186 now run IN Snowflake, as the `dpos_core` and
  `dpos_fusion` cells. Running them on the host proved only that the version worked
  locally.
- The host keeps `snowflake` 1.5.12 as the backward-compatibility floor, `fusion`
  2.0.0rc212 as the newest PyPI release, and `core2` 2.0.0b2. dbt Core 2.0 betas are not
  a DPOS engine, so `core2` cannot move.
- Reference doc: https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-dbt-core-versions
- DPOS applies to the snowflake adapter only. postgres, oracle and sqlserver track dbt Labs releases. Adapter availability caps them: dbt-postgres tops out at 1.11.0 and dbt-sqlserver at 1.11.1. Neither can test 1.12.x. dbt-oracle reaches 1.12.0.

## Known Gaps

- Snowflake also supports dbt Fusion 2.0.0-preview.175. The PyPI `dbt` package rc series starts at rc178. preview.175 cannot be installed with uv, so it is not covered.
- 2.0.0rc212 is the newest `dbt` release on PyPI. It matches the dbt Labs `stable` channel, not `latest`. CDN latest (preview.218) is not on PyPI. Channel source: https://public.cdn.getdbt.com/fs/versions.json
- install.sh is gone. No `latest` or `stable` channel auto-tracking remains. Bump all pins manually.
- An unpinned `uv pip install dbt` resolves to version 1.0.0.40.21, the old dbt Cloud CLI. It installs without error. Only `2.0.0rcNNN` releases of the `dbt` package are Fusion. Exact pins are mandatory.
- The in-database cells stage their dependencies with the matching PyPI engine, then run
  in Snowflake. `dpos_fusion` stages with `dbt==2.0.0rc186` and runs on
  `2.0.0-preview.186`. The mapping is exact today. If the two ever resolve dependencies
  differently, attach an external access integration and let Snowflake run `dbt deps`.
- Most assertions read dbt stdout, not the catalog. `test_source_constraints.py` is the
  one module that verifies constraints in `INFORMATION_SCHEMA`, through a dbt singular
  test that runs in the warehouse.

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
       "newdb": ["1.11.14"]
     }
   }
   ```

### New dbt Version

Just update `config/test-versions.json`:

```json
{
  "dbt_versions": {
    "postgres": ["1.8.10", "1.9.11", "1.11.14"]
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

## Client Install Optimization

Each matrix cell installs its dbt client into its own uv venv under `<repo>/.venvs/`.
`dbt_venv.ensure_venv` writes a marker file recording the install spec and the Python
version. A later run reuses the venv when the marker matches, so it pays no install cost.

```python
# One venv per cell. Core pinned exactly; the adapter pinned to the matching MINOR,
# because adapter patch numbers are independent of core patch numbers.
["dbt-core==1.11.14", "dbt-postgres~=1.11.0"]
```

**Why a venv and not a runner container:**

- A clean venv per cell cannot read a shadowed distribution. A shared Docker base layer
  breaks dbt-core 1.5.x, because a newer `snowplow_tracker` module replaces the module
  its pin needs while the metadata still reports the pin satisfied.
- `target/` and the logs are readable on the host.
- There is no `host.docker.internal` hop. The database containers publish host ports.

**Cost:** `msodbcsql18` becomes a host prerequisite. `dbt_venv.sqlserver_driver_present`
skips the sqlserver cells with a clear message when it is absent.

The in-database cells build a venv only to run `dbt deps` while staging. Snowflake
supplies the engine for the run itself.

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

| Database   | Per Version | All Versions (3) |
|-----------|-------------|------------------|
| PostgreSQL | ~2 min     | ~6 min           |
| SQL Server | ~3 min     | ~9 min           |
| Oracle     | ~4 min     | ~12 min          |

**Total:** The suite is now 18 cells, not 10. postgres, oracle and sqlserver run 3 versions each. snowflake runs 6. fusion runs 2. core2 runs 1. The previously measured per-version figures still apply per cell. Total runtime is higher than the previous 18-minute figure.

**Fast Mode** (`--fast` flag):

| Database   | Time    | What it tests |
|-----------|---------|---------------|
| PostgreSQL | ~30 sec | `dbt debug` only |
| SQL Server | ~45 sec | `dbt debug` only |
| Oracle     | ~90 sec | `dbt debug` only |

**Total: ~3 minutes** (validation only)

### Why One Venv Per Cell?

- **Isolation**: no distribution shadowing between dbt versions
- **Version-specific**: the install spec pins core exactly and the adapter to its minor
- **Cached**: the venv survives between runs, so a repeat run installs nothing
- **Host-readable**: `target/` and the logs need no container hop

## Framework Features

| Feature | Implementation |
|---------|----------------|
| **Orchestration** | pytest-docker plugin |
| **Database Lifecycle** | Session-scoped (shared) |
| **Host dbt Client** | One cached uv venv per cell |
| **In-database dbt Client** | Snowflake, via `snow dbt execute` |
| **Test Discovery** | Automatic via pytest |
| **Fast Mode** | `--fast` flag |
| **Security** | Randomized credentials for the local databases |

**Parallel execution:** the local database cells tolerate `pytest-xdist`. The Snowflake
cells do not, and never did. All host Snowflake cells share one schema
(`dbt_constraints_test`) with no run-scoped suffix, and Snowflake permits only one
concurrent `EXECUTE DBT PROJECT` per object. The in-database cells each hold their own
object and their own schema, so `dpos_core` and `dpos_fusion` can run at the same time as
each other and as the host cells.

## Benefits

1. **Standards-Based**: Uses pytest idioms and the pytest-docker plugin
2. **Efficient**: Databases start once; venvs are cached between runs
3. **Secure**: Randomized credentials for every local database session
4. **Faithful**: The versions Snowflake runs are tested inside Snowflake
5. **Organized**: Clear folder structure
6. **Maintainable**: Easy to understand and modify
7. **Extensible**: Simple to add databases/versions/tests
8. **Fast**: Session-scoped DBs save 6+ minutes per run
9. **Flexible**: CLI filters and markers
10. **Documented**: `../AGENTS.md` for operating rules, this file for internals

---

**Last Updated**: 2026-08-29
**Framework Version**: 1.0 (Initial Release)
