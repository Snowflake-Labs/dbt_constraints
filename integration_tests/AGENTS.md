# AGENTS.md — integration_tests

This directory holds the test projects and the automated test harness for the
`dbt_constraints` dbt package. Read this file before you edit or run the tests.

## Layout

```
integration_tests/
├── dbt-core/                 # dbt Core reference project
│   ├── data/tpc_h_seeds/     # TPC-H seed CSVs
│   ├── macros/               # clone_table + issue_105 custom naming macros
│   ├── models/               # dim_*, fact_* models, sources.yml, schema.yml
│   ├── tests/                # singlular_test.sql, assert_source_constraints.sql
│   ├── packages.yml          # dbt_utils + local dbt_constraints
│   ├── profiles.yml          # local dbt reads this
│   ├── dbt_projects_profiles.yml  # dbt Projects on Snowflake reads this instead
│   └── env.yml               # supplies the values that file reads
├── dbt-fusion/               # dbt Fusion project (also used by the core2 target)
│   └── (mirrors dbt-core layout)
└── automated_tests/          # pytest harness
    ├── conftest.py           # fixtures, matrix parametrization, dbt runner
    ├── dbt_venv.py           # per-cell uv virtual environments
    ├── dpos_stage.py         # builds a deployable copy for the in-database cells
    ├── dpos_runner.py        # translates dbt commands into snow CLI calls
    ├── config/test-versions.json  # dbt version matrix (single source of truth)
    ├── docker/compose/       # DB server containers (postgres/oracle/sqlserver)
    ├── scripts/run_dbt_tests.sh   # full seed + build workflow
    ├── tests/                # parameterized test modules
    └── pytest.ini
```

## Two projects, many targets

- `dbt-core/` runs on the adapter databases: `postgres`, `oracle`, `snowflake`, and on the in-database `dpos_core` cell.
- `dbt-fusion/` runs on the dbt v2 engine targets: `fusion` and `core2`, and on
  the in-database `dpos_fusion` cell. Those targets parse this project. Keep the
  two source trees in sync when you edit models, sources, macros, or schema YAML.
  `dbt-fusion` wraps test arguments in an `arguments:` key; `dbt-core` does not.

## Two execution modes

A cell runs dbt in one of two places. `conftest.py` dispatches on the database key.

| Mode | Keys | Where dbt runs |
|---|---|---|
| Host | `postgres` `oracle` `snowflake` `fusion` `core2` | a cached uv venv on this machine |
| In-database | `dpos_core` `dpos_fusion` | inside Snowflake, through dbt Projects on Snowflake |

Both modes return a `subprocess.CompletedProcess`, because the in-database mode
shells out to the `snow` CLI. The assertions in the tests are identical.

## Harness architecture (current)

- Database servers run as Docker containers, one per adapter, at session scope.
  Compose files live in `docker/compose/`.
- The dbt client runs on the host, not in a container. Each matrix cell
  (database + dbt version) gets its own cached uv venv, created and reused by
  `dbt_venv.py`. This avoids distribution shadowing from a shared base image.
- `config/test-versions.json` is the single source of truth for the matrix.
  `conftest.py` reads `dbt_versions` there to parametrize tests.
- The `adapters` block in that file is descriptive only. Test code ignores it.
- Legacy files `.dockerenv/`, `docker-compose.test-runner.yml`, and
  `requirements-test.txt` runner build steps are not part of the active path.

## In-database cells (dbt Projects on Snowflake)

The `dpos_core` and `dpos_fusion` cells deploy the project to Snowflake and run every
command there. Snowflake supplies the engine, so the host installs no dbt client for the
run. It still builds one venv per cell to run `dbt deps` while staging.

Each test session:

1. `dpos_stage.py` copies the project to `automated_tests/.dpos-stage/<cell>/`, copies
   the repository root to `<stage>/local_packages/dbt_constraints/`, rewrites
   `packages.yml`, runs `dbt deps`, and prunes each installed package's own test suite.
2. `dpos_runner.py` runs `snow dbt deploy`, which adds a new `VERSION$N`.
3. Each test command becomes `snow dbt execute ... --dbt-version <version>`.

Rules that follow from the platform, not from choice:

- **`- local: ../../` cannot be deployed.** Snowflake rejects a package path that leaves
  the project root. The staging copy exists only for this.
- **A deployed object is immutable.** `dbt clean` and `dbt debug` are unsupported, and
  `dbt deps` against the object is a no-op. `dpos_runner.py` raises
  `UnsupportedCommand`, and `run_dbt` turns that into a skip with the reason.
- **`run_dbt_tests.sh` cannot run.** No host shell takes part. `test_dbt_workflow`
  issues the same four steps one at a time for these cells.
- **One concurrent execution per object.** Each cell therefore has its own object,
  `DBT_CONSTRAINTS_DPOS_CORE` and `DBT_CONSTRAINTS_DPOS_FUSION`.
- **These flags are unsupported:** `--state --target-path --log-path --profiles-dir
  --project-dir --log-format --log-format-file`. `dpos_runner.py` strips them.
- **Version strings use the Snowflake form.** Snowflake reports
  `2.0.0-preview.186` where PyPI publishes `2.0.0rc186`. `dbt_venv.dpos_version_to_pypi`
  converts one to the other for the staging venv. Check what an account supports with
  `SELECT SYSTEM$SUPPORTED_DBT_VERSIONS();`.

### Profiles and environment

`dbt Projects on Snowflake` reads `dbt_projects_profiles.yml` and ignores `profiles.yml`
when both are present. Standard dbt reads only `profiles.yml` and never looks at the
other name. That is why the two coexist: the host cells are unaffected by anything in
`dbt_projects_profiles.yml`.

`env.yml` supplies every value that file reads. Each project's `env.yml` holds one
environment, named after its cell, and gives that cell its own schema:

| Cell | Schema |
|---|---|
| host cells | `dbt_constraints_test` |
| `dpos_core` | `dbt_constraints_dpos_core` |
| `dpos_fusion` | `dbt_constraints_dpos_fusion` |

**Never rely on the connections.toml defaults for the session context.** `env.yml`
resolves the run target with `CURRENT_DATABASE()`, `CURRENT_ROLE()` and
`CURRENT_WAREHOUSE()`. A connections.toml entry can name a different database and carry
no role, so a run would build the models in the wrong database and still report success.
`dpos_runner._connection_flags` passes `--database --schema --role --warehouse`
explicitly on every call, taken from the `SNOWFLAKE_*` values in `.env`.

`profiles.yml` is deliberately excluded from the stage. It calls `env_var()` with no
default for the account, role, database and warehouse, so a run that read it would fail.

## How to run

Run pytest from `automated_tests/`, with `python3 -m pytest`:

```
cd integration_tests/automated_tests
uv pip install -r requirements-test.txt   # once
python3 -m pytest --database postgres --dbt-version 1.11.14
python3 -m pytest                                    # all databases + versions
python3 -m pytest --fast --database postgres         # validation only (dbt debug)
python3 -m pytest --database snowflake --dbt-version 1.5.12   # needs SNOWFLAKE_* env
python3 -m pytest --database dpos_core               # runs inside Snowflake
python3 -m pytest --database dpos_fusion             # runs inside Snowflake
```

Use `python3 -m pytest`, not a bare `pytest`. `automated_tests/__init__.py` makes this
directory a package, so pytest puts its PARENT on `sys.path` and `import dbt_venv`
fails. `python3 -m` puts the working directory on `sys.path` and the import resolves.

Selectors:

- `--database {postgres,oracle,snowflake,fusion,core2,dpos_core,dpos_fusion}`
- `--dbt-version <exact pin>`
- `-m {postgres,oracle,snowflake,fast,slow}`

Cloud targets (`snowflake`, `fusion`, `core2`) start no container and need
Snowflake credentials.

sqlserver is deliberately absent from the matrix. The package implements bigquery,
oracle, postgres, redshift, snowflake and vertica, with no sqlserver__ and no default__,
so its on-run-end hook fails at macro dispatch. Add a cell back only after the package
supports SQL Server.

The in-database targets (`dpos_core`, `dpos_fusion`) additionally need
`SNOWFLAKE_CONNECTION_NAME` in `.env`, naming a `connections.toml` entry for the same
account that the `SNOWFLAKE_*` values describe. There is no default: one would deploy to
whichever account `connections.toml` marks as `default`. The cells skip with an
explanation when it is absent.

**Check the skip count on any new cell.** A cell whose fixture guard fires reports
SKIPPED, not failed, so a run of nothing but skips reads as a pass. Compare collected
against skipped.

## Coding standards

### Package and environment management

- Use `uv` for all package operations. Never use `pip` directly.
- Pin dbt Core to an exact version. Pin the adapter to the matching minor
  (for example `dbt-core~=1.11.11` does not exist; use `dbt-core==<core>` and
  `dbt-snowflake~=1.11.0`). See `dbt_venv.py`.

### Version pins

- Every version in `test-versions.json` is an exact patch pin. Pins are bumped
  manually. Authoritative sources:
  - Snowflake supported versions: `SELECT SYSTEM$SUPPORTED_DBT_VERSIONS();`
  - dbt Fusion channels: https://public.cdn.getdbt.com/fs/versions.json
- Never install an unpinned `dbt`. It resolves to the old dbt Cloud CLI and
  installs without error.

### Comments and docstrings

- Write every comment and docstring in ASD-STE100 Simplified Technical English
  (see the repo-root `AGENTS.md`). Rules that matter here:
  - Active voice and imperative mood. Say the action: "Drop the table", not
    "The table is dropped".
  - One thought per sentence. Keep each sentence under 20 words.
  - Use the literal meaning of a word. Avoid idioms and filler. Do not use
    vague verbs such as "leverage", "utilize", "handle", or "perform".
  - Do not use metaphors.
  - Keep a comment concise. Say only what the reader cannot read from the code.
- Do not describe what the code already states. Explain the "why" when it is
  not visible: concurrency and locking, a `graph.sources` fallback, or why a
  macro must use CTAS rather than Snowflake `CLONE`.

### Python

- Lint and format with `ruff`. Configuration lives in `pyproject.toml`
  (targets `integration_tests/automated_tests/**/*.py`, line length 88).
- Run: `uvx ruff check integration_tests/automated_tests` and
  `uvx ruff format integration_tests/automated_tests`.
- Pre-commit runs `ruff-format` and `ruff --fix` on changed Python, plus
  biome for JSON and general hygiene hooks.

### Tests

- Name test modules `test_*.py` and functions `test_*`.
- Part-of-matrix tests take `database` and `dbt_version` fixtures plus the
  `run_dbt` fixture, which yields a command runner bound to the cell's venv
  and project directory.
- Assert against the database catalog, not only log output, when you check
  that a constraint actually exists (see `assert_source_constraints.sql` and
  `tests/test_source_constraints.py`).
- A new matrix key needs an entry in `test-versions.json` and, for adapter
  databases, a compose file. A missing compose file reports as SKIPPED, not
  failed. Check collected-versus-skipped counts when you add a cell.

### SQL and Jinja macros

- Editing style targets the dbt_project version of constraints. Use the
  lifecycle of the object (table, incremental, view, seed, snapshot) to decide
  whether constraints are released.
- A source is not built by dbt and is held in `graph.sources`, not
  `graph.nodes`. Always read `graph.sources` as a fallback when resolving a
  source dependency.
- The `clone_table` macro must use CTAS, not Snowflake `CLONE`. A Snowflake
  clone copies constraints; a fresh source table must be constraint-free.
