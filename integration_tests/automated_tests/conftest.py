"""
Pytest configuration for dbt_constraints automated tests.
Uses pytest-docker to manage database containers.
Database servers run in containers (session scope). The dbt client runs on the
host, from one uv venv per cell (function scope). See dbt_venv.py.
"""

import json
import os
import secrets
import shutil
import string
import subprocess
from pathlib import Path

import dbt_venv as dbt_venv_mod
import dpos_runner as dpos_runner_mod
import dpos_stage as dpos_stage_mod
import pytest
from dotenv import load_dotenv

# Get directories
TEST_DIR = Path(__file__).parent
DOCKER_DIR = TEST_DIR / "docker"
COMPOSE_DIR = DOCKER_DIR / "compose"
CONFIG_DIR = TEST_DIR / "config"
SCRIPTS_DIR = TEST_DIR / "scripts"
INTEGRATION_TESTS_DIR = TEST_DIR.parent
PROJECT_ROOT = INTEGRATION_TESTS_DIR.parent

# Project directories for different dbt versions
DBT_CORE_PROJECT_DIR = INTEGRATION_TESTS_DIR / "dbt-core"
DBT_FUSION_PROJECT_DIR = INTEGRATION_TESTS_DIR / "dbt-fusion"

# dbt v2 engine targets. Each one installs a single self-contained wheel and no
# adapter package. Each one runs against Snowflake and parses the dbt-fusion project:
#   fusion -> the "dbt" package (Fusion)
#   core2  -> the "dbt-core" 2.x package (the Apache 2.0 base of Fusion)
V2_ENGINE_TARGETS = ("fusion", "core2")

# Targets that run dbt INSIDE Snowflake, through dbt Projects on Snowflake. No dbt
# client runs on the host for these cells. Snowflake supplies the engine.
#   dpos_core   -> dbt Core, parses the dbt-core project
#   dpos_fusion -> dbt Fusion, parses the dbt-fusion project
# See dpos_stage.py and dpos_runner.py.
DPOS_DATABASES = ("dpos_core", "dpos_fusion")

# Targets that parse the dbt-fusion project directory.
FUSION_PROJECT_TARGETS = (*V2_ENGINE_TARGETS, "dpos_fusion")

# Targets that have no local database container to start
CLOUD_DATABASES = ("snowflake", *V2_ENGINE_TARGETS, *DPOS_DATABASES)

# Name of the snow CLI connection that the dpos_* cells use. The snow CLI reads its
# credentials from connections.toml, so no secret passes through this repository.
SNOW_CONNECTION_ENV_VAR = "SNOWFLAKE_CONNECTION_NAME"

# Staging area for the deployable copy of each project. See dpos_stage.py.
DPOS_STAGE_DIR = TEST_DIR / ".dpos-stage"

# Load environment variables from integration_tests/.env for Snowflake credentials
ENV_FILE = INTEGRATION_TESTS_DIR / ".env"
if ENV_FILE.exists():
    print(f"\n📁 Loading environment variables from {ENV_FILE}")
    load_dotenv(ENV_FILE)
else:
    print(f"\n⚠️  No .env file found at {ENV_FILE} - Snowflake tests will be skipped")


def get_project_dir(database: str) -> Path:
    """Get the appropriate dbt project directory for the database type."""
    if database in FUSION_PROJECT_TARGETS:
        return DBT_FUSION_PROJECT_DIR
    else:
        return DBT_CORE_PROJECT_DIR


def generate_secure_password(length: int = 16, oracle_safe: bool = False) -> str:
    """Generate a secure random password.

    Args:
        length: Password length
        oracle_safe: If True, exclude characters that cause issues in Oracle SQL (&, backslash)
    """
    # Oracle has issues with & (substitution variable) and backslash (escape char)
    special_chars = "!@#$%^*" if oracle_safe else "!@#$%^&*"
    alphabet = string.ascii_letters + string.digits + special_chars

    # Ensure at least one of each type
    password = [
        secrets.choice(string.ascii_lowercase),
        secrets.choice(string.ascii_uppercase),
        secrets.choice(string.digits),
        secrets.choice(special_chars),
    ]
    # Fill the rest
    password.extend(secrets.choice(alphabet) for _ in range(length - 4))
    # Shuffle
    secrets.SystemRandom().shuffle(password)
    return "".join(password)


def generate_db_identifier(prefix: str = "test") -> str:
    """Generate a random database identifier."""
    return f"{prefix}_{secrets.token_hex(4)}"


def pytest_addoption(parser):
    """Add command line options."""
    parser.addoption(
        "--database",
        action="store",
        default=None,
        help="Test specific database: postgres, oracle, sqlserver",
    )
    parser.addoption(
        "--dbt-version",
        action="store",
        default=None,
        help="Test specific dbt version, e.g., 1.9.0",
    )
    parser.addoption(
        "--fast",
        action="store_true",
        default=False,
        help="Run fast mode (minimal validation)",
    )


def _build_test_parameters(
    versions_data: dict, database_filter: str | None, version_filter: str | None
) -> list[tuple[str, str]]:
    """Build test parameter combinations based on filters.

    Args:
        versions_data: Dictionary containing dbt version configurations
        database_filter: Optional database name to filter by
        version_filter: Optional dbt version to filter by

    Returns:
        List of (database, version) tuples
    """
    test_params = []

    for db, db_versions in versions_data["dbt_versions"].items():
        # Apply database filter
        if database_filter and db != database_filter:
            continue

        for version in db_versions:
            # Apply version filter
            if version_filter and version != version_filter:
                continue

            test_params.append((db, version))

    return test_params


def pytest_generate_tests(metafunc):
    """Dynamically generate test parameters from test-versions.json."""
    if (
        "database" not in metafunc.fixturenames
        or "dbt_version" not in metafunc.fixturenames
    ):
        return

    # Load version matrix
    versions_file = CONFIG_DIR / "test-versions.json"
    with open(versions_file) as f:
        versions = json.load(f)

    # Get CLI filters
    database_filter = metafunc.config.getoption("database")
    version_filter = metafunc.config.getoption("dbt_version")

    # Build test parameters using helper function
    test_params = _build_test_parameters(versions, database_filter, version_filter)

    # Parameterize tests
    metafunc.parametrize("database,dbt_version", test_params, scope="function")


# Per-test timeout for an in-database cell, in seconds.
#
# pytest.ini sets 600 seconds, which suits a host cell. A dpos_* cell pays more inside
# the first test of the session: it stages the project, runs dbt deps, deploys the
# object, then seeds and builds. Each later test is a single command and finishes well
# inside this value.
DPOS_TEST_TIMEOUT = 2400


def pytest_collection_modifyitems(items):
    """Give each in-database cell a longer timeout than the host cells."""
    for item in items:
        database = (
            item.callspec.params.get("database") if hasattr(item, "callspec") else None
        )
        if database in DPOS_DATABASES:
            item.add_marker(pytest.mark.timeout(DPOS_TEST_TIMEOUT))


# =============================================================================
# DATABASE CONTAINERS (Session Scope - Start Once)
# =============================================================================


@pytest.fixture(scope="session")
def docker_compose_command() -> str:
    """Use Docker Compose V2."""
    return "docker compose"


@pytest.fixture(scope="session")
def docker_compose_project_name() -> str:
    """Fixed project name for database containers."""
    return "dbt-test-db"


@pytest.fixture(scope="session")
def database_project_name() -> str:
    """Alias for docker_compose_project_name for backwards compatibility."""
    return "dbt-test-db"


@pytest.fixture(scope="session")
def db_connection_config() -> dict[str, dict[str, str]]:
    """Generate random credentials for all databases (once per session).

    Local databases get randomly generated credentials.
    Cloud databases (Snowflake) use credentials from environment variables.
    """
    print("\n🔐 Setting up database credentials...")

    credentials = {
        "postgres": {
            "user": generate_db_identifier("dbtusr"),
            "password": generate_secure_password(),
            "database": generate_db_identifier("dbtdb"),
        },
        "oracle": {
            "user": generate_db_identifier("dbtusr"),
            "password": generate_secure_password(
                20, oracle_safe=True
            ),  # Oracle needs stronger passwords, exclude problematic chars
            "service": "FREEPDB1",  # Oracle service name (not random)
            "database": "FREEPDB1",  # Oracle database name (not random)
        },
        "sqlserver": {
            "user": "sa",  # SQL Server requires 'sa' user
            "password": generate_secure_password(
                20
            ),  # SQL Server needs complex passwords
            "database": "master",  # SQL Server default database
        },
    }

    # Add Snowflake credentials from environment variables (loaded from .env)
    # Loop through expected Snowflake env vars and add only if set
    snowflake_env_vars = {
        "account": "SNOWFLAKE_ACCOUNT",
        "user": "SNOWFLAKE_USER",
        "password": "SNOWFLAKE_PASSWORD",
        "private_key_path": "SNOWFLAKE_PRIVATE_KEY_PATH",
        "private_key_passphrase": "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE",
        "role": "SNOWFLAKE_ROLE",
        "database": "SNOWFLAKE_DATABASE",
        "warehouse": "SNOWFLAKE_WAREHOUSE",
        "schema": "SNOWFLAKE_SCHEMA",
    }

    snowflake_creds = {}
    for key, env_var in snowflake_env_vars.items():
        value = os.environ.get(env_var)
        if value:
            snowflake_creds[key] = value

    # Only add Snowflake if we have at least account and user
    if snowflake_creds.get("account") and snowflake_creds.get("user"):
        credentials["snowflake"] = snowflake_creds
        # The v2 engines use the same Snowflake credentials
        for target in V2_ENGINE_TARGETS:
            credentials[target] = snowflake_creds.copy()
        # The dpos_* cells do not use these credentials to run dbt. Snowflake runs the
        # engine, and the snow CLI authenticates from connections.toml. They still need
        # the database and schema names, which name where the project object is deployed.
        for target in DPOS_DATABASES:
            credentials[target] = snowflake_creds.copy()

        # Determine auth method for logging
        if snowflake_creds.get("private_key_path"):
            auth_method = "private_key"
        elif snowflake_creds.get("password"):
            auth_method = "password"
        else:
            auth_method = "unknown"

        print(
            f"  ☁️  snowflake: account={snowflake_creds['account']}, user={snowflake_creds['user']}, "
            f"db={snowflake_creds.get('database', 'N/A')}, auth={auth_method}"
        )
        print("  🚀 fusion, core2: using same Snowflake credentials (dbt v2 engines)")
    else:
        print(
            "  ⚠️  Snowflake credentials not found in environment (need at least SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER)"
        )
        print(
            "  ⚠️  Fusion and core2 tests will be skipped (requires Snowflake credentials)"
        )

    # Print local database credentials for debugging
    for db, creds in credentials.items():
        if db != "snowflake":
            print(
                f"  {db}: user={creds.get('user', 'N/A')}, db={creds.get('database', 'N/A')}"
            )

    return credentials


@pytest.fixture(scope="session")
def database_compose_files(request) -> dict[str, list[str]]:
    """Map of database -> compose file for database containers.
    Note: Snowflake is cloud-based and doesn't need a local database container."""
    return {
        "postgres": [str(COMPOSE_DIR / "postgres-db.yml")],
        "oracle": [str(COMPOSE_DIR / "oracle-db.yml")],
        "sqlserver": [str(COMPOSE_DIR / "sqlserver-db.yml")],
        # snowflake: no local database needed (cloud service)
    }


@pytest.fixture(scope="session")
def docker_compose_file(request, database_compose_files) -> list[str]:
    """Get compose files for pytest-docker plugin.
    Returns list of all compose files to start based on --database filter."""
    database_filter = request.config.getoption("database")

    if database_filter:
        if database_filter in database_compose_files:
            return database_compose_files[database_filter]
        else:
            return []  # Cloud database, no compose file
    else:
        # Return all compose files for parallel startup
        all_files = []
        for files in database_compose_files.values():
            all_files.extend(files)
        return all_files


@pytest.fixture(scope="session")
def docker_setup() -> list[str]:
    """Commands to run for docker setup. Uses --wait for healthchecks."""
    return ["up -d --build --wait"]


@pytest.fixture(scope="session")
def docker_cleanup() -> list[str]:
    """Commands to run for docker cleanup."""
    return ["down -v"]


@pytest.fixture(scope="session", autouse=True)
def setup_database_env(db_connection_config):
    """Set up environment variables for database credentials before docker starts.
    This is autouse so it runs before docker_services fixture."""
    print("\n🔧 Setting up environment variables for databases...")

    # Store original env to restore later
    original_env = {}

    # Set all database credentials in environment
    for db, creds in db_connection_config.items():
        if db == "postgres":
            original_env["POSTGRES_USER"] = os.environ.get("POSTGRES_USER")
            original_env["POSTGRES_PASSWORD"] = os.environ.get("POSTGRES_PASSWORD")
            original_env["POSTGRES_DB"] = os.environ.get("POSTGRES_DB")
            os.environ["POSTGRES_USER"] = creds["user"]
            os.environ["POSTGRES_PASSWORD"] = creds["password"]
            os.environ["POSTGRES_DB"] = creds["database"]
        elif db == "oracle":
            original_env["ORACLE_USER"] = os.environ.get("ORACLE_USER")
            original_env["ORACLE_PASSWORD"] = os.environ.get("ORACLE_PASSWORD")
            os.environ["ORACLE_USER"] = creds["user"]
            os.environ["ORACLE_PASSWORD"] = creds["password"]
        elif db == "sqlserver":
            original_env["SQLSERVER_PASSWORD"] = os.environ.get("SQLSERVER_PASSWORD")
            os.environ["SQLSERVER_PASSWORD"] = creds["password"]

    yield

    # Restore original environment
    for key, value in original_env.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


@pytest.fixture(scope="session")
def start_databases(request, database_compose_files):
    """Start all database containers using pytest-docker.
    docker_services fixture handles startup and waiting automatically with --wait flag.
    Note: Cloud databases (e.g., Snowflake) are skipped as they don't need local containers."""

    # Determine which databases need to be started
    database_filter = request.config.getoption("database")

    if database_filter:
        # Check if this database needs a local container
        if database_filter not in database_compose_files:
            # Cloud database - no container needed
            print(f"\n☁️  {database_filter} is a cloud database, no container needed")
            yield [database_filter]
            return

        databases_to_start = [database_filter]
    else:
        # Start all local databases
        databases_to_start = list(database_compose_files.keys())

    # Only request docker_services if we have containers to start
    if databases_to_start:
        # Import docker_services dynamically to avoid triggering it for cloud DBs
        _docker_services = request.getfixturevalue("docker_services")  # noqa: F841
        print("\n✅ All database containers started and healthy (via pytest-docker)")

        if "oracle" in databases_to_start:
            _create_oracle_custom_schema(request)

    yield databases_to_start


def _create_oracle_custom_schema(request) -> None:
    """
    Create the extra schema the issue_105 tests need on Oracle.

    On Oracle a schema is a user, and only a privileged account can create one. dbt
    cannot create it, so the models that set `schema='test_schema_<env>'` fail with
    ORA-01918 unless it exists first. PostgreSQL needs nothing equivalent because dbt
    can create a schema itself.

    The schema name must match generate_schema_name in the dbt project, which replaces
    the <env> placeholder with DBT_TEST_ENV and defaults to 'dev'.
    """
    env_name = os.environ.get("DBT_TEST_ENV", "dev")
    custom_schema = f"test_schema_{env_name}".upper()
    app_user = os.environ.get("ORACLE_USER", "")

    # Discover the container rather than rebuilding its name. oracle-db.yml derives the
    # name from COMPOSE_PROJECT_NAME, which is not exported to this process.
    found = subprocess.run(
        ["docker", "ps", "--filter", "name=oracle", "--format", "{{.Names}}"],
        capture_output=True,
        text=True,
        check=False,
    )
    containers = [n for n in found.stdout.split() if n.endswith("-oracle")]
    if not containers:
        print("\n⚠️  No running Oracle container found; skipping schema preparation")
        return
    container = containers[0]

    statements = [
        f"CREATE USER {custom_schema} IDENTIFIED BY dbt_test_schema_pw "
        f"QUOTA UNLIMITED ON USERS",
        f"GRANT CREATE SESSION TO {custom_schema}",
    ]
    # The dbt user builds objects inside that schema, so it needs the ANY privileges.
    if app_user:
        for priv in (
            "CREATE ANY TABLE",
            "DROP ANY TABLE",
            "SELECT ANY TABLE",
            "INSERT ANY TABLE",
            "ALTER ANY TABLE",
            "CREATE ANY INDEX",
            "DROP ANY INDEX",
        ):
            statements.append(f"GRANT {priv} TO {app_user}")

    # ORA-01920 means the user is already there, which is fine on a reused container.
    sql = "ALTER SESSION SET CONTAINER = FREEPDB1;\n"
    sql += "\n".join(
        f"BEGIN EXECUTE IMMEDIATE '{s}'; "
        f"EXCEPTION WHEN OTHERS THEN IF SQLCODE != -1920 THEN RAISE; END IF; END;\n/"
        for s in statements
    )
    sql += "\nEXIT\n"

    # Connect with OS authentication inside the container, not over the network. The
    # generated Oracle password can contain '@', which breaks an Easy Connect string
    # such as sys/pw@//host:port/service and fails with ORA-12262. "/ as sysdba" lands
    # in CDB$ROOT, so the SQL above switches to the FREEPDB1 container first.
    result = subprocess.run(
        ["docker", "exec", "-i", container, "sqlplus", "-S", "/", "as", "sysdba"],
        input=sql,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0 or "ORA-" in result.stdout:
        print(
            f"\n⚠️  Could not prepare Oracle schema {custom_schema}. "
            f"The issue_105 custom-schema tests will fail.\n{result.stdout}{result.stderr}"
        )
    else:
        print(f"\n🔧 Oracle schema {custom_schema} ready")


@pytest.fixture(scope="function")
def dbt_env(
    database: str,
    dbt_version: str,
    database_project_name: str,
    db_connection_config: dict[str, dict[str, str]],
) -> dict[str, str]:
    """Environment variables for the dbt client (using random credentials)."""
    env = os.environ.copy()
    # The v2 engines and the in-database Fusion cell use Snowflake as the database target
    env["DBT_TARGET"] = (
        "snowflake"
        if database in V2_ENGINE_TARGETS or database in DPOS_DATABASES
        else database
    )
    env["DBT_VERSION"] = dbt_version
    env["COMPOSE_PROJECT_NAME"] = database_project_name  # To connect to DB

    # Use host paths. dbt runs on the host, not in a container.
    project_dir = get_project_dir(database)
    env["DBT_PROJECT_DIR"] = str(project_dir)
    env["DBT_PROFILES_DIR"] = str(project_dir)

    # The database containers publish their ports to the host. profiles.yml uses
    # localhost as the default host. Remove each container hostname from the
    # environment. This prevents an old value from the caller.
    for stale in ("POSTGRES_HOST", "ORACLE_HOST", "SQLSERVER_HOST"):
        env.pop(stale, None)

    # A container did not read the shell of the developer. The host does. Remove the
    # ambient client settings, such as PGSERVICE, that override the profile.
    dbt_venv_mod.strip_ambient_db_vars(env)

    # Database-specific env vars (using random credentials from fixture)
    if database in db_connection_config:
        creds = db_connection_config[database]
        if database == "postgres":
            env.update(
                {
                    "POSTGRES_USER": creds["user"],
                    "POSTGRES_PASSWORD": creds["password"],
                    "POSTGRES_DB": creds["database"],
                }
            )
        elif database == "oracle":
            env.update(
                {
                    "ORACLE_USER": creds["user"],
                    "ORACLE_PASSWORD": creds["password"],
                    "ORACLE_SERVICE": creds["service"],
                    "ORACLE_DATABASE": creds["database"],
                }
            )
        elif database == "sqlserver":
            env.update(
                {
                    "SQLSERVER_USER": creds["user"],
                    "SQLSERVER_PASSWORD": creds["password"],
                    "SQLSERVER_DATABASE": creds["database"],
                }
            )
        elif database in CLOUD_DATABASES:
            # Map credential keys to environment variable names
            # Snowflake and the two v2 engines use the same Snowflake credentials
            snowflake_env_map = {
                "account": "SNOWFLAKE_ACCOUNT",
                "user": "SNOWFLAKE_USER",
                "password": "SNOWFLAKE_PASSWORD",
                "private_key_path": "SNOWFLAKE_PRIVATE_KEY_PATH",
                "private_key_passphrase": "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE",
                "role": "SNOWFLAKE_ROLE",
                "database": "SNOWFLAKE_DATABASE",
                "warehouse": "SNOWFLAKE_WAREHOUSE",
                "schema": "SNOWFLAKE_SCHEMA",
            }

            # Set environment variables for each credential that exists
            for key, env_var in snowflake_env_map.items():
                if key in creds:
                    env[env_var] = creds[key]

    return env


# =============================================================================
# DBT CLIENT VIRTUAL ENVIRONMENTS (Function Scope - Per Test)
# =============================================================================
# dbt runs on the host, from one uv venv per cell. The database servers are still
# Docker containers. Only the dbt client moved. See dbt_venv.py for the reason.


@pytest.fixture(scope="session")
def venv_root() -> Path:
    """Directory holding one cached venv per matrix cell."""
    return PROJECT_ROOT / ".venvs"


@pytest.fixture(scope="session")
def prepared_projects() -> set[str]:
    """Track which matrix cells were cleaned, deps-installed, seeded and built."""
    return set()


@pytest.fixture(scope="session")
def baseline_outputs() -> dict[str, str]:
    """Hold the captured output of each cell's preparation build.

    Keyed by the same cell key that `prepared_projects` uses. A value of None means the
    build did not finish.
    """
    return {}


# =============================================================================
# DBT PROJECTS ON SNOWFLAKE (Session Scope - Deploy Once Per Cell)
# =============================================================================
# A dpos_* cell runs dbt inside Snowflake. The host stages a deployable copy of the
# project, deploys it as a DBT PROJECT object, then runs each command with the snow CLI.
# See dpos_stage.py for why the stage is necessary and dpos_runner.py for the commands
# that Snowflake supports.


@pytest.fixture(scope="session")
def snow_connection() -> str | None:
    """
    Return the snow CLI connection name for the in-database cells.

    There is no default. A default would deploy to whichever account connections.toml
    marks as "default", which is rarely the account the test credentials name. Set
    SNOWFLAKE_CONNECTION_NAME in integration_tests/.env.
    """
    return os.environ.get(SNOW_CONNECTION_ENV_VAR)


@pytest.fixture(scope="session")
def deployed_dpos_projects() -> dict[str, dict[str, str]]:
    """Track which cells already have a deployed project object this session."""
    return {}


@pytest.fixture(scope="function")
def dpos_project(
    database: str,
    dbt_version: str,
    dbt_venv: Path,
    snow_connection: str | None,
    db_connection_config: dict[str, dict[str, str]],
    deployed_dpos_projects: dict[str, dict[str, str]],
) -> dict[str, str] | None:
    """
    Stage and deploy this cell's project, one time per session.

    Return the values that dpos_runner.execute needs, or None for a host cell.
    """
    if database not in DPOS_DATABASES:
        return None

    key = f"{database}:{dbt_version}"
    if key in deployed_dpos_projects:
        return deployed_dpos_projects[key]

    if not snow_connection:
        pytest.skip(
            f"{SNOW_CONNECTION_ENV_VAR} is not set. The dpos_* cells run dbt inside "
            "Snowflake with the snow CLI, which reads credentials from "
            "connections.toml. Set it to the name of the connection for the account "
            "that the SNOWFLAKE_* values in .env describe."
        )

    if not dpos_runner_mod.connection_exists(snow_connection):
        pytest.skip(
            f"snow CLI connection '{snow_connection}' not found. "
            "Run 'snow connection list' to see the available names."
        )

    creds = db_connection_config.get(database, {})
    object_database = creds.get("database")
    object_schema = creds.get("schema", "dbt_constraints_test")
    # Pass the role and the warehouse explicitly. env.yml resolves the run target from
    # the session, and a connections.toml entry can name a different database or no role.
    object_role = creds.get("role")
    object_warehouse = creds.get("warehouse")
    if not object_database:
        pytest.skip(
            "SNOWFLAKE_DATABASE is not set. The dpos_* cells need it to name where "
            "the dbt project object is deployed."
        )

    project_dir = get_project_dir(database)
    stage_dir = DPOS_STAGE_DIR / database
    dbt_bin = dbt_venv_mod.dbt_executable(dbt_venv)

    print(f"\n📦 Staging {project_dir.name} for {database} at {stage_dir}")
    try:
        dpos_stage_mod.stage_project(project_dir, PROJECT_ROOT, stage_dir, dbt_bin)
    except RuntimeError as exc:
        pytest.fail(f"Could not stage {database}:\n{exc}")

    file_count = dpos_stage_mod.count_files(stage_dir)
    print(f"  staged {file_count} files (a dbt project object allows 100,000)")
    if file_count > 100_000:
        pytest.fail(
            f"The stage holds {file_count} files, above the 100,000 file limit for a "
            "dbt project object. Widen the exclusions in dpos_stage.py."
        )

    object_name = dpos_runner_mod.object_name_for(database)

    schema_result = dpos_runner_mod.create_schema(
        snow_connection, object_database, object_schema, object_role, object_warehouse
    )
    if schema_result.returncode != 0:
        pytest.fail(
            f"Could not create {object_database}.{object_schema}:\n"
            f"{schema_result.stdout}\n{schema_result.stderr}"
        )

    print(f"  🚀 Deploying {object_database}.{object_schema}.{object_name}")
    deploy_result = dpos_runner_mod.deploy(
        stage_dir,
        snow_connection,
        object_name,
        object_database,
        object_schema,
        dbt_version,
        object_role,
        object_warehouse,
    )
    if deploy_result.returncode != 0:
        pytest.fail(
            f"snow dbt deploy failed for {database}:\n"
            f"{deploy_result.stdout}\n{deploy_result.stderr}"
        )

    info = {
        "connection": snow_connection,
        "object_name": object_name,
        "database": object_database,
        "schema": object_schema,
        "dbt_version": dbt_version,
        "role": object_role,
        "warehouse": object_warehouse,
    }
    deployed_dpos_projects[key] = info
    return info


@pytest.fixture(scope="session")
def prepared_venvs() -> dict[str, Path]:
    """Track which cells already have a venv this session."""
    return {}


@pytest.fixture(scope="function")
def dbt_venv(
    database: str,
    dbt_version: str,
    venv_root: Path,
    prepared_venvs: dict[str, Path],
) -> Path:
    """Return a ready venv for this cell, creating it on first use."""
    # The runner image supplied msodbcsql18. On the host it is a real prerequisite.
    # Skip the cell with a clear message instead of a failure inside dbt.
    if database == "sqlserver" and not dbt_venv_mod.sqlserver_driver_present():
        pytest.skip(
            "ODBC Driver 18 for SQL Server not installed on this host. "
            "Install msodbcsql18 to run sqlserver cells."
        )

    key = f"{database}:{dbt_version}"
    if key in prepared_venvs:
        return prepared_venvs[key]

    try:
        venv = dbt_venv_mod.ensure_venv(venv_root, database, dbt_version)
    except RuntimeError as exc:
        pytest.fail(f"Could not prepare venv for {key}:\n{exc}")

    prepared_venvs[key] = venv
    print(f"\n🐍 venv ready for dbt-{database}:{dbt_version}")
    return venv


@pytest.fixture(scope="function")
def run_dbt(
    database: str,
    dbt_version: str,
    dbt_env: dict[str, str],
    dbt_venv: Path,
    prepared_projects: set[str],
    baseline_outputs: dict[str, str],
    start_databases,
    dpos_project: dict[str, str] | None,
):
    """Run dbt commands, either on the host or inside Snowflake."""

    # A cloud target has no local database container to wait for.
    if database not in start_databases and database not in CLOUD_DATABASES:
        pytest.skip(f"Database {database} not started")

    project_dir = get_project_dir(database)
    dbt_bin = dbt_venv_mod.dbt_executable(dbt_venv)

    def _run_on_host(command: str) -> subprocess.CompletedProcess:
        """Run one dbt command from this cell's venv, in the project directory."""
        # A test passes a full shell string, such as "dbt build --full-refresh".
        # Some tests use shell features. Put this venv first on PATH. A bare "dbt"
        # then runs the client of this cell, not another install. The scripts
        # directory follows the venv. A test can then call run_dbt_tests.sh in the
        # same way as before, when the script was on the container PATH.
        env = dbt_env.copy()
        env["PATH"] = os.pathsep.join(
            [str(dbt_bin.parent), str(SCRIPTS_DIR), env.get("PATH", "")]
        )
        env["VIRTUAL_ENV"] = str(dbt_venv)

        return subprocess.run(
            command,
            shell=True,
            cwd=str(project_dir),
            capture_output=True,
            text=True,
            timeout=600,
            check=False,
            env=env,
        )

    def _run_in_snowflake(command: str) -> subprocess.CompletedProcess:
        """Run one dbt command against the deployed dbt project object."""
        # snow is a host process, so this still returns a CompletedProcess. The
        # assertions in the tests read .returncode and .stdout either way.
        try:
            return dpos_runner_mod.execute(
                command,
                connection=dpos_project["connection"],
                object_name=dpos_project["object_name"],
                database=dpos_project["database"],
                schema=dpos_project["schema"],
                dbt_version=dpos_project["dbt_version"],
                role=dpos_project["role"],
                warehouse=dpos_project["warehouse"],
            )
        except dpos_runner_mod.UnsupportedCommandError as exc:
            pytest.skip(str(exc))

    def _run_dbt_command(
        command: str, check: bool = True
    ) -> subprocess.CompletedProcess:
        """Run one dbt command and return the completed process."""
        if dpos_project is not None:
            result = _run_in_snowflake(command)
        else:
            result = _run_on_host(command)

        if check and result.returncode != 0:
            print(f"\n{'=' * 80}")
            print(f"COMMAND FAILED: {command}")
            print(f"{'=' * 80}")
            print("STDOUT:")
            print(result.stdout)
            print("\nSTDERR:")
            print(result.stderr)
            print(f"{'=' * 80}\n")
            raise subprocess.CalledProcessError(
                result.returncode, command, result.stdout, result.stderr
            )

        return result

    # Prepare each matrix cell one time per session.
    #
    # `dbt clean` removes dbt_packages and target, because clean-targets in
    # dbt_project.yml lists both. A run of `dbt clean` in each test removes the
    # packages that the later tests need. It runs one time here. Each test resets
    # only target/.
    #
    # The full build is necessary for correctness, not only for speed. A test that
    # runs `dbt build --select <model>` also selects the relationship tests that
    # belong to OTHER models and refer to the selected model. Each of those tests
    # needs its own model. A `+` selector does not help. It selects the downstream
    # tests, but not the models that declare them. One full build gives each
    # selective build a complete schema.
    #
    # The key holds the cell, not only the project directory. Two cells that share a
    # project directory write to different databases or schemas, so each one needs its
    # own seed and build. Keying on the directory alone prepared only the first cell.
    cell_key = f"{database}:{dbt_version}:{project_dir.name}"
    if cell_key not in prepared_projects:
        # Record the attempt before running it. If preparation fails, every later
        # test would otherwise retry the whole clean/deps/seed/build sequence, which
        # turns one failure into one slow failure per test. On Oracle that took a
        # single broken model to 28 minutes.
        prepared_projects.add(cell_key)
        if dpos_project is not None:
            # A deployed dbt project object is immutable, so `dbt clean` and `dbt deps`
            # have nothing to act on. dpos_stage.py already ran deps while staging.
            print(f"\n🧹 Preparing {database}: seed, build")
        else:
            print(f"\n🧹 Preparing {project_dir.name}: clean, deps, seed, build")
            _run_dbt_command("dbt clean")
            _run_dbt_command("dbt deps")
        _run_dbt_command("dbt seed --full-refresh")
        # Tolerate a failure here. Some models cannot build on every target, for
        # example the issue_105 custom-schema models on Oracle, where a schema is a
        # user that must already exist. A hard failure would error every test in the
        # cell at setup and hide which test actually cares. Each test still runs its
        # own build and reports its own error.
        prepare_build = _run_dbt_command("dbt build --full-refresh", check=False)
        # Keep the output. Most tests only need to read this one build, so they assert
        # against it through the baseline_build fixture and run no dbt command of their
        # own. See baseline_build for why that is safe.
        baseline_outputs[cell_key] = (
            None
            if prepare_build.returncode != 0
            else prepare_build.stdout + prepare_build.stderr
        )
        if prepare_build.returncode != 0:
            print(
                f"\n⚠️  Preparation build did not complete for {cell_key}. "
                "Tests that need a fully built schema may fail individually."
            )

    yield _run_dbt_command

    # There is nothing to remove. The venv stays in the cache, so a later run pays
    # no install cost. This fixture starts no container.


# =============================================================================
# TEST-SPECIFIC FIXTURES (For Issue #105 Tests)
# =============================================================================


@pytest.fixture(scope="function")
def dbt_runner(run_dbt):
    """
    Wrapper fixture that provides a callable dbt runner for tests.
    This is used by test_issue_105.py and provides a simpler interface.
    """

    def _dbt_runner(args):
        """Run dbt with the given arguments and return the result."""
        # Convert args list to dbt command string (with "dbt" prefix)
        cmd = "dbt " + " ".join(args)
        return run_dbt(cmd)

    return _dbt_runner


@pytest.fixture(scope="function")
def reset_target(database: str):
    """
    Remove the target directory to force a full recompile.

    Use this fixture instead of `dbt clean`. dbt clean also removes dbt_packages.
    The session installs dbt_packages one time, and each later test needs it.
    """

    def _reset() -> None:
        # A dpos_* cell has no host target/ directory. Snowflake holds the compiled
        # output inside the deployed object, which is immutable.
        if database in DPOS_DATABASES:
            return
        target = get_project_dir(database) / "target"
        if target.exists():
            shutil.rmtree(target)

    return _reset


@pytest.fixture(scope="function")
def dbt_seed(run_dbt, database):
    """
    Guarantee that the seed data is loaded before a test runs.

    This does NOT run `dbt seed` itself. Requesting `run_dbt` prepares the cell, and that
    preparation already ran `dbt seed --full-refresh` once for the session.

    Re-seeding per test bought nothing. The seeds are static CSV files under `data/`, so
    no test can change them. It cost one dbt invocation per test, which on an in-database
    cell is 30 to 60 seconds of fixed overhead each time.

    A test that needs seeds reloaded, for example after deliberately corrupting a seed
    table, must call `run_dbt("dbt seed --full-refresh")` itself and say why.
    """
    yield


@pytest.fixture(scope="function")
def target(database):
    """
    Provide the database target name for tests.

    A dpos_* cell runs against Snowflake, so it reports "snowflake". Tests that check
    Snowflake behaviour, such as RELY on a constraint, then run for it. Tests that check
    a host-only Fusion behaviour, such as `dbt --version`, gate on "fusion" and stay
    skipped, which is correct: Snowflake does not run `dbt --version`.
    """
    if database in DPOS_DATABASES:
        return "snowflake"
    return database


@pytest.fixture(scope="function")
def baseline_build(
    run_dbt, database: str, dbt_version: str, baseline_outputs: dict[str, str]
) -> str:
    """
    Return the log of this cell's one full build, as lower-case text.

    The session prepares each cell with `dbt seed --full-refresh` followed by
    `dbt build --full-refresh`. That single build creates every model and runs every
    constraint the package can create, so a test that only needs to confirm "the package
    created constraint X" can read this log and run no dbt command at all.

    Why this matters: each dbt invocation pays a large fixed cost, and it dominates the
    suite. An in-database cell pays a CLI start, a dbt runtime start inside Snowflake and
    a full project parse before any useful work. Rebuilding one model to grep its log
    cost 60 to 90 seconds and told us nothing the full build had not already shown.

    A test that reads this log MUST assert on a specific constraint name, such as
    DIM_PART_P_PARTKEY_PK. Do NOT assert on a bare word such as "primary_key". This log
    covers every model, so a bare word is present because of some OTHER model and the
    assertion would pass even when the model under test failed.

    A test that changes data or needs its own selection or vars must still call `run_dbt`.
    """
    cell_key = f"{database}:{dbt_version}:{get_project_dir(database).name}"
    output = baseline_outputs.get(cell_key)
    if output is None:
        pytest.fail(
            f"The preparation build for {cell_key} did not complete, so there is no "
            "baseline log to assert against. Read the preparation output above for the "
            "cause. This is a setup failure, not a constraint failure."
        )
    return output.lower()


@pytest.fixture(scope="session")
def transition_outputs() -> dict[str, str]:
    """Hold the captured output of each cell's one transition build. See transition_build."""
    return {}


@pytest.fixture(scope="function")
def transition_build(
    run_dbt,
    baseline_build,
    database: str,
    dbt_version: str,
    transition_outputs: dict[str, str],
) -> str:
    """
    Return the log of one build that runs AFTER the baseline, as lower-case text.

    This covers the realistic sequence for issue #110: a constraint that the baseline
    created as RELY must flip to NORELY once the data degrades. `dim_issue_110_rely_flip`
    is an append-only incremental model, and the var appends a duplicate row.

    The same code path is also covered inside the baseline build, with no extra
    invocation, by the three `dim_issue_110_prestaged_*` models whose post-hooks
    pre-create the constraint with the wrong flag. This build exists because a user meets
    the bug through changing data over two runs, not through a pre-staged constraint.

    Every other model rebuilds unchanged here, which also exercises the incremental path
    over tables that already carry their constraints.

    Runs once per cell. Depends on baseline_build so the ordering is guaranteed.
    """
    cell_key = f"{database}:{dbt_version}:{get_project_dir(database).name}"
    if cell_key in transition_outputs:
        return transition_outputs[cell_key]

    result = run_dbt(
        "dbt build --vars '{issue_110_inject_dup: true}'",
        check=False,
    )
    if result.returncode != 0:
        pytest.fail(
            f"The transition build for {cell_key} failed, so no test can observe a "
            "constraint changing state:\n"
            f"{result.stdout}\n{result.stderr}"
        )

    output = (result.stdout + result.stderr).lower()
    transition_outputs[cell_key] = output
    return output


@pytest.fixture(scope="function")
def runs_in_snowflake(database) -> bool:
    """
    Report whether this cell runs dbt inside Snowflake.

    A test uses this to avoid a command that only works on the host, such as a shell
    script or `dbt debug`. See dpos_runner.py for the commands Snowflake supports.
    """
    return database in DPOS_DATABASES


@pytest.fixture(scope="function")
def dbt_issue_105_models(baseline_build):
    """
    Deprecated. Return the baseline build log twice.

    This fixture used to run `dbt build --select issue_105*` on every test that requested
    it, which was 13 invocations of the same build in one session. The session's full
    build already covers every issue_105 model.

    Kept only so an out-of-tree test keeps working. Read `baseline_build` directly and
    assert on a specific constraint name instead. See test_issue_105.py.
    """
    return (baseline_build, baseline_build)
