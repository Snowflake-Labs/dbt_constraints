"""
Pytest configuration for dbt_constraints automated tests.
Uses pytest-docker to manage database containers.
Separates database containers (session scope) from dbt runners (function scope).
"""

import json
import os
import secrets
import string
import subprocess
from pathlib import Path

import pytest
from dotenv import load_dotenv

# Get directories
TEST_DIR = Path(__file__).parent
DOCKER_DIR = TEST_DIR / "docker"
COMPOSE_DIR = DOCKER_DIR / "compose"
CONFIG_DIR = TEST_DIR / "config"
INTEGRATION_TESTS_DIR = TEST_DIR.parent

# Project directories for different dbt versions
DBT_CORE_PROJECT_DIR = INTEGRATION_TESTS_DIR / "dbt-core"
DBT_FUSION_PROJECT_DIR = INTEGRATION_TESTS_DIR / "dbt-fusion"

# Load environment variables from integration_tests/.env for Snowflake credentials
ENV_FILE = INTEGRATION_TESTS_DIR / ".env"
if ENV_FILE.exists():
    print(f"\n📁 Loading environment variables from {ENV_FILE}")
    load_dotenv(ENV_FILE)
else:
    print(f"\n⚠️  No .env file found at {ENV_FILE} - Snowflake tests will be skipped")


def get_project_dir(database: str) -> Path:
    """Get the appropriate dbt project directory for the database type."""
    if database == "fusion":
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
        # Fusion uses the same Snowflake credentials
        credentials["fusion"] = snowflake_creds.copy()

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
        print("  🚀 fusion: using same Snowflake credentials (dbt-fusion engine)")
    else:
        print(
            "  ⚠️  Snowflake credentials not found in environment (need at least SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER)"
        )
        print("  ⚠️  Fusion tests will be skipped (requires Snowflake credentials)")

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

    yield databases_to_start


# =============================================================================
# DBT RUNNER CONTAINERS (Function Scope - Per Test)
# =============================================================================


@pytest.fixture(scope="function")
def runner_project_name(database: str, dbt_version: str) -> str:
    """Unique project name for this test run."""
    version_clean = dbt_version.replace(".", "")
    return f"dbt-test-{database}-{version_clean}"


@pytest.fixture(scope="function")
def runner_compose_file(database: str) -> str:
    """Get runner compose file for the database."""
    compose_file = COMPOSE_DIR / f"{database}-runner.yml"
    if not compose_file.exists():
        pytest.skip(f"No runner compose file for {database}")
    return str(compose_file)


@pytest.fixture(scope="function")
def dbt_env(
    database: str,
    dbt_version: str,
    database_project_name: str,
    db_connection_config: dict[str, dict[str, str]],
) -> dict[str, str]:
    """Environment variables for dbt runner (using random credentials)."""
    env = os.environ.copy()
    # Fusion uses Snowflake as its underlying database target
    env["DBT_TARGET"] = "snowflake" if database == "fusion" else database
    env["DBT_VERSION"] = dbt_version
    env["COMPOSE_PROJECT_NAME"] = database_project_name  # To connect to DB

    # Set the correct project directory based on database type
    project_dir = get_project_dir(database)
    env["DBT_PROJECT_DIR"] = f"/project/integration_tests/{project_dir.name}"
    env["DBT_PROFILES_DIR"] = f"/project/integration_tests/{project_dir.name}"

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
        elif database in ("snowflake", "fusion"):
            # Map credential keys to environment variable names
            # Both snowflake and fusion use the same Snowflake credentials
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


@pytest.fixture(scope="session")
def built_images() -> dict[str, bool]:
    """Track which database+version images have been built this session."""
    return {}


@pytest.fixture(scope="function")
def run_dbt(
    database: str,
    dbt_version: str,
    runner_project_name: str,
    runner_compose_file: str,
    dbt_env: dict[str, str],
    start_databases,
    docker_compose_command: str,
    built_images: dict[str, bool],
):
    """Run dbt commands inside the test container."""

    # Ensure this database is started (skip check for cloud databases like Snowflake/Fusion)
    cloud_databases = ["snowflake", "fusion"]
    if database not in start_databases and database not in cloud_databases:
        pytest.skip(f"Database {database} not started")

    # Build the runner container only once per database+version combo
    build_key = f"{database}:{dbt_version}"
    if build_key not in built_images:
        print(
            f"\n🏗️  Building dbt-{database}:{dbt_version} container (first time this session)..."
        )
        cmd = docker_compose_command.split() + [
            "-f",
            runner_compose_file,
            "-p",
            runner_project_name,
            "build",
            "--pull",  # Pull latest base images
            "--build-arg",
            f"DBT_VERSION={dbt_version}",
        ]
        # Only add DBT_ADAPTER for non-fusion databases (fusion is standalone binary)
        if database != "fusion":
            cmd.extend(["--build-arg", f"DBT_ADAPTER={database}"])

        # Enable Docker BuildKit for better caching
        build_env = dbt_env.copy()
        build_env["DOCKER_BUILDKIT"] = "1"
        build_env["COMPOSE_DOCKER_CLI_BUILD"] = "1"

        subprocess.run(
            cmd,
            env=build_env,
            check=True,
        )
        built_images[build_key] = True
        print(f"✅ Image cached for {build_key}")
    else:
        print(f"♻️  Reusing cached image for dbt-{database}:{dbt_version}")

    def _run_dbt_command(
        command: str, check: bool = True
    ) -> subprocess.CompletedProcess:
        """Execute a dbt command in the container."""

        # Build docker compose run command
        # Always use bash -c for proper command execution
        cmd = docker_compose_command.split() + [
            "-f",
            runner_compose_file,
            "-p",
            runner_project_name,
            "run",
            "--rm",
            f"dbt-{database}",
            "bash",
            "-c",
            command,
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600,
            check=False,
            env=dbt_env,
        )

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
                result.returncode, cmd, result.stdout, result.stderr
            )

        return result

    yield _run_dbt_command

    # Cleanup runner container
    cmd = docker_compose_command.split() + [
        "-f",
        runner_compose_file,
        "-p",
        runner_project_name,
        "down",
        "-v",
    ]
    subprocess.run(
        cmd,
        env=dbt_env,
    )


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
def dbt_seed(run_dbt, database):
    """
    Fixture that runs dbt seed before tests that need seeded data.
    Runs deps and seed commands to set up test data.
    """
    # Run deps first to ensure packages are installed
    run_dbt("dbt deps")

    # Run seed to load test data. Fusion >= 2.0.0-preview.176 supports the
    # generic-test arguments needed for FK creation (dbt-fusion#1575); older
    # Fusion would silently skip FK constraints but still complete the seed,
    # so we no longer need a Fusion-specific suppress block here.
    run_dbt("dbt seed --full-refresh")
    yield
    # No cleanup needed - seeds persist for the test run


@pytest.fixture(scope="function")
def target(database):
    """Provide the database target name for tests."""
    return database


@pytest.fixture(scope="function")
def dbt_issue_105_models(dbt_runner):
    """
    Run the issue #105 test models (parent and child with custom properties).
    Returns tuple of (parent_result, child_result) for assertions.
    """
    # Run all issue_105 models together (dbt build handles FK dependency order and runs tests)
    result = dbt_runner(["build", "--select", "issue_105*"])

    # Return the same result twice for compatibility with tests expecting (parent, child)
    return (result, result)
