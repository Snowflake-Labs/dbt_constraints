"""
Run dbt commands inside Snowflake, through dbt Projects on Snowflake.

The local cells run `dbt` from a uv venv on the host. A dpos_* cell instead deploys the
project to Snowflake as a DBT PROJECT object and runs each command with the `snow` CLI.
Snowflake supplies the engine, so the host installs no client for the run.

`snow` is itself a host process, so each call returns a `subprocess.CompletedProcess`.
The existing tests read `.returncode` and `.stdout`, so they need no change.

Command support differs from a local dbt client. Read `dbt_command_support` before you
add a command to a test:

  supported     build compile deps list parse run run-operation seed show snapshot test
  unsupported   clean debug
  no-op         deps, against a deployed object. The object is read-only, so deps only
                confirms that external access is configured.

These flags are also unsupported: --state --target-path --log-path --profiles-dir
--project-dir --log-format --log-format-file.
"""

import shlex
import subprocess

# Commands that dbt Projects on Snowflake accepts.
SUPPORTED_COMMANDS = frozenset(
    {
        "build",
        "compile",
        "deps",
        "list",
        "parse",
        "run",
        "run-operation",
        "seed",
        "show",
        "snapshot",
        "test",
    }
)

# Commands with no in-database equivalent.
#   clean   removes local target/ and dbt_packages/. A deployed object is immutable.
#   debug   validates a local profile and connection. Snowflake owns both.
UNSUPPORTED_COMMANDS = {
    "clean": "a deployed dbt project object is immutable, so there is nothing to clean",
    "debug": "Snowflake owns the connection, so there is no local profile to check",
}

# Flags that dbt Projects on Snowflake rejects. The host harness sets some of these for
# the local cells, so strip them instead of failing the run.
UNSUPPORTED_FLAGS = (
    "--state",
    "--target-path",
    "--log-path",
    "--profiles-dir",
    "--project-dir",
    "--log-format",
    "--log-format-file",
)


class UnsupportedCommandError(RuntimeError):
    """Raised when a test asks for a command that Snowflake does not run."""


def _connection_flags(
    connection: str,
    database: str,
    schema: str,
    role: str | None,
    warehouse: str | None,
) -> list[str]:
    """
    Return the snow CLI flags that set the session context.

    These flags do more than locate the dbt project object. They set the session that
    Snowflake runs the project under, and env.yml reads that session with CURRENT_ROLE(),
    CURRENT_DATABASE() and CURRENT_WAREHOUSE().

    Pass every value explicitly. A connections.toml entry can carry a different default
    database and no role at all. Relying on those defaults would build the models in the
    wrong database, and the run would still report success.
    """
    flags = ["-c", connection, "--database", database, "--schema", schema]
    if role:
        flags += ["--role", role]
    if warehouse:
        flags += ["--warehouse", warehouse]
    return flags


def split_dbt_command(command: str) -> list[str]:
    """
    Turn a harness command string into the argument list to pass to Snowflake.

    Accept both "dbt build --select x" and "build --select x". Drop the leading "dbt".
    Raise UnsupportedCommandError for a command Snowflake does not run.
    """
    parts = shlex.split(command)
    if parts and parts[0] == "dbt":
        parts = parts[1:]
    if not parts:
        raise UnsupportedCommandError("empty dbt command")

    subcommand = parts[0]
    if subcommand in UNSUPPORTED_COMMANDS:
        raise UnsupportedCommandError(
            f"'dbt {subcommand}' is not supported by dbt Projects on Snowflake: "
            f"{UNSUPPORTED_COMMANDS[subcommand]}"
        )
    if subcommand not in SUPPORTED_COMMANDS:
        raise UnsupportedCommandError(
            f"'dbt {subcommand}' is not in the list of commands that "
            "dbt Projects on Snowflake supports"
        )

    return _strip_unsupported_flags(parts)


def _strip_unsupported_flags(parts: list[str]) -> list[str]:
    """Remove each flag that Snowflake rejects, together with its value."""
    kept: list[str] = []
    skip_next = False
    for part in parts:
        if skip_next:
            skip_next = False
            continue
        if part in UNSUPPORTED_FLAGS:
            skip_next = True
            continue
        if any(part.startswith(f"{flag}=") for flag in UNSUPPORTED_FLAGS):
            continue
        kept.append(part)
    return kept


def create_schema(
    connection: str,
    database: str,
    schema: str,
    role: str | None = None,
    warehouse: str | None = None,
) -> subprocess.CompletedProcess:
    """
    Create the schema that will hold the dbt project object.

    `snow dbt deploy` needs the target schema to exist. dbt creates the schema it
    materializes models into, but not the schema that holds the object itself.
    """
    command = ["snow", "sql", "-c", connection]
    if role:
        command += ["--role", role]
    if warehouse:
        command += ["--warehouse", warehouse]
    command += ["-q", f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}"]
    return subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )


def deploy(
    stage_dir,
    connection: str,
    object_name: str,
    database: str,
    schema: str,
    dbt_version: str,
    role: str | None = None,
    warehouse: str | None = None,
    timeout: int = 1800,
) -> subprocess.CompletedProcess:
    """
    Deploy a staged project, creating a new version of the dbt project object.

    The same command creates the object and updates it. Each call adds VERSION$N, so a
    later run reads the working tree of that run and not a cached copy.

    `--dbt-version` is REQUIRED here, not optional. Snowflake compiles the project when
    it creates the version, and that compile uses the version recorded ON THE OBJECT. It
    does not use the `--dbt-version` passed later to `execute`. Without this flag the
    object defaults to an older dbt Core, and the dbt-fusion project fails to compile:
    its generic tests use the `arguments:` key, which only Fusion 2.x understands.

    No external access integration is passed. The stage already contains dbt_packages,
    so Snowflake does not need to reach hub.getdbt.com. Pass one only if you stop
    staging dependencies.
    """
    return subprocess.run(
        [
            "snow",
            "dbt",
            "deploy",
            *_connection_flags(connection, database, schema, role, warehouse),
            "--dbt-version",
            dbt_version,
            "--source",
            str(stage_dir),
            object_name,
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def execute(
    command: str,
    connection: str,
    object_name: str,
    database: str,
    schema: str,
    dbt_version: str,
    role: str | None = None,
    warehouse: str | None = None,
    timeout: int = 1800,
) -> subprocess.CompletedProcess:
    """
    Run one dbt command against a deployed dbt project object.

    The connection flags set the session context. env.yml reads that session, and
    dbt_projects_profiles.yml reads env.yml, so these flags decide which database,
    role and warehouse the run uses. See _connection_flags.

    `--dbt-version` selects the engine for this call only. It does not change the
    object. The value is the Snowflake version string, for example "1.11.11" or
    "2.0.0-preview.186".

    Raise UnsupportedCommandError if Snowflake does not run the command.
    """
    dbt_args = split_dbt_command(command)

    return subprocess.run(
        [
            "snow",
            "dbt",
            "execute",
            *_connection_flags(connection, database, schema, role, warehouse),
            "--dbt-version",
            dbt_version,
            object_name,
            *dbt_args,
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def connection_exists(connection: str) -> bool:
    """Report whether the snow CLI knows this connection name."""
    result = subprocess.run(
        ["snow", "connection", "list", "--format", "json"],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if result.returncode != 0:
        return False
    return f'"{connection}"' in result.stdout


def object_name_for(database_key: str) -> str:
    """Return the dbt project object name for one matrix cell."""
    return f"DBT_CONSTRAINTS_{database_key.upper()}"
