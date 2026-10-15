"""
Per-cell dbt virtual environments, managed with uv.

Each matrix cell (database plus dbt version) has its own venv. The venv stays in
the cache between runs. A run reuses it when the recorded install spec matches, so
a later run pays no install cost.

Why this code uses venvs and not runner containers:
- A clean venv per cell cannot read the shadowed distribution that a shared Docker
  base image layer causes. dbt-core 1.5.x fails in the shared image, because a newer
  snowplow_tracker module replaces the module that its pin needs.
- dbt runs on the host, so target/ and the logs are directly readable.
- There is no host.docker.internal hop. The database containers publish host ports.

The database containers are still Docker. Only the dbt client moved to a venv.
"""

import json
import os
import shutil
import subprocess
from pathlib import Path

# Each cell installs on this interpreter. It matches the earlier runner image.
# dbt-core 1.5.x does not run on Python 3.12 or later, so this value is necessary.
PYTHON_VERSION = "3.11"

# Adapter package name for each database key
ADAPTER_PACKAGES = {
    "postgres": "dbt-postgres",
    "oracle": "dbt-oracle",
    "sqlserver": "dbt-sqlserver",
    "snowflake": "dbt-snowflake",
}

# Each dbt v2 engine installs one self-contained wheel and no adapter package.
#   fusion -> "dbt"      : Fusion, the Rust engine
#   core2  -> "dbt-core" : dbt Core 2.0, the Apache 2.0 base of Fusion
V2_PACKAGES = {
    "fusion": "dbt",
    "core2": "dbt-core",
}

# Snowflake runs the engine for a dpos_* cell, so the host installs no client for the
# run itself. The host still needs one dbt client to run `dbt deps` while it stages the
# project, because a deployed dbt project object is read-only. These keys name the
# engine that matches the in-database version, so the staged dbt_packages folder comes
# from the same engine that will read it.
DPOS_ADAPTER_PACKAGES = {
    "dpos_core": "dbt-snowflake",
}
DPOS_V2_PACKAGES = {
    "dpos_fusion": "dbt",
}


def dpos_version_to_pypi(dbt_version: str) -> str:
    """
    Convert a Snowflake dbt version string to the matching PyPI version string.

    Snowflake reports Fusion as "2.0.0-preview.186". PyPI publishes the same engine
    build as "2.0.0rc186". The mapping is exact. A dbt Core version needs no change.
    """
    prefix = "2.0.0-preview."
    if dbt_version.startswith(prefix):
        return f"2.0.0rc{dbt_version[len(prefix) :]}"
    if dbt_version == "2.0.0-preview":
        raise ValueError(
            "Snowflake version '2.0.0-preview' has no exact PyPI equivalent. "
            "Pin an explicit preview number, for example 2.0.0-preview.186."
        )
    return dbt_version


# dbt-core 2.0.0b2 imports msgpack in dbt/runner.py, but its wheel metadata declares
# only mashumaro. uv installs only the declared packages, so the import fails at run
# time without these packages. Remove them when upstream declares them.
V2_EXTRA_DEPS = {
    "core2": ["msgpack", "mashumaro"],
}


def build_install_spec(database: str, dbt_version: str) -> list[str]:
    """
    Return the package specifiers to install for one matrix cell.

    Pin core to an exact version, because dbt Projects on Snowflake supports exact
    patches such as 1.11.11. Pin the adapter to the matching minor version only,
    because the adapter patch numbers are independent of the core patch numbers. For
    example, dbt-core 1.11.11 exists but dbt-snowflake 1.11.11 does not.
    """
    if database in DPOS_V2_PACKAGES:
        pypi_version = dpos_version_to_pypi(dbt_version)
        return [f"{DPOS_V2_PACKAGES[database]}=={pypi_version}"]

    if database in DPOS_ADAPTER_PACKAGES:
        minor = ".".join(dbt_version.split(".")[:2])
        return [
            f"dbt-core=={dbt_version}",
            f"{DPOS_ADAPTER_PACKAGES[database]}~={minor}.0",
        ]

    if database in V2_PACKAGES:
        spec = [f"{V2_PACKAGES[database]}=={dbt_version}"]
        spec.extend(V2_EXTRA_DEPS.get(database, []))
        return spec

    adapter = ADAPTER_PACKAGES.get(database)
    if adapter is None:
        raise ValueError(f"Unknown database: {database}")

    minor = ".".join(dbt_version.split(".")[:2])
    return [f"dbt-core=={dbt_version}", f"{adapter}~={minor}.0"]


def venv_path(venv_root: Path, database: str, dbt_version: str) -> Path:
    """Return the venv directory for one matrix cell."""
    # A version string can hold a character that is awkward in a path. Replace it.
    safe_version = dbt_version.replace("+", "_")
    return venv_root / f"dbt-{database}-{safe_version}"


def dbt_executable(venv: Path) -> Path:
    """Return the dbt entry point inside a venv."""
    return venv / "bin" / "dbt"


def _marker(venv: Path) -> Path:
    return venv / ".dbt-constraints-spec.json"


def _is_current(venv: Path, spec: list[str]) -> bool:
    """Report whether an existing venv was built from this exact spec."""
    marker = _marker(venv)
    if not dbt_executable(venv).exists() or not marker.exists():
        return False
    try:
        recorded = json.loads(marker.read_text())
    except (OSError, json.JSONDecodeError):
        return False
    return recorded.get("spec") == spec and recorded.get("python") == PYTHON_VERSION


def ensure_venv(
    venv_root: Path,
    database: str,
    dbt_version: str,
    rebuild: bool = False,
) -> Path:
    """
    Create the venv for one matrix cell, or reuse it when already current.

    Return the venv directory. Raise RuntimeError if uv fails.
    """
    spec = build_install_spec(database, dbt_version)
    venv = venv_path(venv_root, database, dbt_version)

    if not rebuild and _is_current(venv, spec):
        return venv

    # An incomplete venv from an interrupted run resolves in an inconsistent way.
    if venv.exists():
        shutil.rmtree(venv)
    venv_root.mkdir(parents=True, exist_ok=True)

    create = subprocess.run(
        ["uv", "venv", str(venv), "--python", PYTHON_VERSION],
        capture_output=True,
        text=True,
    )
    if create.returncode != 0:
        raise RuntimeError(
            f"uv venv failed for {database} {dbt_version}:\n{create.stderr}"
        )

    # VIRTUAL_ENV sends the install to this venv. It does not activate the venv.
    env = os.environ.copy()
    env["VIRTUAL_ENV"] = str(venv)
    env.pop("UV_SYSTEM_PYTHON", None)

    install = subprocess.run(
        ["uv", "pip", "install", *spec],
        capture_output=True,
        text=True,
        env=env,
    )
    if install.returncode != 0:
        raise RuntimeError(
            f"uv pip install failed for {database} {dbt_version}\n"
            f"spec: {spec}\n{install.stdout}\n{install.stderr}"
        )

    _marker(venv).write_text(
        json.dumps({"spec": spec, "python": PYTHON_VERSION}, indent=2)
    )
    return venv


def installed_version(venv: Path) -> str:
    """Return `dbt --version` output for a venv, or an error string."""
    result = subprocess.run(
        [str(dbt_executable(venv)), "--version"],
        capture_output=True,
        text=True,
    )
    return (result.stdout + result.stderr).strip()


def sqlserver_driver_present() -> bool:
    """
    Report whether the SQL Server ODBC driver is installed on this host.

    The runner image supplied msodbcsql18. On the host it is a real prerequisite, so
    each sqlserver cell must skip instead of fail with an unclear message.
    """
    odbcinst = shutil.which("odbcinst")
    if odbcinst is None:
        return False
    result = subprocess.run([odbcinst, "-q", "-d"], capture_output=True, text=True)
    return "ODBC Driver 18 for SQL Server" in result.stdout


# Ambient database client settings that must not reach dbt.
#
# A container did not read the shell of the developer, so these variables were safe
# before. On the host they are not safe. libpq reads the PG* variables directly, and
# they override the dbt profile. A developer who sets PGSERVICE gets
# 'definition of service "..." not found'. A developer who sets PGHOST or PGDATABASE
# can silently run the tests against the WRONG database. Oracle behaves in the same
# way with TNS_ADMIN and TWO_TASK.
#
# This list keeps ODBCSYSINI and ODBCINI. The SQL Server driver needs them to find
# the driver registration.
AMBIENT_DB_VARS = (
    # libpq
    "PGSERVICE",
    "PGSERVICEFILE",
    "PGHOST",
    "PGHOSTADDR",
    "PGPORT",
    "PGUSER",
    "PGPASSWORD",
    "PGPASSFILE",
    "PGDATABASE",
    "PGOPTIONS",
    "PGSSLMODE",
    "PGSSLCERT",
    "PGSSLKEY",
    "PGSSLROOTCERT",
    "PGREQUIRESSL",
    "PGCONNECT_TIMEOUT",
    "PGCLIENTENCODING",
    "PGAPPNAME",
    # Oracle
    "TNS_ADMIN",
    "TWO_TASK",
    "LOCAL",
    "ORACLE_SID",
)


def strip_ambient_db_vars(env: dict[str, str]) -> dict[str, str]:
    """Remove ambient database client variables from an environment mapping."""
    for name in AMBIENT_DB_VARS:
        env.pop(name, None)
    return env
