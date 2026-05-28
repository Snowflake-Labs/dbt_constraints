"""
dbt runner fixtures using testcontainers-python.

This module provides dbt runner container fixtures that build and run dbt
commands in isolated Docker containers. Uses Testcontainers for container
lifecycle management and exposed host ports for database connectivity.

Based on testcontainers patterns from:
https://testcontainers-python.readthedocs.io/en/latest/modules/generic/README.html

Usage:
    # In conftest.py
    from dbt_fixtures import DbtCoreRunnerFixture, DbtFusionRunnerFixture

    @pytest.fixture(scope="function")
    def dbt_runner(database, dbt_version, db_credentials):
        with DbtCoreRunnerFixture(database, dbt_version, db_credentials) as runner:
            yield runner.run_command
"""

import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import Any

from testcontainers.core.container import DockerContainer
from testcontainers.core.image import DockerImage

logger = logging.getLogger(__name__)

# Get directories relative to this file
_THIS_DIR = Path(__file__).parent
_INTEGRATION_TESTS_DIR = _THIS_DIR.parent
_PROJECT_ROOT = _INTEGRATION_TESTS_DIR.parent

# Docker host address for container-to-host networking
DOCKER_HOST_INTERNAL = "host.docker.internal"


class AbstractDbtRunnerFixture(ABC):
    """
    Abstract base class for dbt runner fixtures.

    Provides common functionality for building and running dbt containers.
    Subclasses implement database/engine-specific configuration.

    Uses testcontainers DockerImage for building (with Docker layer caching)
    and DockerContainer for running, following the pattern from:
    https://testcontainers-python.readthedocs.io/en/latest/modules/generic/README.html
    """

    def __init__(
        self,
        database: str,
        dbt_version: str,
        db_credentials: dict[str, str],
        project_dir: str = "dbt-core",
    ):
        """
        Initialize a dbt runner fixture.

        Args:
            database: Target database type (postgres, oracle, sqlserver, snowflake)
            dbt_version: dbt version to install (e.g., "1.9.0")
            db_credentials: Database connection credentials dict
            project_dir: dbt project directory name under integration_tests/
        """
        self._database = database
        self._dbt_version = dbt_version
        self._db_credentials = db_credentials
        self._project_dir = project_dir
        self._container: DockerContainer | None = None
        self._image: DockerImage | None = None
        self._image_tag: str | None = None

    @property
    @abstractmethod
    def dockerfile_name(self) -> str:
        """Dockerfile filename (e.g., 'Dockerfile' or 'Dockerfile.fusion')."""
        pass

    @property
    @abstractmethod
    def image_prefix(self) -> str:
        """Image name prefix for this runner type."""
        pass

    @abstractmethod
    def get_build_args(self) -> dict[str, str]:
        """Get Docker build arguments for this runner."""
        pass

    @abstractmethod
    def get_environment(self) -> dict[str, str]:
        """Get environment variables for the container."""
        pass

    def _get_image_tag(self) -> str:
        """Generate a unique image tag for this runner configuration."""
        return f"{self.image_prefix}:{self._database}-{self._dbt_version}"

    def _build_image(self) -> DockerImage:
        """
        Build the Docker image using testcontainers DockerImage.

        Uses Docker's built-in layer caching for fast rebuilds.
        The DockerImage class handles the build process and cleanup.

        Returns:
            DockerImage instance (manages the built image lifecycle)
        """
        tag = self._get_image_tag()
        logger.info(f"🏗️  Building image: {tag}")

        # Use testcontainers DockerImage for building
        # path: build context directory (project root)
        # dockerfile_path: relative path to Dockerfile within the context
        # tag: image tag to apply
        # buildargs: Docker build arguments
        image = DockerImage(
            path=str(_PROJECT_ROOT),
            dockerfile_path=f"integration_tests/automated_tests/docker/build/{self.dockerfile_name}",
            tag=tag,
            buildargs=self.get_build_args(),
            clean_up=False,  # Keep image for Docker layer caching across runs
        )

        # Build the image (testcontainers handles this in __enter__)
        image.__enter__()
        logger.info(f"✅ Built image: {tag}")

        return image

    def start(self) -> None:
        """Build and start the dbt runner container."""
        if self._container is not None:
            logger.warning("Container already started")
            return

        # Build image using testcontainers DockerImage
        self._image = self._build_image()
        self._image_tag = self._get_image_tag()

        # Create container with Testcontainers DockerContainer
        # Following the pattern: DockerContainer(image=image) or with tag string
        self._container = DockerContainer(image=self._image_tag)

        # Set environment variables using with_env()
        for key, value in self.get_environment().items():
            self._container.with_env(key, value)

        # Mount project directory for access to dbt project files
        self._container.with_volume_mapping(
            str(_PROJECT_ROOT),
            "/project",
            mode="rw",
        )

        # Set working directory
        project_path = f"/project/integration_tests/{self._project_dir}"
        self._container.with_kwargs(working_dir=project_path)

        # Keep container running for multiple exec commands
        self._container.with_command("tail -f /dev/null")

        logger.info(f"🚀 Starting container: {self._image_tag}")
        self._container.start()
        logger.info(
            f"✅ Container started: {self._container.get_wrapped_container().short_id}"
        )

    def stop(self) -> None:
        """Stop and remove the container."""
        if self._container is None:
            return

        logger.info("🛑 Stopping container")
        try:
            self._container.stop()
        except Exception as e:
            logger.warning(f"Error stopping container: {e}")
        self._container = None

        # Note: We don't call self._image.__exit__() here because we set
        # clean_up=False to preserve Docker layer caching across test runs.
        # The image will remain available for faster subsequent builds.
        self._image = None

        logger.info("✅ Container stopped")

    def run_command(self, command: str, check: bool = True) -> dict[str, Any]:
        """
        Execute a command in the container.

        Args:
            command: Shell command to run
            check: If True, raise exception on non-zero exit code

        Returns:
            Dict with keys: exit_code, stdout, stderr
        """
        if self._container is None:
            raise RuntimeError("Container is not running")

        wrapped = self._container.get_wrapped_container()
        logger.info(f"▶️  Running: {command}")

        exec_result = wrapped.exec_run(
            cmd=["bash", "-c", command],
            environment=self.get_environment(),
            workdir=f"/project/integration_tests/{self._project_dir}",
        )

        exit_code = exec_result.exit_code
        output = exec_result.output.decode("utf-8") if exec_result.output else ""

        result = {
            "exit_code": exit_code,
            "stdout": output,
            "stderr": "",  # exec_run combines stdout/stderr
            "returncode": exit_code,  # Compatibility with subprocess
        }

        if check and exit_code != 0:
            logger.error(f"❌ Command failed (exit {exit_code})")
            logger.error(f"Output:\n{output}")
            raise RuntimeError(
                f"Command '{command}' failed with exit code {exit_code}\n"
                f"Output:\n{output}"
            )

        logger.debug(f"✅ Command completed (exit {exit_code})")
        return result

    def get_run_function(self) -> Callable[[str, bool], dict[str, Any]]:
        """Get a callable that runs commands in this container."""
        return self.run_command

    def __enter__(self):
        """Context manager entry - starts the container."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - stops the container."""
        self.stop()
        return False


class DbtCoreRunnerFixture(AbstractDbtRunnerFixture):
    """
    dbt-core runner fixture for testing with standard dbt adapters.

    Supports postgres, oracle, sqlserver, and snowflake databases.

    Example:
        with DbtCoreRunnerFixture("postgres", "1.9.0", creds) as runner:
            runner.run_command("dbt deps")
            runner.run_command("dbt seed")
            result = runner.run_command("dbt run")
    """

    # Map database types to dbt adapter names
    ADAPTER_MAP = {
        "postgres": "postgres",
        "oracle": "oracle",
        "sqlserver": "sqlserver",
        "snowflake": "snowflake",
    }

    @property
    def dockerfile_name(self) -> str:
        return "Dockerfile"

    @property
    def image_prefix(self) -> str:
        return "dbt-constraints-test"

    def get_build_args(self) -> dict[str, str]:
        adapter = self.ADAPTER_MAP.get(self._database, self._database)
        return {
            "DBT_VERSION": self._dbt_version,
            "DBT_ADAPTER": adapter,
        }

    def get_environment(self) -> dict[str, str]:
        """Get environment variables including database credentials."""
        env = {
            "DBT_TARGET": self._database,
            "DBT_PROJECT_DIR": f"/project/integration_tests/{self._project_dir}",
            "DBT_PROFILES_DIR": f"/project/integration_tests/{self._project_dir}",
        }

        # Add database-specific environment variables
        creds = self._db_credentials

        if self._database == "postgres":
            env.update(
                {
                    "POSTGRES_HOST": creds.get("host", DOCKER_HOST_INTERNAL),
                    "POSTGRES_PORT": creds.get("port", "5432"),
                    "POSTGRES_USER": creds.get("user", ""),
                    "POSTGRES_PASSWORD": creds.get("password", ""),
                    "POSTGRES_DB": creds.get("database", ""),
                }
            )

        elif self._database == "oracle":
            env.update(
                {
                    "ORACLE_HOST": creds.get("host", DOCKER_HOST_INTERNAL),
                    "ORACLE_PORT": creds.get("port", "1521"),
                    "ORACLE_USER": creds.get("user", ""),
                    "ORACLE_PASSWORD": creds.get("password", ""),
                    "ORACLE_SERVICE": creds.get("service", "FREEPDB1"),
                    "ORACLE_DATABASE": creds.get("database", "FREEPDB1"),
                }
            )

        elif self._database == "sqlserver":
            env.update(
                {
                    "SQLSERVER_HOST": creds.get("host", DOCKER_HOST_INTERNAL),
                    "SQLSERVER_PORT": creds.get("port", "1433"),
                    "SQLSERVER_USER": creds.get("user", ""),
                    "SQLSERVER_PASSWORD": creds.get("password", ""),
                    "SQLSERVER_DATABASE": creds.get("database", "master"),
                }
            )

        elif self._database == "snowflake":
            # Map all Snowflake credential keys to environment variables
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
            for key, env_var in snowflake_env_map.items():
                if key in creds:
                    env[env_var] = creds[key]

        return env


class DbtFusionRunnerFixture(AbstractDbtRunnerFixture):
    """
    dbt-fusion runner fixture for testing with the Fusion engine.

    Fusion is a Rust-based dbt execution engine that uses Snowflake as backend.

    Example:
        with DbtFusionRunnerFixture("1.0.0", snowflake_creds) as runner:
            runner.run_command("dbt deps")
            runner.run_command("dbt run")
    """

    def __init__(
        self,
        dbt_version: str,
        db_credentials: dict[str, str],
    ):
        """
        Initialize a Fusion runner fixture.

        Args:
            dbt_version: Fusion version (or "latest")
            db_credentials: Snowflake connection credentials
        """
        super().__init__(
            database="fusion",
            dbt_version=dbt_version,
            db_credentials=db_credentials,
            project_dir="dbt-fusion",
        )

    @property
    def dockerfile_name(self) -> str:
        return "Dockerfile.fusion"

    @property
    def image_prefix(self) -> str:
        return "dbt-constraints-fusion"

    def get_build_args(self) -> dict[str, str]:
        return {
            "DBT_VERSION": self._dbt_version,
        }

    def get_environment(self) -> dict[str, str]:
        """Get environment variables for Fusion with Snowflake backend."""
        env = {
            "DBT_TARGET": "snowflake",
            "DBT_PROJECT_DIR": f"/project/integration_tests/{self._project_dir}",
            "DBT_PROFILES_DIR": f"/project/integration_tests/{self._project_dir}",
        }

        # Map all Snowflake credential keys to environment variables
        creds = self._db_credentials
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
        for key, env_var in snowflake_env_map.items():
            if key in creds:
                env[env_var] = creds[key]

        return env


# Fixture factory for easy access
FIXTURE_CLASSES = {
    "postgres": DbtCoreRunnerFixture,
    "oracle": DbtCoreRunnerFixture,
    "sqlserver": DbtCoreRunnerFixture,
    "snowflake": DbtCoreRunnerFixture,
    "fusion": DbtFusionRunnerFixture,
}


def create_dbt_runner_fixture(
    database: str,
    dbt_version: str,
    db_credentials: dict[str, str],
) -> AbstractDbtRunnerFixture:
    """
    Factory function to create a dbt runner fixture by database type.

    Args:
        database: Database type (postgres, oracle, sqlserver, snowflake, fusion)
        dbt_version: dbt version to use
        db_credentials: Database connection credentials

    Returns:
        Appropriate dbt runner fixture instance
    """
    if database == "fusion":
        return DbtFusionRunnerFixture(dbt_version, db_credentials)

    if database not in FIXTURE_CLASSES:
        raise ValueError(
            f"Unknown database type: {database}. "
            f"Available: {list(FIXTURE_CLASSES.keys())}"
        )

    return DbtCoreRunnerFixture(database, dbt_version, db_credentials)
