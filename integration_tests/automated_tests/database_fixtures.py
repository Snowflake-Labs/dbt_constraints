"""
Database fixtures using testcontainers-python.

This module provides database container fixtures similar to the Java testutil fixtures
in the Snowflake-Arrow-Agent project. Each fixture manages a database container lifecycle
and provides connection information.

Usage:
    # In conftest.py
    from database_fixtures import PostgresFixture, OracleFixture, SqlServerFixture

    @pytest.fixture(scope="session")
    def postgres_container():
        with PostgresFixture() as fixture:
            yield fixture

    # In tests
    def test_something(postgres_container):
        creds = postgres_container.get_credentials()
        # Use creds["host"], creds["port"], etc.
"""

import logging
import secrets
import string
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.mssql import SqlServerContainer
from testcontainers.postgres import PostgresContainer

logger = logging.getLogger(__name__)


def generate_secure_password(length: int = 16, oracle_safe: bool = False) -> str:
    """Generate a secure random password.

    Args:
        length: Password length
        oracle_safe: If True, exclude characters that cause issues in Oracle SQL
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


class AbstractDatabaseFixture(ABC):
    """
    Abstract base class for database test fixtures.

    Similar to the Java AbstractAgentFixture pattern, this provides common
    functionality for all database-specific fixtures.
    """

    def __init__(self):
        self._container = None
        self._started = False

    @abstractmethod
    def start(self) -> None:
        """Start the database container."""
        pass

    @abstractmethod
    def stop(self) -> None:
        """Stop the database container."""
        pass

    @abstractmethod
    def get_credentials(self) -> dict[str, Any]:
        """Get connection credentials for this database.

        Returns:
            Dictionary with keys: host, port, user, password, database
        """
        pass

    @abstractmethod
    def get_database_type(self) -> str:
        """Return the database type identifier (e.g., 'postgres', 'oracle')."""
        pass

    def is_running(self) -> bool:
        """Check if the container is running."""
        return self._started and self._container is not None

    def __enter__(self):
        """Context manager entry - starts the container."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - stops the container."""
        self.stop()
        return False


class PostgresFixture(AbstractDatabaseFixture):
    """
    PostgreSQL database fixture using testcontainers.

    Provides a PostgreSQL 17 container for integration testing.

    Example:
        with PostgresFixture() as fixture:
            creds = fixture.get_credentials()
            # Connect using creds["host"], creds["port"], etc.
    """

    DEFAULT_IMAGE = "postgres:17-alpine"

    def __init__(self, image: str | None = None):
        super().__init__()
        self._image = image or self.DEFAULT_IMAGE
        self._user = generate_db_identifier("dbtusr")
        self._password = generate_secure_password()
        self._database = generate_db_identifier("dbtdb")

    def start(self) -> None:
        if self._started:
            logger.warning("PostgreSQL container already started")
            return

        logger.info(f"Starting PostgreSQL container: {self._image}")
        self._container = PostgresContainer(
            image=self._image,
            username=self._user,
            password=self._password,
            dbname=self._database,
        )
        self._container.start()
        self._started = True
        logger.info(
            f"✅ PostgreSQL started on port {self._container.get_exposed_port(5432)}"
        )

    def stop(self) -> None:
        if not self._started:
            return

        logger.info("Stopping PostgreSQL container")
        if self._container:
            self._container.stop()
            self._container = None
        self._started = False
        logger.info("✅ PostgreSQL stopped")

    def get_credentials(self) -> dict[str, Any]:
        if not self._container:
            raise RuntimeError("PostgreSQL container is not running")

        return {
            "host": self._container.get_container_host_ip(),
            "port": self._container.get_exposed_port(5432),
            "user": self._user,
            "password": self._password,
            "database": self._database,
        }

    def get_database_type(self) -> str:
        return "postgres"

    def get_connection_url(self) -> str:
        """Get SQLAlchemy-compatible connection URL."""
        if not self._container:
            raise RuntimeError("PostgreSQL container is not running")
        return self._container.get_connection_url()


class OracleFixture(AbstractDatabaseFixture):
    """
    Oracle database fixture using testcontainers GenericContainer.

    Uses the official Oracle Free image for ARM64/x64 compatibility.

    Example:
        with OracleFixture() as fixture:
            creds = fixture.get_credentials()
            # Connect using creds["host"], creds["port"], etc.

    Note:
        - First run may be slow (pulls Oracle Free image ~2GB)
        - Oracle Free container requires significant memory (~2GB)
        - Uses SYSTEM user with auto-generated password
    """

    # Oracle Free - official Oracle container with ARM64 support
    DEFAULT_IMAGE = "gvenzl/oracle-free:23-slim-faststart"
    ORACLE_PORT = 1521
    SERVICE_NAME = "FREEPDB1"

    def __init__(self, image: str | None = None):
        super().__init__()
        self._image = image or self.DEFAULT_IMAGE
        # Oracle requires uppercase user names and strong passwords
        self._user = generate_db_identifier("DBTUSR").upper()
        self._password = generate_secure_password(20, oracle_safe=True)

    def start(self) -> None:
        if self._started:
            logger.warning("Oracle container already started")
            return

        logger.info(
            f"Starting Oracle container: {self._image} (this may take several minutes)"
        )

        # Using GenericContainer for Oracle as there's no official testcontainers-oracle for Python
        self._container = DockerContainer(image=self._image)
        self._container.with_exposed_ports(self.ORACLE_PORT)
        self._container.with_env("ORACLE_PASSWORD", self._password)
        self._container.with_env("APP_USER", self._user)
        self._container.with_env("APP_USER_PASSWORD", self._password)

        self._container.start()

        # Wait for Oracle to be ready
        wait_for_logs(
            self._container,
            "DATABASE IS READY TO USE",
            timeout=300,  # 5 minutes timeout
        )

        self._started = True
        logger.info(
            f"✅ Oracle started on port {self._container.get_exposed_port(self.ORACLE_PORT)}"
        )

    def stop(self) -> None:
        if not self._started:
            return

        logger.info("Stopping Oracle container")
        if self._container:
            self._container.stop()
            self._container = None
        self._started = False
        logger.info("✅ Oracle stopped")

    def get_credentials(self) -> dict[str, Any]:
        if not self._container:
            raise RuntimeError("Oracle container is not running")

        return {
            "host": self._container.get_container_host_ip(),
            "port": self._container.get_exposed_port(self.ORACLE_PORT),
            "user": self._user,
            "password": self._password,
            "database": self.SERVICE_NAME,
            "service": self.SERVICE_NAME,
        }

    def get_database_type(self) -> str:
        return "oracle"

    def get_jdbc_url(self) -> str:
        """Get JDBC-style connection URL."""
        creds = self.get_credentials()
        return f"jdbc:oracle:thin:@{creds['host']}:{creds['port']}/{creds['service']}"


class SqlServerFixture(AbstractDatabaseFixture):
    """
    SQL Server database fixture using testcontainers.

    Uses the official Microsoft SQL Server 2022 image.

    Example:
        with SqlServerFixture() as fixture:
            creds = fixture.get_credentials()
            # Connect using creds["host"], creds["port"], etc.

    Note:
        - Requires acceptance of Microsoft EULA (handled automatically)
        - First run may be slow (pulls SQL Server image)
    """

    DEFAULT_IMAGE = "mcr.microsoft.com/mssql/server:2022-latest"

    def __init__(self, image: str | None = None):
        super().__init__()
        self._image = image or self.DEFAULT_IMAGE
        # SQL Server requires 'sa' user and complex passwords
        self._user = "sa"
        self._password = generate_secure_password(20)
        self._database = "master"

    def start(self) -> None:
        if self._started:
            logger.warning("SQL Server container already started")
            return

        logger.info(f"Starting SQL Server container: {self._image}")

        self._container = SqlServerContainer(
            image=self._image,
            password=self._password,
        )
        self._container.start()
        self._started = True
        logger.info(
            f"✅ SQL Server started on port {self._container.get_exposed_port(1433)}"
        )

    def stop(self) -> None:
        if not self._started:
            return

        logger.info("Stopping SQL Server container")
        if self._container:
            self._container.stop()
            self._container = None
        self._started = False
        logger.info("✅ SQL Server stopped")

    def get_credentials(self) -> dict[str, Any]:
        if not self._container:
            raise RuntimeError("SQL Server container is not running")

        return {
            "host": self._container.get_container_host_ip(),
            "port": self._container.get_exposed_port(1433),
            "user": self._user,
            "password": self._password,
            "database": self._database,
        }

    def get_database_type(self) -> str:
        return "sqlserver"

    def get_connection_url(self) -> str:
        """Get SQLAlchemy-compatible connection URL."""
        if not self._container:
            raise RuntimeError("SQL Server container is not running")
        return self._container.get_connection_url()


# Fixture factory for easy access
FIXTURE_CLASSES = {
    "postgres": PostgresFixture,
    "oracle": OracleFixture,
    "sqlserver": SqlServerFixture,
}


def create_database_fixture(database_type: str) -> AbstractDatabaseFixture:
    """
    Factory function to create a database fixture by type.

    Args:
        database_type: One of 'postgres', 'oracle', 'sqlserver'

    Returns:
        Appropriate database fixture instance
    """
    if database_type not in FIXTURE_CLASSES:
        raise ValueError(
            f"Unknown database type: {database_type}. "
            f"Available: {list(FIXTURE_CLASSES.keys())}"
        )

    return FIXTURE_CLASSES[database_type]()


@contextmanager
def database_fixture(database_type: str):
    """
    Context manager for database fixtures.

    Example:
        with database_fixture("postgres") as fixture:
            creds = fixture.get_credentials()
            # Use the database
    """
    fixture = create_database_fixture(database_type)
    try:
        fixture.start()
        yield fixture
    finally:
        fixture.stop()
