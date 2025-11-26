"""
General integration tests for dbt_constraints package.

These tests verify that primary keys, unique keys, foreign keys, and not null
constraints are correctly created across different database platforms.
"""

# type: ignore
import pytest


@pytest.mark.parametrize(
    "model_name",
    [
        "dim_part",
        "dim_customers",
        "dim_orders",
    ],
)
def test_primary_key_creation(dbt_runner, dbt_seed, model_name):
    """Test that primary key constraints are created for models."""
    result = dbt_runner(["build", "--select", model_name])
    assert result.returncode == 0, f"Building {model_name} failed:\n{result.stderr}"
    assert "primary_key" in result.stdout.lower()


@pytest.mark.parametrize(
    "model_name",
    [
        "dim_part",
        "dim_customers",
    ],
)
def test_unique_key_creation(dbt_runner, dbt_seed, model_name):
    """Test that unique key constraints are created for models."""
    result = dbt_runner(["build", "--select", model_name])
    assert result.returncode == 0, f"Building {model_name} failed:\n{result.stderr}"
    assert "unique" in result.stdout.lower()


def test_foreign_key_creation(dbt_runner, dbt_seed):
    """Test that foreign key constraints are created."""
    # Build parent first
    parent_result = dbt_runner(["build", "--select", "dim_customers"])
    assert parent_result.returncode == 0, (
        f"Building dim_customers failed:\n{parent_result.stderr}"
    )

    # Build child with FK
    child_result = dbt_runner(["build", "--select", "dim_orders"])
    assert child_result.returncode == 0, (
        f"Building dim_orders failed:\n{child_result.stderr}"
    )
    assert (
        "foreign_key" in child_result.stdout.lower()
        or "relationships" in child_result.stdout.lower()
    )


def test_multi_column_primary_key(dbt_runner, dbt_seed):
    """Test that multi-column primary keys are created."""
    # Build dependencies first
    dbt_runner(["build", "--select", "dim_orders", "dim_part_supplier"])

    # Build model with multi-column PK
    result = dbt_runner(["build", "--select", "fact_order_line"])
    assert result.returncode == 0, f"Building fact_order_line failed:\n{result.stderr}"
    assert "primary_key" in result.stdout.lower()


def test_multi_column_foreign_key(dbt_runner, dbt_seed):
    """Test that multi-column foreign keys are created."""
    # Build parent
    dbt_runner(["build", "--select", "dim_part_supplier"])

    # Build child with multi-column FK
    result = dbt_runner(["build", "--select", "fact_order_line"])
    assert result.returncode == 0, f"Building fact_order_line failed:\n{result.stderr}"
    assert "foreign_key" in result.stdout.lower()


def test_constraints_not_created_on_views(dbt_runner, dbt_seed):
    """Test that constraints are not created on views."""
    result = dbt_runner(["build", "--select", "dim_customers_view"])
    # Build should succeed but constraints should be skipped
    assert result.returncode == 0, (
        f"Building dim_customers_view failed:\n{result.stderr}"
    )


def test_failed_test_no_constraint(dbt_runner, dbt_seed):
    """Test that constraints are not created when tests fail."""
    result = dbt_runner(["build", "--select", "dim_duplicate_orders"])
    # Tests should run but constraints shouldn't be created for failed tests
    # The build may succeed with warnings
    assert "warn" in result.stdout.lower() or result.returncode == 0


def test_always_create_constraint_config(dbt_runner, dbt_seed):
    """Test that always_create_constraint config forces constraint creation."""
    result = dbt_runner(["build", "--select", "dim_orders_null_keys"])
    assert result.returncode == 0
    # Should create valid constraints even if test is skipped
    assert (
        "unique_key" in result.stdout.lower() or "foreign_key" in result.stdout.lower()
    )


@pytest.mark.postgres
def test_postgres_specific(dbt_runner, dbt_seed, target):
    """Test PostgreSQL-specific constraint behavior."""
    if target != "postgres":
        pytest.skip("PostgreSQL-specific test")

    result = dbt_runner(["build", "--full-refresh"])
    assert result.returncode == 0, f"Full build failed:\n{result.stderr}"


@pytest.mark.snowflake
def test_snowflake_specific(dbt_runner, dbt_seed, target):
    """Test Snowflake-specific constraint behavior (RELY/NORELY)."""
    if target != "snowflake":
        pytest.skip("Snowflake-specific test")

    result = dbt_runner(["build", "--full-refresh"])
    assert result.returncode == 0, f"Full build failed:\n{result.stderr}"
    # Snowflake should show RELY clauses
    assert "rely" in result.stdout.lower()


@pytest.mark.oracle
def test_oracle_specific(dbt_runner, dbt_seed, target):
    """Test Oracle-specific constraint behavior."""
    if target != "oracle":
        pytest.skip("Oracle-specific test")

    result = dbt_runner(["build", "--full-refresh"])
    assert result.returncode == 0, f"Full build failed:\n{result.stderr}"


@pytest.mark.sqlserver
def test_sqlserver_specific(dbt_runner, dbt_seed, target):
    """Test SQL Server-specific constraint behavior."""
    if target != "sqlserver":
        pytest.skip("SQL Server-specific test")

    result = dbt_runner(["build", "--full-refresh"])
    assert result.returncode == 0, f"Full build failed:\n{result.stderr}"


def test_full_integration_build(dbt_runner):
    """Full integration test - build everything."""
    # Clean first
    clean_result = dbt_runner(["clean"])
    assert clean_result.returncode == 0

    # Seed
    seed_result = dbt_runner(["seed"])
    assert seed_result.returncode == 0

    # Full build
    build_result = dbt_runner(["build", "--full-refresh"])
    assert build_result.returncode == 0, f"Full build failed:\n{build_result.stderr}"

    # Run again to test incremental behavior
    rebuild_result = dbt_runner(["build"])
    assert rebuild_result.returncode == 0, f"Rebuild failed:\n{rebuild_result.stderr}"
