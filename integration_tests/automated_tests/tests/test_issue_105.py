"""
Tests for Issue #105: Foreign key creation doesn't respect customized properties of referenced model.

These tests verify that foreign key constraints are correctly created when the referenced
parent table uses custom generate_database_name(), generate_schema_name(), or generate_alias_name() macros.
"""

# type: ignore
import re

import pytest


@pytest.mark.issue_105
class TestIssue105CustomSchema:
    """Test FK creation with custom schema names."""

    def test_parent_table_created(self, dbt_runner, dbt_issue_105_models):
        """Verify parent table with custom schema is created."""
        result = dbt_runner(["build", "--select", "issue_105_parent_custom_schema"])
        assert result.returncode == 0
        assert (
            "Completed successfully" in result.stdout or "OK created" in result.stdout
        )

    def test_child_table_created(self, dbt_runner, dbt_issue_105_models):
        """Verify child table with FK to custom schema parent is created."""
        result = dbt_runner(["build", "--select", "issue_105_child_custom_schema"])
        assert result.returncode == 0
        assert (
            "Completed successfully" in result.stdout or "OK created" in result.stdout
        )

    def test_foreign_key_constraint_created(self, dbt_runner, dbt_issue_105_models):
        """Verify FK constraint is created despite custom schema."""
        # Check that the FK test passed and constraint was created
        _, child_result = dbt_issue_105_models

        # Look for FK creation or success message
        assert "foreign_key" in child_result.stdout.lower()

        # Should not have the cache miss error from issue #105
        assert "cache miss" not in child_result.stdout.lower()
        assert "unexpected '<'" not in child_result.stderr.lower()


@pytest.mark.issue_105
class TestIssue105CustomAlias:
    """Test FK creation with custom alias names."""

    def test_parent_table_created(self, dbt_runner, dbt_issue_105_models):
        """Verify parent table with custom alias is created."""
        result = dbt_runner(["build", "--select", "issue_105_parent_custom_alias"])
        assert result.returncode == 0
        assert (
            "Completed successfully" in result.stdout or "OK created" in result.stdout
        )

    def test_child_table_created(self, dbt_runner, dbt_issue_105_models):
        """Verify child table with FK to custom alias parent is created."""
        result = dbt_runner(["build", "--select", "issue_105_child_custom_alias"])
        assert result.returncode == 0
        assert (
            "Completed successfully" in result.stdout or "OK created" in result.stdout
        )

    def test_foreign_key_constraint_created(self, dbt_runner, dbt_issue_105_models):
        """Verify FK constraint is created despite custom alias."""
        _, child_result = dbt_issue_105_models

        # Look for FK creation or success message
        assert "foreign_key" in child_result.stdout.lower()

        # Should not have errors
        assert child_result.returncode == 0


@pytest.mark.issue_105
class TestIssue105CustomDatabase:
    """Test FK creation with custom database names."""

    def test_parent_table_created(self, dbt_runner, dbt_issue_105_models, target):
        """Verify parent table with custom database is created."""
        # Skip for databases that don't support multiple databases
        if target in ["oracle", "postgres"]:
            pytest.skip(f"Custom database not supported on {target}")

        result = dbt_runner(["build", "--select", "issue_105_parent_custom_database"])
        assert result.returncode == 0
        assert (
            "Completed successfully" in result.stdout or "OK created" in result.stdout
        )

    def test_child_table_created(self, dbt_runner, dbt_issue_105_models, target):
        """Verify child table with FK to custom database parent is created."""
        if target in ["oracle", "postgres"]:
            pytest.skip(f"Custom database not supported on {target}")

        result = dbt_runner(["build", "--select", "issue_105_child_custom_database"])
        assert result.returncode == 0
        assert (
            "Completed successfully" in result.stdout or "OK created" in result.stdout
        )

    def test_foreign_key_constraint_created(
        self, dbt_runner, dbt_issue_105_models, target
    ):
        """Verify FK constraint is created despite custom database."""
        if target in ["oracle", "postgres"]:
            pytest.skip(f"Custom database not supported on {target}")

        _, child_result = dbt_issue_105_models

        # Look for FK creation or success message
        assert "foreign_key" in child_result.stdout.lower()
        assert child_result.returncode == 0


@pytest.mark.issue_105
class TestIssue105AllCustom:
    """Test FK creation with all customizations (database, schema, alias)."""

    def test_parent_table_created(self, dbt_runner, dbt_issue_105_models, target):
        """Verify parent table with all customizations is created."""
        # Skip database customization for databases that don't support it
        if target in ["oracle", "postgres"]:
            pytest.skip(f"Custom database not supported on {target}")

        result = dbt_runner(["build", "--select", "issue_105_parent_all_custom"])
        assert result.returncode == 0
        assert (
            "Completed successfully" in result.stdout or "OK created" in result.stdout
        )

    def test_child_table_created(self, dbt_runner, dbt_issue_105_models, target):
        """Verify child table with FK to fully customized parent is created."""
        if target in ["oracle", "postgres"]:
            pytest.skip(f"Custom database not supported on {target}")

        result = dbt_runner(["build", "--select", "issue_105_child_all_custom"])
        assert result.returncode == 0
        assert (
            "Completed successfully" in result.stdout or "OK created" in result.stdout
        )

    def test_foreign_key_constraint_created(
        self, dbt_runner, dbt_issue_105_models, target
    ):
        """Verify FK constraint is created despite all customizations."""
        if target in ["oracle", "postgres"]:
            pytest.skip(f"Custom database not supported on {target}")

        _, child_result = dbt_issue_105_models

        # Look for FK creation or success message
        assert "foreign_key" in child_result.stdout.lower()

        # The critical test: should not have the original bug symptoms
        assert "cache miss" not in child_result.stdout.lower()
        assert "unexpected '<'" not in child_result.stderr.lower()
        assert "syntax error" not in child_result.stderr.lower()
        assert child_result.returncode == 0


@pytest.mark.issue_105
def test_issue_105_regression(dbt_runner, dbt_issue_105_models):
    """
    Regression test for issue #105.

    This test ensures that the bug reported in issue #105 is fixed:
    - FK relation search should respect generate_database_name, generate_schema_name, and generate_alias_name
    - Should not get cache miss errors for non-existent schemas
    - Should not get SQL compilation errors with unexpected characters
    """
    parent_result, child_result = dbt_issue_105_models

    # Both builds should succeed
    assert parent_result.returncode == 0, (
        f"Parent models build failed:\n{parent_result.stderr}"
    )
    assert child_result.returncode == 0, (
        f"Child models build failed:\n{child_result.stderr}"
    )

    # Should not see the original bug symptoms in output
    combined_output = (
        parent_result.stdout
        + parent_result.stderr
        + child_result.stdout
        + child_result.stderr
    )

    # Check for absence of error patterns from issue #105
    error_patterns = [
        r"cache miss for schema.*<",  # Cache miss with template placeholder
        r"unexpected '<'",  # SQL syntax error with template placeholder
        r"syntax error line \d+ at position \d+ unexpected '<'",  # Snowflake specific error
    ]

    for pattern in error_patterns:
        assert not re.search(pattern, combined_output, re.IGNORECASE), (
            f"Found error pattern '{pattern}' indicating issue #105 regression"
        )

    print(
        "✓ Issue #105 regression test passed - FK constraints created successfully with custom properties"
    )
