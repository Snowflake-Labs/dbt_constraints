"""
Tests for dbt Fusion compatibility.

These tests document known compatibility issues between dbt Fusion and dbt_constraints.
Tests are marked with pytest.mark.xfail to indicate expected failures.
"""

import pytest


@pytest.mark.fusion
def test_fusion_version_detection(dbt_runner, target):
    """Verify that Fusion is detected correctly."""
    if target != "fusion":
        pytest.skip("Fusion-specific test")

    # Test that we can run dbt commands
    result = dbt_runner(["--version"])
    assert result.returncode == 0
    assert "dbt-fusion" in result.stdout.lower()


@pytest.mark.fusion
def test_fusion_seed_with_constraints(dbt_runner, target):
    """
    Test that demonstrates the Fusion compatibility issue.

    Expected to fail because:
    - dbt Fusion does not include test arguments in test_metadata.kwargs
    - The dbt_constraints package needs access to parameters like:
      - pk_column_name
      - pk_table_name
      - fk_column_name
    - Without these parameters, constraint creation fails

    This test documents the issue and will pass once Fusion compatibility is resolved.
    """
    if target != "fusion":
        pytest.skip("Fusion-specific test")

    # Clean and seed
    clean_result = dbt_runner(["clean"])
    assert clean_result.returncode == 0

    # This should fail with the known error about missing parameters
    seed_result = dbt_runner(["seed", "--full-refresh"])

    # If this passes, Fusion compatibility has been fixed!
    assert seed_result.returncode == 0, (
        "Seed should succeed once Fusion compatibility is resolved"
    )


@pytest.mark.fusion
def test_fusion_project_parsing(dbt_runner, target):
    """
    Test that Fusion can parse the dbt-fusion project correctly.

    This verifies that:
    - The dbt-fusion project structure is correct
    - YAML files use the correct Fusion format (arguments: wrapper)
    - dbt Fusion can compile the project
    """
    if target != "fusion":
        pytest.skip("Fusion-specific test")

    # Test that the project can be parsed/compiled
    result = dbt_runner(["parse"])
    assert result.returncode == 0, f"Project parsing failed:\n{result.stderr}"
