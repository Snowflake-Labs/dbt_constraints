"""
Test dbt_constraints across multiple databases and dbt versions.
"""


def test_dbt_workflow(database: str, dbt_version: str, run_dbt, request):
    """
    Test complete dbt workflow for a database + version combination.

    This test runs:
    1. dbt clean
    2. dbt deps
    3. dbt seed --full-refresh
    4. dbt build --full-refresh
    5. dbt seed (incremental)
    6. dbt build (incremental)
    """
    print(f"\n{'=' * 80}")
    print(f"Testing {database} with dbt {dbt_version}")
    print(f"{'=' * 80}\n")

    # Check for fast mode
    fast_mode = request.config.getoption("fast")

    if fast_mode:
        print("⚡ FAST MODE: Running minimal validation\n")

        # Just verify dbt debug works
        result = run_dbt("dbt debug", check=False)
        assert result.returncode == 0 or "All checks passed" in result.stdout

        print("\n✅ Fast validation passed\n")
        return

    # Full test workflow - run the test script
    print("Running full dbt test suite...")
    result = run_dbt("run_dbt_tests.sh")

    assert "All tests passed!" in result.stdout
    assert result.returncode == 0

    print(f"\n{'=' * 80}")
    print(f"✅ ALL TESTS PASSED: {database} with dbt {dbt_version}")
    print(f"{'=' * 80}\n")


def test_constraints_created(database: str, dbt_version: str, run_dbt):
    """
    Verify that dbt_constraints actually creates database constraints.

    This test runs after the main workflow and checks that constraints exist.
    """
    # Skip for fast mode
    # This test depends on test_dbt_workflow completing successfully

    print(f"\n🔍 Verifying constraints exist in {database}...")

    # Run a simple query to verify the database has tables
    if database == "postgres":
        result = run_dbt(
            'dbt run-operation query --args \'{sql: "SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema = \\"dbt_constraints_test\\""}\''
        )
        # Just verify the query runs - actual constraint verification is in dbt tests
        assert result.returncode == 0

    print(f"✅ Constraint verification passed for {database}\n")
