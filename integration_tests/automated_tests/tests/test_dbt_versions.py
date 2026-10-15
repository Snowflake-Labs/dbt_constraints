"""
Test dbt_constraints across multiple databases and dbt versions.
"""

import re


def test_dbt_workflow(
    database: str, dbt_version: str, run_dbt, request, runs_in_snowflake: bool
):
    """
    Test the complete dbt workflow for a database + version combination.

    The session prepares each cell with `dbt seed --full-refresh` followed by
    `dbt build --full-refresh`. That IS this workflow, so this test asserts the
    preparation succeeded rather than running the same two commands again.
    `test_incremental_rebuild` in test_constraints.py covers the second, incremental pass.

    Under --fast this runs one cheap check instead, and never asks for the baseline, so
    fast mode does not pay for a full build.
    """
    print(f"\n{'=' * 80}")
    print(f"Testing {database} with dbt {dbt_version}")
    print(f"{'=' * 80}\n")

    if request.config.getoption("fast"):
        print("⚡ FAST MODE: Running minimal validation\n")

        if runs_in_snowflake:
            # Snowflake owns the connection, so `dbt debug` has no local profile to
            # check and is not a supported command. `dbt parse` is the closest check
            # that Snowflake does run: it reads the project and resolves every ref.
            result = run_dbt("dbt parse", check=False)
            assert result.returncode == 0, result.stdout + result.stderr
        else:
            # Just verify dbt debug works
            result = run_dbt("dbt debug", check=False)
            assert result.returncode == 0 or "All checks passed" in result.stdout

        print("\n✅ Fast validation passed\n")
        return

    # Ask for the baseline only outside fast mode. Requesting it triggers the seed and
    # full build, and it fails clearly if either did not finish.
    baseline_build = request.getfixturevalue("baseline_build")

    # The build ran the models, the tests and the package's on-run-end hook. Confirm the
    # hook actually ran: without it no constraint is ever created, and every constraint
    # assertion in the suite would fail for one shared reason.
    assert "dbt constraints" in baseline_build, (
        "The full build log does not mention the dbt_constraints hook. "
        "The package did not run, so no constraint was created."
    )

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

    if database == "postgres":
        # The schema name is a string literal, so it needs single quotes. Double
        # quotes would make PostgreSQL read it as an identifier. The whole --args
        # value is shell double-quoted so the single quotes survive, and dbt parses
        # it as a YAML plain scalar.
        count_sql = (
            "SELECT COUNT(*) FROM information_schema.table_constraints "
            "WHERE constraint_schema = 'dbt_constraints_test'"
        )
        result = run_dbt(f'dbt run-operation query --args "{{sql: {count_sql}}}"')
        assert result.returncode == 0, (
            f"Constraint count query failed:\n{result.stdout}"
        )

        # Assert the constraints are actually there, not merely that the query ran.
        match = re.search(r"QUERY RESULT: (\d+)", result.stdout)
        assert match, f"No QUERY RESULT row in output:\n{result.stdout}"
        constraint_count = int(match.group(1))
        assert constraint_count > 0, (
            f"Expected dbt_constraints to create constraints in dbt_constraints_test, "
            f"but information_schema.table_constraints reports {constraint_count}"
        )
        print(f"✅ Found {constraint_count} constraints in dbt_constraints_test")

    print(f"✅ Constraint verification passed for {database}\n")
