"""
Tests for PR #135: create the constraint with quoted columns when the test sets quote_columns.

A test can set quote_columns: true. Before the fix, the package used that value for
the test SQL only. It created the constraint with the hook's global quote_columns,
which is false by default. A column whose name needs quotes then broke the
ALTER TABLE: Snowflake looked for ORDERKEY and PostgreSQL for orderkey.

dim_issue_135_mixed_case has one column, "OrderKey". Its primary_key test sets
quote_columns: true and names the constraint I135_MIXED_PK.

On Snowflake and PostgreSQL the unfixed ALTER TABLE raises, and the preparation
build fails. Oracle's create macro runs the ALTER in an exception handler that only
prints the error, so on Oracle the log still shows "Creating primary key" either way
and this test cannot tell the two apart.

This test reads baseline_build and runs no dbt command of its own.
"""

# type: ignore
import re

import pytest


def _test_passed(build_log: str, test_name: str) -> bool:
    # dbt-core logs "PASS <test>", and dbt Fusion logs "Passed [ 0.2s] test  <test>".
    return re.search(rf"\bpass(ed)?\b[^\n]*\b{test_name}", build_log) is not None


@pytest.mark.issue_135
def test_primary_key_created_with_quoted_columns(baseline_build):
    """The package must create I135_MIXED_PK on the mixed-case column."""
    assert _test_passed(
        baseline_build, "dbt_constraints_primary_key_dim_issue_135_mixed_case_orderkey"
    ), "The primary_key test on dim_issue_135_mixed_case did not pass."
    assert "creating primary key: i135_mixed_pk" in baseline_build, (
        "Expected the package to create I135_MIXED_PK on dim_issue_135_mixed_case."
    )
