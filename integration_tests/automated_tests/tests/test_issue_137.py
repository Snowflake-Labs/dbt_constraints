"""
Tests for Issue #137: either dbt_constraints_enabled flag set to false turns a test's constraint off.

A test can set dbt_constraints_enabled in its config or in its config.meta. Both
default to true. Before the fix, the package created the constraint when EITHER
flag was true, so a test had to set both to false to turn the constraint off.

dim_issue_137_meta_off sets the flag to false only in config.meta. Its unique_key
test names its constraint I137_META_OFF_UK, so the package must not log
"Creating unique key: I137_META_OFF_UK".

The test must still run and pass. That shows the constraint is missing because of
the flag, and not because the test or its model did not run.

The same flag directly under config is not tested. dbt Fusion rejects an unknown
key in a test's config (dbt1060), and dbt-core 1.5 drops it from the manifest, so
no fixture could use it in every cell. The fixed check treats both places the same.

This test reads baseline_build and runs no dbt command of its own.
"""

# type: ignore
import re

import pytest


def _test_passed(build_log: str, test_name: str) -> bool:
    # dbt-core logs "PASS <test>", and dbt Fusion logs "Passed [ 0.2s] test  <test>".
    return re.search(rf"\bpass(ed)?\b[^\n]*\b{test_name}", build_log) is not None


@pytest.mark.issue_137
def test_meta_false_turns_constraint_off(baseline_build):
    """A test that sets the flag to false in config.meta must not create its constraint."""
    assert _test_passed(
        baseline_build, "dbt_constraints_unique_key_dim_issue_137_meta_off_o_orderkey"
    ), "The unique_key test on dim_issue_137_meta_off did not run and pass."
    assert "creating unique key: i137_meta_off_uk" not in baseline_build, (
        "The package created I137_META_OFF_UK although the test sets "
        "meta: {dbt_constraints_enabled: false}."
    )
