"""
Tests for Issue #131: dbt_constraints skips every constraint since 1.0.8.

Two clauses in `create_constraints_by_type` read `meta.materialized` with a
literal default of `"other"`:

    node.config.get("meta", {}).get("materialized", "other") not in (...)

Almost no node sets `meta.materialized`, so both clauses read `"other"`.
`"other"` is in neither tuple, so both clauses were always true.

That produced two separate faults:

1.  `verify_permissions` became true for every model. The package then ran the
    ownership pre-check before every constraint. That check does not reliably
    report OWNERSHIP for a table that the same run just replaced, so the package
    skipped the constraint. This is the reported symptom: no constraints at all.
2.  The clause that excludes views, ephemeral models, and dynamic tables sits in
    an `or` inside an inclusive filter. An always-true operand made the whole
    exclusion dead, so those nodes passed the filter.

`meta.materialized` is an override, not an independent property. A custom
materialization uses it to declare the kind of object it builds. The fix reads
`meta.materialized` first and falls back to `config.materialized`, in the new
`dbt_constraints.effective_materialized` macro.

## Why this is tested with run-operation and not with a build

The reported symptom needs a role setup where the ownership pre-check fails.
In this test account dbt owns every table, so the pre-check passes and a build
creates the constraints whether or not the bug is present. A build assertion
would therefore pass both before and after the fix and would prove nothing.

`assert_effective_materialized` instead calls the macro directly with
synthetic nodes and checks both decisions that the real code derives from it.
It fails on the unfixed macro and passes on the fixed one.

See macros/issue_131/assert_effective_materialized.sql in each test project.
"""

# type: ignore
import pytest

# The message that the assertion macro logs when every case passes.
PASS_MESSAGE = "assert_effective_materialized passed"

# Text of the log line that the adapter macros emit when the ownership
# pre-check fails. The package logs some of these at info level.
PRIVILEGE_SKIP = "because of insufficient privileges"


@pytest.mark.issue_131
def test_effective_materialized_reads_config_when_meta_is_absent(run_dbt, target):
    """
    Check every materialization case through the package macro.

    The operation raises a compiler error and returns a non-zero code if any
    case reads the wrong materialization, or derives the wrong decision for
    the view exclusion or for verify_permissions.
    """
    result = run_dbt("dbt run-operation assert_effective_materialized")

    assert result.returncode == 0, (
        f"assert_effective_materialized failed on {target}.\n{result.stdout}"
    )
    assert PASS_MESSAGE in result.stdout, (
        "The assertion macro did not report a pass. The operation may not have "
        f"reached the macro on {target}.\n{result.stdout}"
    )


@pytest.mark.issue_131
def test_baseline_build_skips_no_constraint_for_privileges(baseline_build, target):
    """
    Check that the full build skips no constraint for a privilege reason.

    dbt owns every table in this project, so the package must never report
    insufficient privileges. Issue #131 made the package run the pre-check on
    every model, which is where these messages come from.
    """
    assert PRIVILEGE_SKIP not in baseline_build, (
        f"The build on {target} skipped at least one constraint for a privilege "
        "reason. dbt owns every table here, so the pre-check must not run or "
        "must pass."
    )
