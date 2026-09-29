"""
Tests for Issue #110: Snowflake updating existing constraints with set_rely_norely
seems to have gone AWOL.

These tests verify that when a unique/PK/FK test result changes between two dbt
runs, the package issues an ALTER TABLE ... MODIFY CONSTRAINT ... RELY/NORELY
on the existing constraint instead of silently leaving the stale flag in place.

Pre-1.0.5 behavior was correct. The metadata-caching refactor in 1.0.5
(commit f60652b) inadvertently dropped the `set_rely_norely` call path for
already-existing constraints in the three Snowflake create macros. 1.0.9
restores that path and additionally handles the case where the existing
constraint has a different name than the one dbt would have generated.

## How this is tested in a single build

The path only applies to a constraint that ALREADY exists, so a first build cannot reach
it: the package finds nothing and takes the CREATE branch.

Rather than rebuild one table across several runs to reach each state, the project stages
three models whose post-hooks pre-create the constraint with the WRONG flag. A post-hook
runs before the package's on-run-end hook, so the package faces an existing constraint it
must correct. One build therefore covers both directions and the alternate-name case.

    dim_issue_110_prestaged_rely      unique data,     pre-created NORELY -> must go RELY
    dim_issue_110_prestaged_norely    duplicated data, pre-created RELY   -> must go NORELY
    dim_issue_110_prestaged_altname   unique data,     NORELY under a hand-picked name

See macros/stage_existing_constraint.sql in each test project.

`dim_issue_110_rely_flip` remains for the realistic two-run sequence, where the data
itself changes between builds. That is how a user meets this bug, and it reads the one
transition build.

## Careful: "NORELY" contains "RELY"

Assert the whole log line, including the constraint name, as `assert_updated_to` does
below. A check for `"RELY" in output` also matches NORELY.
"""

# type: ignore
import pytest

# The log line the package emits from set_rely_norely.
UPDATE_PREFIX = "updating constraint:"


def _is_snowflake(target: str) -> bool:
    # RELY/NORELY is a Snowflake-only feature. snowflake, fusion and core2 all run
    # against Snowflake, and the in-database cells report "snowflake".
    return target in ("snowflake", "fusion", "core2")


def _skip_unless_snowflake(target: str) -> None:
    if not _is_snowflake(target):
        pytest.skip(f"RELY/NORELY is Snowflake-only (target={target})")


def assert_updated_to(build_log: str, constraint_name: str, rely: str) -> None:
    """
    Fail unless the log shows this constraint being updated to exactly this flag.

    Matches the whole line, `updating constraint: <name> <flag>`. Matching the flag alone
    would not work, because NORELY ends with RELY.
    """
    expected = f"{UPDATE_PREFIX} {constraint_name.lower()} {rely.lower()}"
    assert expected in build_log, (
        f"Expected the package to log {expected!r}.\n"
        "Its absence means the package did not correct the rely flag on a constraint "
        "that already existed, which is the issue #110 regression. The post-hook "
        "pre-created that constraint with the wrong flag on purpose.\n"
        "Lines the build did log:\n"
        + "\n".join(line for line in build_log.splitlines() if UPDATE_PREFIX in line)
    )


@pytest.mark.issue_110
def test_existing_constraint_corrected_to_rely(baseline_build, target):
    """A pre-created NORELY constraint on unique data must be corrected to RELY."""
    _skip_unless_snowflake(target)
    assert_updated_to(
        baseline_build, "dim_issue_110_prestaged_rely_o_orderkey_uk", "RELY"
    )


@pytest.mark.issue_110
def test_existing_constraint_corrected_to_norely(baseline_build, target):
    """A pre-created RELY constraint on duplicated data must be corrected to NORELY."""
    _skip_unless_snowflake(target)
    assert_updated_to(
        baseline_build, "dim_issue_110_prestaged_norely_o_orderkey_uk", "NORELY"
    )


@pytest.mark.issue_110
def test_existing_constraint_found_by_column_not_name(baseline_build, target):
    """
    The package must correct a constraint whose name it did not generate.

    `unique_constraint_exists` matches on columns, so the package must update
    ISSUE_110_HAND_NAMED_UK in place rather than add a second constraint under its own
    generated name. This is the second half of the 1.0.9 fix.
    """
    _skip_unless_snowflake(target)
    assert_updated_to(baseline_build, "issue_110_hand_named_uk", "RELY")

    # It must not also create its own constraint on the same column.
    assert (
        "creating unique key: dim_issue_110_prestaged_altname_o_orderkey_uk"
        not in baseline_build
    ), (
        "The package created a second unique key on a column that already had one. "
        "It did not recognise the hand-named constraint."
    )


@pytest.mark.issue_110
def test_rely_flips_when_data_degrades(request, target, database, dbt_version):
    """
    The realistic sequence: a constraint created RELY must flip to NORELY when the data
    goes bad on a later run.

    The pre-staged tests above isolate the same code path inside one build. This one
    covers how a user actually meets the bug, over two builds with changed data.

    `transition_build` is requested only after the Snowflake guard. Naming it as a
    parameter would run the extra build on every target before the skip could apply, and
    that build serves no purpose where RELY does not exist.

    `database` and `dbt_version` must stay in the signature even though the body does not
    use them. conftest.pytest_generate_tests only parametrizes a test when BOTH names are
    in its fixture closure, and without them this test runs once with no matrix cell.
    """
    _skip_unless_snowflake(target)
    transition_build = request.getfixturevalue("transition_build")
    assert_updated_to(
        transition_build, "dim_issue_110_rely_flip_o_orderkey_uk", "NORELY"
    )


@pytest.mark.issue_110
def test_issue_110_macro_does_not_regress(run_dbt, target):
    """
    Smoke test: parse the project to make sure the macro changes do not
    introduce a Jinja syntax error. This runs on every target (not just
    Snowflake) because parse is dialect-agnostic.

    This keeps its own command rather than reading baseline_build. A successful build
    implies a successful parse, but this test must still report on a target whose build
    cannot complete, which is exactly when a macro syntax error is easiest to miss.
    """
    result = run_dbt("dbt parse", check=False)
    assert result.returncode == 0, (
        f"dbt parse failed after the issue #110 macro changes:\n"
        f"{result.stdout}\n{result.stderr}"
    )
