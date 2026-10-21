"""
Tests for Issue #114: skip a test that the database already proves.

A database that enforces a constraint, and has validated it, cannot hold a row
that breaks it. The primary_key, unique_key or foreign_key test then cannot fail,
but it still scans the table. On a large incremental model that scan can take
longer than the load.

With the var dbt_constraints_skip_tests_if_enforced on, the package compiles such
a test to its full SQL wrapped in `where 1 = 0`, and logs
"dbt Constraints: skipping validation of <kind> on <relation> because the
database enforces validated constraint <name>".

Each adapter reports what its database enforces. PostgreSQL and Oracle enforce
every key, Vertica enforces primary and unique keys, and Snowflake, Redshift and
BigQuery enforce only NOT NULL, so they never skip a key test.

A full build releases every constraint before it rebuilds a table, and creates
constraints only after the tests. So the issue #114 models add their constraint
in a post-hook. See macros/stage_enforced_constraint.sql.

The four models cover both directions:

    dim_issue_114_pk_validated       validated primary key     skipped, passes
    fact_issue_114_fk_validated      validated foreign key     skipped, passes
    dim_issue_114_uk_nullable        unique key, two NULLs     runs, warns
    fact_issue_114_fk_not_validated  NOT VALID / NOVALIDATE    runs, warns

These tests read baseline_build and run no dbt command of their own.
"""

# type: ignore
import re

import pytest

SKIP_PREFIX = "dbt constraints: skipping validation of"

# The adapters whose staging macro adds an enforced constraint.
ENFORCING_TARGETS = ("postgres", "oracle")

# The targets that run against Snowflake. The in-database cells report "snowflake".
SNOWFLAKE_TARGETS = ("snowflake", "fusion", "core2")


def _skip_unless_enforcing(target: str) -> None:
    if target not in ENFORCING_TARGETS:
        pytest.skip(
            f"Only PostgreSQL and Oracle enforce key constraints (target={target})"
        )


def _skip_lines(build_log: str) -> str:
    """Return the skip lines of the build log, for a failure message."""
    return "\n".join(line for line in build_log.splitlines() if SKIP_PREFIX in line)


def _skipped(build_log: str, kind: str, model: str) -> bool:
    return (
        re.search(rf"{SKIP_PREFIX} {kind} on [^\n]*\b{model}\b", build_log) is not None
    )


@pytest.mark.issue_114
def test_validated_primary_key_test_is_skipped(baseline_build, target):
    """A validated primary key must make the primary_key test skip."""
    _skip_unless_enforcing(target)
    pattern = rf"{SKIP_PREFIX} primary_key on [^\n]*\bdim_issue_114_pk_validated\b[^\n]*constraint i114_pk\b"
    assert re.search(pattern, baseline_build), (
        "Expected the primary_key test on dim_issue_114_pk_validated to skip because of "
        "I114_PK. Skip lines the build did log:\n" + _skip_lines(baseline_build)
    )
    assert (
        "pass dbt_constraints_primary_key_dim_issue_114_pk_validated_o_orderkey"
        in baseline_build
    )


@pytest.mark.issue_114
def test_validated_foreign_key_test_is_skipped(baseline_build, target):
    """A validated foreign key to the same parent columns must make the foreign_key test skip."""
    _skip_unless_enforcing(target)
    pattern = rf"{SKIP_PREFIX} foreign_key on [^\n]*\bfact_issue_114_fk_validated\b[^\n]*constraint i114_fk\b"
    assert re.search(pattern, baseline_build), (
        "Expected the foreign_key test on fact_issue_114_fk_validated to skip because of "
        "I114_FK. Skip lines the build did log:\n" + _skip_lines(baseline_build)
    )
    assert (
        "pass dbt_constraints_foreign_key_fact_issue_114_fk_validated_o_orderkey"
        in baseline_build
    )


@pytest.mark.issue_114
def test_unique_constraint_on_nullable_key_does_not_skip(baseline_build, target):
    """
    A unique constraint lets repeated NULLs through, but the unique_key test
    counts two NULL keys as a duplicate. The test must run and find them.
    """
    _skip_unless_enforcing(target)
    assert not _skipped(baseline_build, "unique_key", "dim_issue_114_uk_nullable"), (
        "The unique_key test on dim_issue_114_uk_nullable must not skip: its key allows NULL.\n"
        + _skip_lines(baseline_build)
    )
    assert (
        "warn 1 dbt_constraints_unique_key_dim_issue_114_uk_nullable_o_orderkey"
        in baseline_build
    )


@pytest.mark.issue_114
def test_unvalidated_foreign_key_does_not_skip(baseline_build, target):
    """A NOT VALID or ENABLE NOVALIDATE foreign key must not make the test skip."""
    _skip_unless_enforcing(target)
    assert not _skipped(
        baseline_build, "foreign_key", "fact_issue_114_fk_not_validated"
    ), (
        "The foreign_key test on fact_issue_114_fk_not_validated must not skip: its "
        "constraint is not validated.\n" + _skip_lines(baseline_build)
    )
    assert (
        "warn 1 dbt_constraints_foreign_key_fact_issue_114_fk_not_validated_o_orderkey"
        in baseline_build
    )


@pytest.mark.issue_114
def test_snowflake_never_skips_a_key_test(baseline_build, target):
    """Snowflake enforces only NOT NULL, so no key test may skip even with the var on."""
    if target not in SNOWFLAKE_TARGETS:
        pytest.skip(f"Snowflake-only check (target={target})")
    assert SKIP_PREFIX not in baseline_build, (
        "Snowflake does not enforce key constraints, so no test may skip:\n"
        + _skip_lines(baseline_build)
    )
