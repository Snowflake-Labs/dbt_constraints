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

    model                            staged                         expected
    dim_issue_114_pk_validated       primary key                    skipped
    fact_issue_114_fk_validated      foreign key                    skipped (not Vertica)
    dim_issue_114_uk_nullable        unique key, two NULLs          runs, warns
    fact_issue_114_fk_not_validated  NOT VALID / NOVALIDATE         runs, warns
    dim_issue_114_uk_not_null        NOT NULL + unique key          both tests skipped
    dim_issue_114_uk_nnd             UNIQUE NULLS NOT DISTINCT      skipped on PostgreSQL
    dim_issue_114_pk_unusable        primary key, UNUSABLE index    runs on Oracle
    dim_issue_114_pk_not_validated   NOVALIDATE / DISABLED / none   runs
    dim_issue_114_fk_parent2         composite primary key          skipped
    fact_issue_114_fk_crossed        foreign key, pairs crossed     runs, warns
    dim_issue_114_case_collision     key on "OrderKey", test on     runs, warns
                                     orderkey (not Vertica)

The on-run-start hook assert_enforced_constraint_matching also checks the
matching code with stubbed keys on every target.

These tests read baseline_build and run no dbt command of their own.
"""

# type: ignore
import re

import pytest

SKIP_PREFIX = "dbt constraints: skipping validation of"

# The adapters whose staging macro adds an enforced constraint.
ENFORCING_TARGETS = ("postgres", "oracle", "vertica")

# The enforcing adapters that also enforce foreign keys.
FK_ENFORCING_TARGETS = ("postgres", "oracle")

# The targets that run against Snowflake. The in-database cells report "snowflake".
SNOWFLAKE_TARGETS = ("snowflake", "fusion", "core2")


def _skip_unless(target: str, targets: tuple) -> None:
    if target not in targets:
        pytest.skip(f"Only {', '.join(targets)} apply here (target={target})")


def _skip_lines(build_log: str) -> str:
    """Return the skip lines of the build log, for a failure message."""
    return "\n".join(line for line in build_log.splitlines() if SKIP_PREFIX in line)


def _skipped(build_log: str, kind: str, model: str) -> bool:
    return (
        re.search(rf"{SKIP_PREFIX} {kind} on [^\n]*\b{model}\b", build_log) is not None
    )


def _assert_skipped(build_log: str, kind: str, model: str, constraint: str) -> None:
    pattern = (
        rf"{SKIP_PREFIX} {kind} on [^\n]*\b{model}\b[^\n]*constraint {constraint}\b"
    )
    assert re.search(pattern, build_log), (
        f"Expected the {kind} test on {model} to skip because of {constraint}. "
        "Skip lines the build did log:\n" + _skip_lines(build_log)
    )


def _assert_runs(build_log: str, kind: str, model: str, result: str) -> None:
    """The test must not skip, and it must end with `result`: 'pass' or 'warn 1'."""
    assert not _skipped(build_log, kind, model), (
        f"The {kind} test on {model} must not skip.\n" + _skip_lines(build_log)
    )
    assert re.search(rf"\b{result} dbt_constraints_{kind}_{model}", build_log), (
        f"Expected the {kind} test on {model} to {result}."
    )


@pytest.mark.issue_114
def test_validated_primary_key_test_is_skipped(baseline_build, target):
    """A validated primary key must make the primary_key test skip."""
    _skip_unless(target, ENFORCING_TARGETS)
    _assert_skipped(
        baseline_build, "primary_key", "dim_issue_114_pk_validated", "i114_pk"
    )
    assert (
        "pass dbt_constraints_primary_key_dim_issue_114_pk_validated_o_orderkey"
        in baseline_build
    )


@pytest.mark.issue_114
def test_validated_foreign_key_test_is_skipped(baseline_build, target):
    """A validated foreign key to the same parent columns must make the foreign_key test skip."""
    _skip_unless(target, FK_ENFORCING_TARGETS)
    _assert_skipped(
        baseline_build, "foreign_key", "fact_issue_114_fk_validated", "i114_fk"
    )
    assert (
        "pass dbt_constraints_foreign_key_fact_issue_114_fk_validated_o_orderkey"
        in baseline_build
    )


@pytest.mark.issue_114
def test_vertica_never_skips_a_foreign_key_test(baseline_build, target):
    """Vertica does not enforce foreign keys, so a foreign key never proves the test."""
    _skip_unless(target, ("vertica",))
    _assert_runs(baseline_build, "foreign_key", "fact_issue_114_fk_validated", "pass")
    assert not re.search(rf"{SKIP_PREFIX} foreign_key", baseline_build), (
        "Vertica must not skip a foreign_key test.\n" + _skip_lines(baseline_build)
    )


@pytest.mark.issue_114
def test_unique_constraint_on_nullable_key_does_not_skip(baseline_build, target):
    """
    A unique constraint lets repeated NULLs through, but the unique_key test
    counts two NULL keys as a duplicate. The test must run and find them.
    """
    _skip_unless(target, ENFORCING_TARGETS)
    _assert_runs(baseline_build, "unique_key", "dim_issue_114_uk_nullable", "warn 1")


@pytest.mark.issue_114
def test_unvalidated_foreign_key_does_not_skip(baseline_build, target):
    """A NOT VALID or ENABLE NOVALIDATE foreign key must not make the test skip."""
    _skip_unless(target, ENFORCING_TARGETS)
    _assert_runs(
        baseline_build, "foreign_key", "fact_issue_114_fk_not_validated", "warn 1"
    )


@pytest.mark.issue_114
def test_unique_key_on_not_null_column_proves_both_tests(baseline_build, target):
    """A unique key whose column has a validated NOT NULL proves the unique_key and primary_key tests."""
    _skip_unless(target, ENFORCING_TARGETS)
    for kind in ("unique_key", "primary_key"):
        _assert_skipped(baseline_build, kind, "dim_issue_114_uk_not_null", "i114_uknn")


@pytest.mark.issue_114
def test_unique_nulls_not_distinct_skips_on_postgres(baseline_build, target):
    """UNIQUE NULLS NOT DISTINCT proves the unique_key test on a nullable column."""
    _skip_unless(target, ENFORCING_TARGETS)
    if target == "postgres":
        _assert_skipped(
            baseline_build, "unique_key", "dim_issue_114_uk_nnd", "i114_uk_nnd"
        )
    else:
        _assert_runs(baseline_build, "unique_key", "dim_issue_114_uk_nnd", "pass")


@pytest.mark.issue_114
def test_unusable_index_does_not_prove_primary_key(baseline_build, target):
    """An Oracle primary key with an UNUSABLE index can hold duplicates, so the test runs."""
    _skip_unless(target, ENFORCING_TARGETS)
    if target == "oracle":
        _assert_runs(baseline_build, "primary_key", "dim_issue_114_pk_unusable", "pass")
    else:
        _assert_skipped(
            baseline_build,
            "primary_key",
            "dim_issue_114_pk_unusable",
            "i114_pk_unusable",
        )


@pytest.mark.issue_114
def test_unvalidated_primary_key_does_not_skip(baseline_build, target):
    """An ENABLE NOVALIDATE or DISABLED primary key must not make the test skip."""
    _skip_unless(target, ENFORCING_TARGETS)
    _assert_runs(
        baseline_build, "primary_key", "dim_issue_114_pk_not_validated", "pass"
    )


@pytest.mark.issue_114
def test_crossed_foreign_key_pairs_do_not_skip(baseline_build, target):
    """
    The constraint joins (c1, c2) to (k1, k2), and the test joins them to
    (k2, k1). The column sets match but the pairs do not, so the test runs.
    """
    _skip_unless(target, ENFORCING_TARGETS)
    _assert_skipped(
        baseline_build, "primary_key", "dim_issue_114_fk_parent2", "i114_pk2"
    )
    _assert_runs(baseline_build, "foreign_key", "fact_issue_114_fk_crossed", "warn 1")


@pytest.mark.issue_114
def test_case_colliding_column_does_not_skip(baseline_build, target):
    """A key on "OrderKey" must not prove a test on orderkey when the table has both."""
    _skip_unless(target, FK_ENFORCING_TARGETS)
    _assert_runs(baseline_build, "unique_key", "dim_issue_114_case_collision", "warn 1")


@pytest.mark.issue_114
def test_enforced_constraint_matching(baseline_build, target):
    """
    The on-run-start hook assert_enforced_constraint_matching runs the matching
    code with stubbed keys on every target, including Fusion and dbt in
    Snowflake, and runs the skipped test SQL on the database.
    """
    failures = [
        line
        for line in baseline_build.splitlines()
        if "assert_enforced_constraint_matching failed" in line
    ]
    assert not failures, "Matching cases failed:\n" + "\n".join(failures)
    assert "assert_enforced_constraint_matching passed" in baseline_build, (
        "The on-run-start hook assert_enforced_constraint_matching did not log its result."
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
