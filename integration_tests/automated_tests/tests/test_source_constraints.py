"""Tests for constraints on dbt sources.

The package can create constraints on sources, not only on models.
dbt_constraints_sources_enabled and the per-type
dbt_constraints_sources_{pk,uk,fk,nn}_enabled variables control this behaviour.

This support was inert for a long time. The package read test dependencies from
graph.nodes. dbt holds sources in graph.sources, so each source dependency resolved
to nothing. A test declared on a source also has a null attached_node, and the main
loop discarded it.

These tests check the database catalog, not the dbt log output. They fail if the
constraints are absent.
"""

import re

import pytest

# Adapters that have a verified constraint catalog query in assert_source_constraints.
SUPPORTED_TARGETS = ("postgres", "snowflake", "fusion", "core2")

# The package creates constraints in an on-run-end hook. That hook runs after the
# tests. The guard test does nothing unless this variable turns it on, and it must
# run in a separate command after `dbt build`.
# Both catalog guards run in this one `dbt test`, not inside a build. The package
# creates constraints in an on-run-end hook that runs AFTER tests, so a guard placed
# in a build would look at constraints the build had just dropped and not yet
# recreated. assert_fk_parity exists only in the current-syntax project, so it matches
# nothing on the legacy-syntax project. Selecting a name that matches nothing is
# harmless while the other selector matches.
GUARD_SELECTORS = "assert_source_constraints assert_fk_parity"
GUARD_VARS = "'{assert_source_constraints: true, assert_fk_parity: true}'"

# Constraints that the package must create on sources. These names are lower case,
# and the test compares them against lower-case output. Snowflake changes identifiers
# to upper case. Postgres does not.
EXPECTED_LOG_LINES = (
    "creating primary key: source_part_p_partkey_pk",
    "creating unique key: source_supplier_s_suppkey_uk",
    # A foreign key on a model that points AT a source. The package does not create a
    # foreign key declared ON a source. Such a test has a null attached_node and two
    # dependencies. The package cannot identify the child table.
    "creating foreign key: dim_part_supplier_ps_suppkey_fk referencing source_supplier",
)

# A NOT NULL log line names the column and then the relation. The column alone is
# ambiguous: p_partkey exists on the `part` seed and on the `source_part` source.
# Match the column and the source relation together.
EXPECTED_NOT_NULL_PATTERN = re.compile(
    r'creating not null constraint for: p_partkey in .*"source_part"'
)


BUILD_COMMAND = "dbt build"


def _skip_unsupported(target: str) -> None:
    if target not in SUPPORTED_TARGETS:
        pytest.skip(f"No constraint catalog query for {target}")


def test_source_constraints_created(run_dbt, baseline_build, target):
    """The package creates PK, UK, FK and NOT NULL constraints on sources.

    The log assertions read the session's one full build. The catalog check needs its own
    command, because the package creates constraints in an on-run-end hook that runs AFTER
    the tests, so the constraints do not exist during the build that creates them.
    """
    _skip_unsupported(target)

    for line in EXPECTED_LOG_LINES:
        assert line in baseline_build, (
            f"Expected the package to log {line!r}. Its absence shows that the "
            f"package skipped the source constraints.\nOutput:\n{baseline_build}"
        )

    assert EXPECTED_NOT_NULL_PATTERN.search(baseline_build), (
        "Expected a NOT NULL constraint on source_part.p_partkey.\n"
        f"Output:\n{baseline_build}"
    )

    # Check the database catalog. This is the assertion that matters. The log lines
    # above show only that the package made an attempt.
    guard = run_dbt(
        f"dbt test --select {GUARD_SELECTORS} --vars {GUARD_VARS}",
        check=False,
    )
    assert guard.returncode == 0, (
        "assert_source_constraints failed, so a source constraint is absent "
        f"from the database:\n{guard.stdout}\n{guard.stderr}"
    )


def test_source_constraints_respect_disable_var(run_dbt, target):
    """dbt_constraints_sources_enabled=false stops all source constraints.

    This test reseeds on purpose, and the reseed is load-bearing. The seed post-hook
    (clone_table) drops and recreates the source_ tables. Without it the constraints from
    the session's build stay in place, the package finds them already present and logs
    nothing, and the negative assertions below pass for the wrong reason.

    This is why the test does not use the `dbt_seed` fixture: that fixture no longer runs
    a seed, because for every other test the reseed was redundant work.

    This test runs last in the suite and leaves the source_ tables without constraints.
    Add any test that needs them to an earlier module.
    """
    _skip_unsupported(target)

    reseed = run_dbt("dbt seed --full-refresh", check=False)
    assert reseed.returncode == 0, (
        f"Reseed failed, so the source_ tables were not rebuilt and this test cannot "
        f"prove anything:\n{reseed.stdout}\n{reseed.stderr}"
    )

    result = run_dbt(
        f"{BUILD_COMMAND} --vars '{{dbt_constraints_sources_enabled: false}}'",
        check=False,
    )
    assert result.returncode == 0, (
        f"dbt build failed:\n{result.stdout}\n{result.stderr}"
    )

    combined = (result.stdout + result.stderr).lower()
    for line in EXPECTED_LOG_LINES:
        assert line not in combined, (
            f"Found {line!r} even though dbt_constraints_sources_enabled is "
            f"false. The package does not honour the gating variable.\n"
            f"Output:\n{combined}"
        )

    assert not EXPECTED_NOT_NULL_PATTERN.search(combined), (
        "Found a NOT NULL constraint on source_part.p_partkey even though "
        f"dbt_constraints_sources_enabled is false.\nOutput:\n{combined}"
    )
