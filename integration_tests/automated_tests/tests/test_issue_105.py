"""
Tests for Issue #105: Foreign key creation doesn't respect customized properties of referenced model.

These tests verify that foreign key constraints are correctly created when the referenced
parent table uses custom generate_database_name(), generate_schema_name(), or
generate_alias_name() macros.

## Why these tests run no dbt command

The session's one `dbt build --full-refresh` builds every issue_105 model, so these tests
read that build's log through `baseline_build`. See test_constraints.py for the full
reasoning and for the rule about asserting on specific constraint names.

## How the expected names are derived

`{adapter}__create_constraints.sql` names a constraint
`{table_identifier}_{columns}_{PK|UK|FK}`, upper-cased. The identifier is the model ALIAS
when one is configured, which is the whole point of this issue: the package must follow
`generate_alias_name`, `generate_schema_name` and `generate_database_name` to find the
parent relation.

`macros/issue_105/generate_alias_name.sql` replaces `<suffix>` with `DBT_TEST_SUFFIX`,
which defaults to `_test`. So `alias='child_with_alias<suffix>'` becomes
`child_with_alias_test`, and its foreign key is CHILD_WITH_ALIAS_TEST_PARENT_ID_FK.

Every child declares `child_id` as its primary key and `parent_id` as its foreign key.
"""

# type: ignore
import os
import re

import pytest

# The suffix that generate_alias_name substitutes into each custom alias.
SUFFIX = os.environ.get("DBT_TEST_SUFFIX", "_test")

# Child identifier for each scenario, which is what the foreign key is named after.
CHILD_IDENTIFIERS = {
    "custom_schema": "issue_105_child_custom_schema",
    "custom_alias": f"child_with_alias{SUFFIX}",
    "custom_database": "issue_105_child_custom_database",
    "all_custom": f"all_custom_child{SUFFIX}",
}

# Parent identifier for each scenario.
PARENT_IDENTIFIERS = {
    "custom_schema": "issue_105_parent_custom_schema",
    "custom_alias": f"parent_with_alias{SUFFIX}",
    "custom_database": "issue_105_parent_custom_database",
    "all_custom": f"all_custom_parent{SUFFIX}",
}

# Symptoms of the original bug. An unresolved `<placeholder>` reached the SQL, so the
# adapter reported a cache miss for a schema that did not exist, or a syntax error on the
# angle bracket. Lower case, because baseline_build returns lower-cased text.
BUG_PATTERNS = (
    r"cache miss for schema.*<",
    r"unexpected '<'",
    r"syntax error line \d+ at position \d+ unexpected '<'",
)


# Oracle replaces a constraint name longer than this with PK_/UK_/FK_ || ora_hash(name).
# See oracle__create_constraints.sql. The hashed name cannot be predicted from the model
# name, so an exact-name assertion cannot apply to it.
ORACLE_MAX_IDENTIFIER = 30


def _creating_line(constraint_name: str) -> str:
    """
    Return the exact log line the package writes when it CREATES this constraint.

    A bare `name in log` check does not work: the package also logs
    `Skipping <name> because ...`, and dbt logs the test node name, so the constraint name
    is present whether or not the constraint was created.
    """
    suffix = constraint_name.rsplit("_", 1)[-1]
    kinds = {"pk": "primary key", "uk": "unique key", "fk": "foreign key"}
    return f"creating {kinds[suffix]}: {constraint_name}"


def assert_created(baseline_build: str, constraint_name: str, target: str = "") -> None:
    """Fail unless the build log reports creating this exact constraint."""
    if target == "oracle" and len(constraint_name) > ORACLE_MAX_IDENTIFIER:
        pytest.skip(
            f"Oracle hashes {constraint_name!r} because it exceeds "
            f"{ORACLE_MAX_IDENTIFIER} characters, so the stored name is "
            "PK_/UK_/FK_ || ora_hash(...) and cannot be matched by name"
        )
    line = _creating_line(constraint_name)
    assert line in baseline_build, (
        f"The full build never logged {line!r}. The package did not resolve the "
        "customized relation, which is the issue #105 failure. Lines the build did log:\n"
        + "\n".join(
            log_line
            for log_line in baseline_build.splitlines()
            if "creating foreign key:" in log_line
            or "creating primary key:" in log_line
        )
    )


def assert_no_bug_symptoms(baseline_build: str) -> None:
    """Fail if the build log shows an unresolved name placeholder reaching the SQL."""
    for pattern in BUG_PATTERNS:
        assert not re.search(pattern, baseline_build), (
            f"Found error pattern {pattern!r} indicating an issue #105 regression"
        )


def _skip_single_database_adapters(target: str) -> None:
    if target in ("oracle", "postgres"):
        pytest.skip(f"Custom database not supported on {target}")


class _Issue105Scenario:
    """Shared assertions for one customization scenario.

    scenario names the key in CHILD_IDENTIFIERS and PARENT_IDENTIFIERS.
    """

    scenario: str

    def _skip_if_unsupported(self, target: str) -> None:
        """Override where the scenario needs a multi-database adapter."""

    def test_parent_table_created(self, baseline_build, target):
        """Verify the parent primary key is created on the customized relation."""
        self._skip_if_unsupported(target)
        parent = PARENT_IDENTIFIERS[self.scenario]
        assert_created(baseline_build, f"{parent}_id_pk", target)

    def test_child_table_created(self, baseline_build, target):
        """Verify the child primary key is created on the customized relation."""
        self._skip_if_unsupported(target)
        child = CHILD_IDENTIFIERS[self.scenario]
        assert_created(baseline_build, f"{child}_child_id_pk", target)

    def test_foreign_key_constraint_created(self, baseline_build, target):
        """Verify the FK is created even though the parent is customized."""
        self._skip_if_unsupported(target)
        child = CHILD_IDENTIFIERS[self.scenario]
        assert_created(baseline_build, f"{child}_parent_id_fk", target)
        assert_no_bug_symptoms(baseline_build)


@pytest.mark.issue_105
class TestIssue105CustomSchema(_Issue105Scenario):
    """Test FK creation with custom schema names."""

    scenario = "custom_schema"


@pytest.mark.issue_105
class TestIssue105CustomAlias(_Issue105Scenario):
    """Test FK creation with custom alias names."""

    scenario = "custom_alias"


@pytest.mark.issue_105
class TestIssue105CustomDatabase(_Issue105Scenario):
    """Test FK creation with custom database names."""

    scenario = "custom_database"

    def _skip_if_unsupported(self, target: str) -> None:
        _skip_single_database_adapters(target)


@pytest.mark.issue_105
class TestIssue105AllCustom(_Issue105Scenario):
    """Test FK creation with all customizations (database, schema, alias)."""

    scenario = "all_custom"

    def _skip_if_unsupported(self, target: str) -> None:
        _skip_single_database_adapters(target)


@pytest.mark.issue_105
def test_issue_105_regression(baseline_build, target):
    """
    Regression test for issue #105.

    This test ensures that the bug reported in issue #105 is fixed:
    - FK relation search should respect generate_database_name, generate_schema_name, and
      generate_alias_name
    - Should not get cache miss errors for non-existent schemas
    - Should not get SQL compilation errors with unexpected characters

    It checks every scenario the adapter supports in one pass.
    """
    assert_no_bug_symptoms(baseline_build)

    scenarios = ["custom_schema", "custom_alias"]
    if target not in ("oracle", "postgres"):
        scenarios += ["custom_database", "all_custom"]

    for scenario in scenarios:
        assert_created(
            baseline_build, f"{CHILD_IDENTIFIERS[scenario]}_parent_id_fk", target
        )
