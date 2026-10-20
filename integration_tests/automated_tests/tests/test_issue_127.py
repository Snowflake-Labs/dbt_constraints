"""
Tests for Issue #127: set_rely_norely failed on a constraint name that needs quotes.

The package corrects the rely flag on an existing constraint with
ALTER TABLE ... MODIFY CONSTRAINT <name> RELY|NORELY. It reads <name> from
SHOW PRIMARY/UNIQUE/IMPORTED KEYS. Before the fix, the statement used the name without
quotes. Two kinds of name then failed:

    SYS_CONSTRAINT_<uuid>       Snowflake generates this name when a constraint has
                                none. The hyphens cause "unexpected '-'".
    Issue_127_"Mixed"_Case_UK   Snowflake folds an unquoted name to upper case, so it
                                cannot find a mixed-case name. The embedded quote
                                characters also need escaping.

Both models use the issue #110 staging approach. A post-hook pre-creates the
constraint as NORELY, and the data is unique, so the package must update it to RELY.
See macros/stage_existing_constraint.sql.

These tests read baseline_build and run no dbt command of their own. A failed ALTER
fails the preparation build, and then baseline_build fails every test in the cell.
"""

# type: ignore
import re

import pytest

# The log line the package emits from set_rely_norely. Same as test_issue_110.py.
UPDATE_PREFIX = "updating constraint:"


def _skip_unless_snowflake(target: str) -> None:
    # RELY/NORELY is Snowflake-only. snowflake, fusion and core2 run against Snowflake,
    # and the in-database cells report "snowflake".
    if target not in ("snowflake", "fusion", "core2"):
        pytest.skip(f"RELY/NORELY is Snowflake-only (target={target})")


def _update_lines(build_log: str) -> str:
    """Return the set_rely_norely lines of the build log, for a failure message."""
    return "\n".join(line for line in build_log.splitlines() if UPDATE_PREFIX in line)


@pytest.mark.issue_127
def test_auto_named_constraint_corrected_to_rely(baseline_build, target):
    """A constraint with a Snowflake-generated name must be updated to RELY."""
    _skip_unless_snowflake(target)
    # The uuid is random, so match its shape. Only dim_issue_127_auto_named adds a
    # constraint with no name, so a match in this build comes from that model.
    pattern = rf"{UPDATE_PREFIX} sys_constraint_[0-9a-f-]{{36}} rely\b"
    assert re.search(pattern, baseline_build), (
        "Expected the package to update the SYS_CONSTRAINT_<uuid> constraint on "
        "dim_issue_127_auto_named to RELY. Lines the build did log:\n"
        + _update_lines(baseline_build)
    )


@pytest.mark.issue_127
def test_mixed_case_quoted_constraint_corrected_to_rely(baseline_build, target):
    """A mixed-case constraint name with embedded quotes must be updated to RELY."""
    _skip_unless_snowflake(target)
    expected = f'{UPDATE_PREFIX} issue_127_"mixed"_case_uk rely'
    assert expected in baseline_build, (
        f"Expected the package to log {expected!r}. Lines the build did log:\n"
        + _update_lines(baseline_build)
    )
