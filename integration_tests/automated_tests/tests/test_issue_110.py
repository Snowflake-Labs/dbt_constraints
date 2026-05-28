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

The toggleable model `dim_issue_110_rely_flip` lives in the dbt project; the
data flips between unique and duplicated based on the
`issue_110_inject_dup` var.
"""

# type: ignore
import pytest


@pytest.mark.issue_110
class TestIssue110RelyFlip:
    """RELY/NORELY must flip on existing constraints when test results change."""

    @staticmethod
    def _is_snowflake(target: str) -> bool:
        # RELY/NORELY is a Snowflake-only feature. Only the snowflake and fusion
        # targets exercise the patched macros.
        return target in ("snowflake", "fusion")

    def test_first_run_creates_rely(self, dbt_runner, target):
        """First run with unique data should create the UK with RELY."""
        if not self._is_snowflake(target):
            pytest.skip(f"RELY/NORELY is Snowflake-only (target={target})")

        # Make sure dependencies exist.
        dbt_runner(["build", "--select", "+dim_issue_110_rely_flip"])

        # Direct rebuild with the default var — unique data, test passes.
        result = dbt_runner(["run", "--select", "dim_issue_110_rely_flip"])
        assert result.returncode == 0
        # Either creates anew or flips to RELY — both are acceptable.
        combined = result.stdout + result.stderr
        assert (
            "Creating unique key" in combined
            or "Updating constraint" in combined
            or "Found UK key" in combined
        ), f"Unexpected first-run output:\n{combined}"

    def test_second_run_flips_to_norely(self, dbt_runner, target):
        """Re-run with duplicates injected — UK must flip from RELY to NORELY."""
        if not self._is_snowflake(target):
            pytest.skip(f"RELY/NORELY is Snowflake-only (target={target})")

        # Run 1: clean data, constraint should be RELY.
        first = dbt_runner(["build", "--select", "+dim_issue_110_rely_flip"])
        assert first.returncode == 0

        # Run 2: inject duplicates so unique_key test fails (severity=warn).
        # The package must ALTER ... MODIFY CONSTRAINT ... NORELY.
        second = dbt_runner(
            [
                "build",
                "--select",
                "dim_issue_110_rely_flip",
                "--vars",
                "'{issue_110_inject_dup: true}'",
            ]
        )
        # severity=warn -> build returns non-zero only on warnings policy;
        # we accept either as long as the rely flip was attempted.
        combined = second.stdout + second.stderr
        assert "Updating constraint" in combined and "NORELY" in combined, (
            "Expected 'Updating constraint ... NORELY' log on second run; "
            "this is the regression path from issue #110.\nOutput:\n" + combined
        )

    def test_third_run_flips_back_to_rely(self, dbt_runner, target):
        """After data is fixed, UK must flip back from NORELY to RELY."""
        if not self._is_snowflake(target):
            pytest.skip(f"RELY/NORELY is Snowflake-only (target={target})")

        # Set up: leave the constraint in NORELY state.
        dbt_runner(["build", "--select", "+dim_issue_110_rely_flip"])
        dbt_runner(
            [
                "build",
                "--select",
                "dim_issue_110_rely_flip",
                "--vars",
                "'{issue_110_inject_dup: true}'",
            ]
        )

        # Re-run with default var — data is unique again, constraint should
        # flip back to RELY.
        result = dbt_runner(["build", "--select", "dim_issue_110_rely_flip"])
        combined = result.stdout + result.stderr
        assert "Updating constraint" in combined and "RELY" in combined, (
            "Expected 'Updating constraint ... RELY' log when data becomes "
            "unique again.\nOutput:\n" + combined
        )


@pytest.mark.issue_110
def test_issue_110_macro_does_not_regress(dbt_runner, target):
    """
    Smoke test: parse the project to make sure the macro changes do not
    introduce a Jinja syntax error. This runs on every target (not just
    Snowflake) because parse is dialect-agnostic.
    """
    result = dbt_runner(["parse"])
    assert result.returncode == 0, (
        f"dbt parse failed after the issue #110 macro changes:\n"
        f"{result.stdout}\n{result.stderr}"
    )
